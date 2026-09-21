package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

var (
	ErrMessageTooLarge = errors.New("message too large")
	ErrInvalidMessage  = errors.New("invalid message")
	errSkipMessage     = errors.New("skip") // internal sentinel — not a connection error
)

// wsConn is the subset of *websocket.Conn that WSConnector needs. Extracted
// as an interface (rather than depending on *websocket.Conn directly) so
// tests can substitute a fake implementation instead of driving ReadWriteLoop
// through a real network socket. *websocket.Conn satisfies this today with
// no changes on its end.
type wsConn interface {
	Reader(ctx context.Context) (websocket.MessageType, io.Reader, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(code websocket.StatusCode, reason string) error
}

// WSConnector implements core.Connector using coder/websocket.
type WSConnector struct {
	conn     wsConn
	pubKey   [32]byte
	outChan  chan core.OutMessage // Pass by value (slice header) to avoid heap escapes
	isClosed bool
	mu       sync.RWMutex
	app      core.App
}

func NewWSConnector(conn wsConn, pubKey [32]byte, app core.App, outBufSize int) *WSConnector {
	if outBufSize <= 0 {
		outBufSize = 256
	}
	return &WSConnector{
		conn:    conn,
		pubKey:  pubKey,
		outChan: make(chan core.OutMessage, outBufSize),
		app:     app,
	}
}

func (c *WSConnector) ID() [32]byte { return c.pubKey }

func (c *WSConnector) SafePush(msg core.OutMessage) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.isClosed {
		return false
	}
	select {
	case c.outChan <- msg:
		return true
	default:
		return false // Drop-on-full strategy
	}
}

func (c *WSConnector) Close() {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return
	}
	c.isClosed = true
	close(c.outChan)
	c.mu.Unlock()
	_ = c.conn.Close(websocket.StatusNormalClosure, "closing")
}

// readMessageWithFixedFromID reads exactly one relay protocol message from r
// into a precisely-sized mem.Buffer, parsing the fixed header first to
// compute the exact allocation size.
//
// Wire layout (client -> server): ToIDsLen(1) | ToIDs(N*32) | DataLen(4) | Data(DataLen)
//
// The wire never carries a FromID: the server already knows the sender's
// identity from authentication (see network.WSConnector.pubKey), so trusting
// a client-supplied FromID would let a connection claim to be anyone. Instead
// fromID (the caller's authenticated identity) is written directly into the
// first 32 bytes of the returned buffer's in-memory core.Message layout
// ([FromID:32][ToIDsLen:1][ToIDs:N*32][DataLen:4][Data]) — fixed by the
// caller, never by wire content, hence the name. This is the only place that
// ever writes FromID, so every buffer this function returns already carries
// a trustworthy identity; the App (see relayer.Relayer.HandleMessage) does
// not need to touch it.
//
// Returns:
//   - (buf, nil)           — valid message, caller owns buf; buf[0:32] already
//     equals fromID
//   - (nil, errSkipMessage) — nTo==0, reader drained, connection continues
//   - (nil, ErrInvalidMessage) — protocol violation (nTo>max, a repeated
//     recipient in ToIDs, or trailing bytes); caller closes connection
//   - (nil, ErrMessageTooLarge) — DataLen exceeds limit; caller closes connection
//   - (nil, other error)   — I/O error; caller closes connection
//
// errSkipMessage paths drain r to EOF so the connection can continue.
// All other error paths do not drain (caller will close the connection).
func readMessageWithFixedFromID(r io.Reader, maxDataLen int, fromID [32]byte) (*mem.Buffer, error) {
	// Step 1: ToIDsLen(1)
	var nToByte [1]byte
	if _, err := io.ReadFull(r, nToByte[:]); err != nil {
		return nil, err
	}

	nTo := int(nToByte[0])
	if nTo == 0 {
		// No recipients — valid frame but nothing to relay. Drain to keep connection alive.
		_, _ = io.Copy(io.Discard, r)
		return nil, errSkipMessage
	}
	if nTo > core.MaxTargetsPerMessage {
		// Exceeding the per-message target cap is a protocol violation; close the connection.
		// No drain — caller will close.
		return nil, ErrInvalidMessage
	}

	// Step 2: ToIDs(N*32) + DataLen(4) — fits in a fixed stack buffer.
	toIDsLen := nTo * 32
	var tail [core.MaxTargetsPerMessage*32 + 4]byte
	tailN := toIDsLen + 4
	if _, err := io.ReadFull(r, tail[:tailN]); err != nil {
		return nil, err
	}

	if hasDuplicateTarget(tail[:toIDsLen], nTo) {
		// A repeated recipient amplifies delivery (core.ExtractTargets has no
		// dedup of its own) and has no legitimate use — reject as a protocol
		// violation. No drain — caller will close.
		return nil, ErrInvalidMessage
	}

	dataLen := int(binary.BigEndian.Uint32(tail[toIDsLen : toIDsLen+4]))
	if dataLen > maxDataLen {
		// No drain — caller will close.
		return nil, ErrMessageTooLarge
	}

	// Step 3: allocate exactly the bytes the in-memory Message layout needs:
	// 32 reserved bytes for FromID (not on the wire) + ToIDsLen(1) +
	// ToIDs(N*32) + DataLen(4) + Data.
	prefixLen := 33 + tailN
	totalLen := prefixLen + dataLen
	buf := mem.NewBuffer(totalLen)
	data := buf.Bytes()
	copy(data[:32], fromID[:])
	data[32] = nToByte[0]
	copy(data[33:prefixLen], tail[:tailN])

	if dataLen > 0 {
		if _, err := io.ReadFull(r, data[prefixLen:]); err != nil {
			buf.Release()
			return nil, err
		}
	}

	// EOF probe: if DataLen was understated the client sent extra bytes inside this
	// WebSocket frame. coder/websocket silently discards them on the next Reader()
	// call, so without this check the message would be accepted as valid.
	var probe [1]byte
	n, err := r.Read(probe[:])
	if n > 0 {
		buf.Release()
		return nil, ErrInvalidMessage
	}
	if err != io.EOF {
		buf.Release()
		if err == nil {
			return nil, ErrInvalidMessage
		}
		return nil, err
	}

	return buf, nil
}

// hasDuplicateTarget reports whether toIDs — n consecutive 32-byte recipient
// IDs — contains the same ID more than once. n is at most
// core.MaxTargetsPerMessage (already enforced by the caller), so the O(n^2)
// comparison is at most 45 comparisons of 32 bytes each.
func hasDuplicateTarget(toIDs []byte, n int) bool {
	const idSize = 32

	for i := 0; i < n-1; i++ {
		a := toIDs[i*idSize : (i+1)*idSize]

		for j := i + 1; j < n; j++ {
			if bytes.Equal(a, toIDs[j*idSize:(j+1)*idSize]) {
				return true
			}
		}
	}
	return false
}

// ReadWriteLoop is the primary pump. It handles binary protocol parsing and relaying.
func (c *WSConnector) ReadWriteLoop(ctx context.Context) error {
	defer c.Close()
	defer c.app.OnDisconnect(c.pubKey)

	go func(app core.App) {
		for msg := range c.outChan {
			wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.conn.Write(wctx, websocket.MessageBinary, msg.Msg())
			wcancel()
			if msg.Buf != nil {
				msg.Buf.Release()
			}
			if err != nil {
				app.IncrementDeliveryFailure()
				// Close unblocks conn.Reader() in the main loop and closes outChan.
				c.Close()
				for msg := range c.outChan {
					if msg.Buf != nil {
						msg.Buf.Release()
					}
				}
				return
			}
			app.IncrementDeliverySuccess()
			app.RecordLatency(time.Since(msg.RecvTime))
		}
	}(c.app)

	for {
		mt, r, err := c.conn.Reader(ctx)
		if err != nil {
			return err
		}
		if mt != websocket.MessageBinary {
			_, _ = io.Copy(io.Discard, r)
			continue
		}

		buf, err := readMessageWithFixedFromID(r, core.MaxMessageSize, c.pubKey)

		// recvTime captured after full message is in memory — equivalent to conn.Read() semantics.
		recvTime := time.Now()
		switch {
		case errors.Is(err, errSkipMessage):
			continue
		case errors.Is(err, ErrMessageTooLarge):
			_ = c.conn.Close(websocket.StatusMessageTooBig, "message too large")
			return err
		case errors.Is(err, ErrInvalidMessage):
			_ = c.conn.Close(websocket.StatusUnsupportedData, "invalid message")
			return err
		case err != nil:
			return err
		}

		// nTo already validated (1..core.MaxTargetsPerMessage) inside readMessageWithFixedFromID.
		c.app.HandleMessage(c, core.OutMessage{RecvTime: recvTime, Buf: buf})
		buf.Release()
	}
}
