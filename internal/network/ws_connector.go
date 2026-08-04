package network

import (
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

// WSConnector implements core.Connector using coder/websocket.
type WSConnector struct {
	conn     *websocket.Conn
	pubKey   [32]byte
	outChan  chan core.OutMessage // Pass by value (slice header) to avoid heap escapes
	isClosed bool
	mu       sync.RWMutex
	app      core.App
}

func NewWSConnector(conn *websocket.Conn, pubKey [32]byte, app core.App, outBufSize int) *WSConnector {
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

// readMessage reads exactly one relay protocol message from r into a
// precisely-sized mem.Buffer, parsing the fixed header first to compute the
// exact allocation size.
//
// Layout: FromID(32) | ToIDsLen(1) | ToIDs(N*32) | DataLen(4) | Data(DataLen)
//
// Returns:
//   - (buf, nil)           — valid message, caller owns buf
//   - (nil, errSkipMessage) — nTo==0, reader drained, connection continues
//   - (nil, ErrInvalidMessage) — protocol violation (nTo>max, trailing bytes); caller closes connection
//   - (nil, ErrMessageTooLarge) — DataLen exceeds limit; caller closes connection
//   - (nil, other error)   — I/O error; caller closes connection
//
// errSkipMessage paths drain r to EOF so the connection can continue.
// All other error paths do not drain (caller will close the connection).
func readMessage(r io.Reader, maxDataLen int) (*mem.Buffer, error) {
	// Step 1: fixed prefix — FromID(32) + ToIDsLen(1)
	var hdr [33]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	nTo := int(hdr[32])
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

	dataLen := int(binary.BigEndian.Uint32(tail[toIDsLen : toIDsLen+4]))
	if dataLen > maxDataLen {
		// No drain — caller will close.
		return nil, ErrMessageTooLarge
	}

	// Step 3: allocate exactly the bytes the message requires.
	prefixLen := 33 + tailN
	totalLen := prefixLen + dataLen
	buf := mem.NewBuffer(totalLen)
	data := buf.Bytes()
	copy(data[:33], hdr[:])
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

// ReadWriteLoop is the primary pump. It handles binary protocol parsing and relaying.
func (c *WSConnector) ReadWriteLoop(ctx context.Context) error {
	defer c.Close()
	defer c.app.OnDisconnect(c.pubKey)

	go func(app core.App) {
		for msg := range c.outChan {
			wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.conn.Write(wctx, websocket.MessageBinary, msg.Msg)
			wcancel()
			if msg.Buf != nil {
				msg.Buf.Release()
			}
			if err != nil {
				app.IncrementNoRecipient()
				// Close unblocks conn.Reader() in the main loop and closes outChan.
				c.Close()
				for msg := range c.outChan {
					if msg.Buf != nil {
						msg.Buf.Release()
					}
				}
				return
			}
			app.IncrementDelivered()
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

		buf, err := readMessage(r, core.MaxMessageSize)
		// recvTime captured after full message is in memory — equivalent to conn.Read() semantics.
		recvTime := time.Now()
		if errors.Is(err, errSkipMessage) {
			continue
		}
		if errors.Is(err, ErrMessageTooLarge) {
			_ = c.conn.Close(websocket.StatusMessageTooBig, "message too large")
			return err
		}
		if errors.Is(err, ErrInvalidMessage) {
			_ = c.conn.Close(websocket.StatusUnsupportedData, "invalid message")
			return err
		}
		if err != nil {
			return err
		}

		// nTo already validated (1..core.MaxTargetsPerMessage) inside readMessage.
		msg := core.Message(buf.Bytes())
		c.app.HandleMessage(c, msg, buf, recvTime)
	}
}
