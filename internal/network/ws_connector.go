package network

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nguyenzung/relayer-server/internal/core"
)

// WSConnector implements core.Connector using coder/websocket.
type WSConnector struct {
	conn     *websocket.Conn
	pubKey   [32]byte
	outChan  chan core.OutMessage // Pass by value (slice header) to avoid heap escapes
	isClosed bool
	mu       sync.RWMutex
	relayer  *core.Relayer
}

// NewWSConnector creates a new WSConnector with optimized buffer size.
func NewWSConnector(conn *websocket.Conn, pubKey [32]byte, rel *core.Relayer, outBufSize int) *WSConnector {
	// if outBufSize <= 0 {
	// 	outBufSize = 64 // Buffer sized for high-throughput bursts
	// }
	return &WSConnector{
		conn:    conn,
		pubKey:  pubKey,
		outChan: make(chan core.OutMessage, 256),
		relayer: rel,
	}
}

func (c *WSConnector) ID() [32]byte { return c.pubKey }

// SafePush handles asynchronous delivery for non-critical or external paths.
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
	// log close
}

// ReadLoop is the primary pump. It handles binary protocol parsing and relaying.
func (c *WSConnector) ReadWriteLoop(ctx context.Context) error {
	defer func() {
		c.relayer.Unregister(c.pubKey)
		c.Close()
	}()

	// Start Write Pump for SafePush fallback
	go func(relayer *core.Relayer) {
		for msg := range c.outChan {
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.conn.Write(wctx, websocket.MessageBinary, msg.Msg)
			if err == nil {
				relayer.IncrementDelivered()
				relayer.RecordLatency(time.Since(msg.RecvTime))
			} else {
				c.relayer.IncrementNoRecipient()
			}
			cancel()
		}
	}(c.relayer)

	for {
		mt, data, err := c.conn.Read(ctx)
		// capture receive timestamp to measure receive->deliver latency
		recvTime := time.Now()
		if err != nil {
			return err
		}
		if mt != websocket.MessageBinary {
			continue
		}
		if len(data) < 35 {
			continue
		}

		msg := core.Message(data)
		nTo := int(msg.ToIDsLen())

		// CASE: No recipients => broadcast to all (except sender)
		if nTo == 0 {
			continue
		}

		// 1. Prepare targets slice (Dynamic handling)
		// Since we're going async, we allocate this on the heap to be safe
		targets := make([][32]byte, nTo)
		for i := 0; i < nTo; i++ {
			targets[i] = msg.ToIDAt(i)
		}

		// 2. In-place Stats
		c.relayer.IncrementProcessed()

		// 3. Clone once for the entire relay group
		// TODO: We can make a pool of pre-allocated buffers to reduce GC pressure for large messages
		msgClone := make(core.Message, len(msg))
		copy(msgClone, msg)
		msgClone.ZeroToIDs()

		// 4. Fire the relay task to a separate goroutine
		c.Relay(ctx, msgClone, targets, recvTime) // Pass the original ToIDs for routing, but the payload is zeroed
	}
}

// Relay handles only targeted multicast.
// If targets is empty, it simply returns after incrementing the no-recipient counter.
func (c *WSConnector) Relay(ctx context.Context, msg core.Message, targets [][32]byte, recvTime time.Time) {
	if len(targets) == 0 {
		c.relayer.IncrementNoRecipient()
		return
	}

	matched := 0
	outMsg := core.OutMessage{
		Msg:      msg,
		RecvTime: recvTime,
	}
	for _, target := range targets {
		if dest, ok := c.relayer.Get(target); ok {
			matched++
			c.deliver(ctx, dest, outMsg, recvTime)
		} else {
		}
	}

	if matched == 0 {
		c.relayer.IncrementNoRecipient()
	}
}

func (c *WSConnector) deliver(ctx context.Context, dest core.Connector, msg core.OutMessage, recvTime time.Time) {

	if !dest.SafePush(msg) {
		c.relayer.IncrementNoRecipient()
	}
}
