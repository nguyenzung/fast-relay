package network

import (
	"context"
	"sync"
	"time"

	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/coder/websocket"
)

// WSConnector implements core.Connector using coder/websocket.
type WSConnector struct {
	conn     *websocket.Conn
	pubKey   [32]byte
	outChan  chan core.Message // Pass by value (slice header) to avoid heap escapes
	isClosed bool
	mu       sync.Mutex
	relayer  *core.Relayer
}

// NewWSConnector creates a new WSConnector with optimized buffer size.
func NewWSConnector(conn *websocket.Conn, pubKey [32]byte, rel *core.Relayer, outBufSize int) *WSConnector {
	if outBufSize <= 0 {
		outBufSize = 64 // Buffer sized for high-throughput bursts
	}
	return &WSConnector{
		conn:    conn,
		pubKey:  pubKey,
		outChan: make(chan core.Message, outBufSize),
		relayer: rel,
	}
}

func (c *WSConnector) ID() [32]byte { return c.pubKey }

// SafePush handles asynchronous delivery for non-critical or external paths.
func (c *WSConnector) SafePush(msg core.Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
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
func (c *WSConnector) ReadLoop(ctx context.Context) error {
	defer func() {
		c.relayer.Unregister(c.pubKey)
		c.Close()
	}()

	// Start Write Pump for SafePush fallback
	go func() {
		for msg := range c.outChan {
			wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = c.conn.Write(wctx, websocket.MessageBinary, msg)
			cancel()
		}
	}()

	for {
		mt, data, err := c.conn.Read(ctx)
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
			c.relayer.IncrementProcessed()
			// Use relayer Multicast to push to all connectors (excluding sender handled in Multicast)
			c.relayer.Multicast(msg)
			continue
		}

		// 1. Prepare targets slice (Dynamic handling)
		// Since we're going async, we allocate this on the heap to be safe
		targets := make([][32]byte, nTo)
		for i := 0; i < nTo; i++ {
			targets[i] = msg.ToIDAt(i)
		}

		// 2. In-place Zeroing and Stats
		msg.ZeroToIDs()
		c.relayer.IncrementProcessed()

		// 3. Clone once for the entire relay group
		// TODO: We can make a pool of pre-allocated buffers to reduce GC pressure for large messages
		msgClone := make(core.Message, len(msg))
		copy(msgClone, msg)

		// capture receive timestamp to measure receive->deliver latency
		recvTime := time.Now()

		// 4. Fire the relay task to a separate goroutine
		go c.Relay(ctx, msgClone, targets, recvTime) // Pass the original ToIDs for routing, but the payload is zeroed
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
	for _, target := range targets {
		if dest, ok := c.relayer.Get(target); ok {
			matched++
			c.deliver(ctx, dest, msg, recvTime)
		} else {
		}
	}

	if matched == 0 {
		c.relayer.IncrementNoRecipient()
	}
}

func (c *WSConnector) deliver(ctx context.Context, dest core.Connector, msg core.Message, recvTime time.Time) {

	if dest.SafePush(msg) {
		// record delivered counter and latency from receive -> deliver
		c.relayer.IncrementDelivered()
		c.relayer.RecordLatency(time.Since(recvTime))
	} else {
		// push failed (backpressure/drop)
		c.relayer.IncrementNoRecipient()
	}
}
