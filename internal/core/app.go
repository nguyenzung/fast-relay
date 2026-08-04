package core

import (
	"time"

	"github.com/nguyenzung/relayer-server/internal/mem"
)

// App is the pluggability seam of the system. internal/network and
// internal/server depend only on this interface, never on a concrete app
// type, so a different App (e.g. a game server instead of a relay) can be
// substituted at the entrypoint without touching either package.
type App interface {
	OnConnect(pubKey [32]byte, c Connector)
	OnDisconnect(pubKey [32]byte)

	// HandleMessage is called once per successfully framed inbound message
	// (wire framing is already parsed by internal/network). The App owns the
	// entire routing decision: which recipients (if any) receive it, whether
	// the recipient list is stripped before forwarding, and which counters
	// apply. The App takes ownership of buf and must eventually release it —
	// directly, or by retaining it into pushed OutMessages so each
	// recipient's write pump releases its own reference.
	HandleMessage(from Connector, msg Message, buf *mem.Buffer, recvTime time.Time)

	IncrementNoRecipient()
	IncrementDelivered()
	Count() int
	Range(fn func(pub [32]byte, c Connector) bool)
	Processed() uint64
	Delivered() uint64
	NoRecipient() uint64
	RecordLatency(d time.Duration)
	LatencySnapshot() (count uint64, meanMs float64, stdMs float64, p50Ms float64, p95Ms float64, p99Ms float64)
	Close()
}
