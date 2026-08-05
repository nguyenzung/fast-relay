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
	// OnConnect registers c under pubKey (e.g. into the connector registry)
	// once a connection has been accepted and authenticated. Called once per
	// connection, before its read/write loop starts.
	OnConnect(pubKey [32]byte, c Connector)

	// OnDisconnect removes the connector previously registered under pubKey.
	// Called once per connection when its read/write loop exits, regardless
	// of whether it exited cleanly or due to an error.
	OnDisconnect(pubKey [32]byte)

	// HandleMessage is called once per successfully framed inbound message
	// (wire framing is already parsed by internal/network). The App owns the
	// entire routing decision: which recipients (if any) receive it, whether
	// the recipient list is stripped before forwarding, and which counters
	// apply.
	//
	// buf ownership: the caller (internal/network) holds buf's original
	// reference and releases it right after HandleMessage returns.
	// HandleMessage must NOT release that reference itself - it may only
	// Retain() additional references for connectors it pushes to (see
	// DeliverTo in protocol.go), each released independently by that
	// recipient's own write pump once written.
	HandleMessage(from Connector, msg Message, buf *mem.Buffer, recvTime time.Time)

	// Count returns the current (approximate) number of registered
	// connectors. Safe to call concurrently with OnConnect/OnDisconnect;
	// the result may be stale by the time the caller reads it.
	Count() int

	// IncrementDeliverySuccess reports that a delivery attempt to a
	// recipient's connector actually completed (e.g. bytes left the wire).
	// Called by internal/network's write pump - the only place that knows
	// a delivery's real, final outcome, as opposed to HandleMessage which
	// only knows whether the item was successfully queued for delivery.
	// Named generically (not e.g. "IncrementMessageDelivered") so any App
	// domain - a relay delivering messages, a game server delivering state
	// updates, a social feed delivering notifications - can report through
	// the same seam. Must be safe to call concurrently from any
	// connector's write pump.
	IncrementDeliverySuccess()

	// IncrementDeliveryFailure reports that a delivery attempt did not
	// complete - whether because no destination could be resolved at
	// routing time (see HandleMessage) or because a resolved destination's
	// write failed afterward (see internal/network's write pump). Must be
	// safe to call concurrently from any connector's write pump.
	IncrementDeliveryFailure()

	// RecordLatency reports the time elapsed between a message being
	// received and a delivery attempt for it actually completing (e.g. the
	// bytes leaving the recipient's socket write). Called by
	// internal/network's write pump - the only place that knows the real
	// completion time and outcome of a delivery, as opposed to HandleMessage
	// which only knows whether the message was successfully queued for
	// delivery. Must be safe to call concurrently from any connector's
	// write pump.
	RecordLatency(d time.Duration)

	// Close stops any background workers owned by the App (e.g. latency
	// aggregation) and releases their resources. Must be safe to call
	// exactly once during shutdown; behavior of App methods called after
	// Close is unspecified.
	Close()

	// Metrics exposes the counters and latency stats accumulated while
	// routing messages. Embedded here so existing App consumers (see
	// internal/server, internal/network) keep working against a single
	// core.App value; see Metrics itself for why it is a separate,
	// independently usable interface.
	Metrics
}

// Metrics is the lifecycle and reporting surface for whatever metrics an
// App chooses to track internally. It is split out from App because its
// caller is a different audience: internal/server's /metrics handler,
// which only needs to start the collection service once, stop it on
// shutdown, and pull a full snapshot per request - it has no business
// with how an App counts or aggregates. Each App is free to define its
// own metrics internally (counters, histograms, ...) and its own
// snapshot shape; FetchMetrics's result is opaque to the caller, which
// only ever passes it through (e.g. JSON-encodes it) without inspecting
// its fields.
type Metrics interface {
	// StartRecording starts the App's metrics collection service (e.g. a
	// background aggregation worker). Called once, before the first
	// FetchMetrics call.
	StartRecording()

	// StopRecording stops the App's metrics collection service and
	// releases any resources it holds. Called once during shutdown.
	StopRecording()

	// FetchMetrics returns a point-in-time snapshot of all metrics the App
	// has accumulated since StartRecording. The concrete type is defined
	// by the App itself.
	FetchMetrics() any
}
