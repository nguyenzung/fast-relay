package core

// Connector is an adapter boundary for the network layer to plug into the Relayer.
// Implementations must be safe for concurrent use from multiple goroutines.
type Connector interface {
	// ID returns the connector's public key. This is the key it is
	// registered under in App's connector registry (see App.OnConnect).
	ID() [32]byte

	// SafePush attempts to enqueue msg for delivery to this connector and
	// returns true on success. It must not block: if the connector cannot
	// accept the message right now (e.g. its outbound queue is full or it
	// is closed), it must drop the message and return false rather than
	// waiting.
	SafePush(msg OutMessage) bool

	// Close releases all resources held by the connector and makes every
	// subsequent SafePush call return false. Must be safe to call more
	// than once.
	Close()
}
