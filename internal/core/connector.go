package core

// Connector is an adapter boundary for the network layer to plug into the Relayer.
// Implementations must be safe for concurrent use from multiple goroutines.
// SafePush should attempt to enqueue a message for delivery and return true on success.
// Close must release all resources and make further SafePush return false.

type Connector interface {
	ID() [32]byte
	SafePush(msg Message) bool
	Close()
}
