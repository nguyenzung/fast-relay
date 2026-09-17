package core

// This file holds protocol-level primitives shared across internal/domains
// implementations of App. They are mechanical wire-protocol operations with
// no routing policy of their own (no metrics, no privacy stripping, no
// decision about what counts as "processed") - each App decides that for
// itself and composes these primitives to do so.

// ExtractTargets reads the recipient list from msg (msg.ToIDs), excluding
// self, into dst. dst must have capacity >= MaxTargetsPerMessage. Returns
// the number of entries written into dst.
//
// msg.ToIDsLen is untrusted wire input (a uint8, so up to 255) and is
// clamped to MaxTargetsPerMessage here so this function is safe to call
// directly on any Message, regardless of upstream validation - callers that
// already reject nTo > MaxTargetsPerMessage (e.g. internal/network) are
// unaffected, since clamping is a no-op once nTo is already in range.
func ExtractTargets(msg Message, self [32]byte, dst *[MaxTargetsPerMessage][32]byte) int {
	nTo := int(msg.ToIDsLen())
	if nTo > MaxTargetsPerMessage {
		nTo = MaxTargetsPerMessage
	}
	n := 0
	for i := 0; i < nTo; i++ {
		id := msg.ToIDAt(i)
		if id == self {
			continue
		}
		dst[n] = id
		n++
	}
	return n
}

// DeliverTo pushes m to dest, retaining m.Buf's refcount for the duration of
// the push and releasing that retained reference if the push is dropped
// (e.g. dest's outChan is full). DeliverTo only ever manages the reference
// it creates via Retain() here - it never touches the original reference
// that the message's owner (internal/network) holds and releases on its
// own. Safe to call multiple times against the same m (e.g. once per
// target) regardless of how the caller manages its own reference. Returns
// whether the push succeeded.
func DeliverTo(dest Connector, m OutMessage) bool {
	m.Buf.Retain()
	if dest.SafePush(m) {
		return true
	}
	m.Buf.Release() // push dropped, undo retain
	return false
}
