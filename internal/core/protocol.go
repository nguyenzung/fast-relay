package core

import (
	"time"

	"github.com/nguyenzung/relayer-server/internal/mem"
)

// This file holds protocol-level primitives shared across internal/domains
// implementations of App. They are mechanical wire-protocol operations with
// no routing policy of their own (no metrics, no privacy stripping, no
// decision about what counts as "processed") - each App decides that for
// itself and composes these primitives to do so.

// ExtractTargets reads the recipient list from msg (msg.ToIDs), excluding
// self, into dst. dst must have capacity >= MaxTargetsPerMessage. Returns
// the number of entries written into dst.
func ExtractTargets(msg Message, self [32]byte, dst *[MaxTargetsPerMessage][32]byte) int {
	nTo := int(msg.ToIDsLen())
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

// DeliverTo pushes msg to dest, retaining buf's refcount for the duration of
// the push and releasing that retained reference if the push is dropped
// (e.g. dest's outChan is full). DeliverTo only ever manages the reference
// it creates via Retain() here - it never touches the original reference
// that the message's owner (internal/network) holds and releases on its
// own. Safe to call multiple times against the same buf (e.g. once per
// target) regardless of how the caller manages its own reference. Returns
// whether the push succeeded.
func DeliverTo(dest Connector, msg Message, buf *mem.Buffer, recvTime time.Time) bool {
	buf.Retain()
	if dest.SafePush(OutMessage{Msg: msg, RecvTime: recvTime, Buf: buf}) {
		return true
	}
	buf.Release() // push dropped, undo retain
	return false
}
