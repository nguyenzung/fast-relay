package mem

import (
	"sync/atomic"
	"unsafe"
)

// Buffer holds an uninitialized byte region.
// On Linux the backing store is jemalloc (no zero-fill).
// On other platforms it falls back to make (zero-filled).
//
// Ownership is tracked via a reference counter.
// Call Retain before sharing a Buffer across goroutines;
// call Release when done — the last Release frees the memory.
type Buffer struct {
	data []byte
	ptr  unsafe.Pointer // original malloc pointer; nil on non-Linux/CGo platforms
	refs atomic.Int32
}

func (b *Buffer) Bytes() []byte { return b.data }
func (b *Buffer) Len() int      { return len(b.data) }

// Retain increments the reference count.
// Panics if called on a buffer whose refcount has already reached zero —
// retaining a freed buffer is a use-after-free bug.
func (b *Buffer) Retain() {
	b.refs.Add(1)
}
