//go:build linux && cgo

package mem

/*
#cgo LDFLAGS: -ljemalloc
#include <jemalloc/jemalloc.h>
*/
import "C"
import (
	"unsafe"
)

// NewBuffer allocates n bytes via jemalloc — no zero-fill.
// The caller holds one reference; call Release when done.
// n == 0 is valid: returns an empty buffer still holding one reference.
func NewBuffer(n int) *Buffer {
	b := &Buffer{}
	b.refs.Store(1)
	if n == 0 {
		return b
	}
	ptr := C.malloc(C.size_t(n))
	if ptr == nil {
		panic("mem.NewBuffer: malloc out of memory")
	}
	b.ptr = ptr
	b.data = unsafe.Slice((*byte)(ptr), n)
	return b
}

// Release drops one reference. When the count reaches zero the backing
// memory is freed immediately via C.free.
// Panics on double-free.
func (b *Buffer) Release() {
	n := b.refs.Add(-1)
	if n < 0 {
		panic("mem.Buffer.Release: double free")
	}
	if n == 0 && b.ptr != nil {
		C.free(b.ptr)
		b.ptr = nil
		b.data = nil
	}
}
