//go:build linux && cgo

package mem

/*
#cgo LDFLAGS: -ljemalloc
#include <jemalloc/jemalloc.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// freeQueues is a sharded pool of channels. Each worker owns one queue,
// eliminating channel contention on the hot Release path.
var freeQueues []chan *Buffer

func init() {
	n := runtime.NumCPU()
	freeQueues = make([]chan *Buffer, n)
	for i := range n {
		q := make(chan *Buffer, 65536)
		freeQueues[i] = q
		go func() {
			for b := range q {
				C.free(b.ptr)
				b.ptr = nil
				b.data = nil
			}
		}()
	}
}

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
// memory is queued for C.free on the manager goroutine.
// Panics on double-free.
func (b *Buffer) Release() {
	n := b.refs.Add(-1)
	if n < 0 {
		panic("mem.Buffer.Release: double free")
	}
	if n == 0 && b.ptr != nil {
		shard := uintptr(b.ptr) % uintptr(len(freeQueues))
		freeQueues[shard] <- b
	}
}
