//go:build linux

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

// NewBuffer allocates n bytes via jemalloc — skips zero-fill on Linux.
func NewBuffer(n int) *Buffer {
	if n == 0 {
		return &Buffer{}
	}
	ptr := C.malloc(C.size_t(n))
	if ptr == nil {
		panic("mem.NewBuffer: malloc out of memory")
	}
	b := &Buffer{
		data: unsafe.Slice((*byte)(ptr), n),
	}
	// Free via jemalloc (overrides malloc/free at link time); data[0] is valid because n > 0.
	runtime.SetFinalizer(b, func(b *Buffer) {
		C.free(unsafe.Pointer(&b.data[0]))
		b.data = nil
	})
	return b
}
