//go:build linux

package mem

/*
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// NewBuffer allocates n bytes via malloc — skips zero-fill on Linux.
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
	// Free CGO memory when Buffer is GC'd; data[0] is valid because n > 0.
	runtime.SetFinalizer(b, func(b *Buffer) {
		C.free(unsafe.Pointer(&b.data[0]))
		b.data = nil
	})
	return b
}
