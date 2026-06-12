//go:build !linux || !cgo

package mem

// NewBuffer allocates n zero-filled bytes via make on non-Linux/CGo platforms.
// The caller holds one reference; call Release when done.
// n == 0 is valid: returns an empty buffer still holding one reference.
func NewBuffer(n int) *Buffer {
	b := &Buffer{}
	b.refs.Store(1)
	if n > 0 {
		b.data = make([]byte, n)
	}
	return b
}

// Release drops one reference. When the count reaches zero the slice is
// cleared so the GC can reclaim the backing array.
// Panics on double-free.
func (b *Buffer) Release() {
	n := b.refs.Add(-1)
	if n < 0 {
		panic("mem.Buffer.Release: double free")
	}
	if n == 0 {
		b.data = nil
	}
}
