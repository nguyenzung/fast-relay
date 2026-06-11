package mem

// Buffer holds an uninitialized byte region.
// On Linux the backing store is C malloc (no zero-fill).
// On other platforms it falls back to make (zero-filled).
//
// Always keep the *Buffer alive at least as long as Bytes() is in use —
// on Linux the underlying memory is freed when the Buffer is GC'd.
type Buffer struct {
	data []byte
}

func (b *Buffer) Bytes() []byte { return b.data }
func (b *Buffer) Len() int      { return len(b.data) }
