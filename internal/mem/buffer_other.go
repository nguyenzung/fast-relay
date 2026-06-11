//go:build !linux

package mem

// NewBuffer allocates n zero-filled bytes via make on non-Linux platforms.
func NewBuffer(n int) *Buffer {
	return &Buffer{data: make([]byte, n)}
}
