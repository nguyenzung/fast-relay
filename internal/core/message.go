package core

import "encoding/binary"

// Message is a zero-copy byte slice view of the raw network buffer.
// Layout: [FromID:32][ToIDsLen:1][ToIDs:N*32][DataLen:4][Payload:M]
type Message []byte

// FromID returns the sender's 32-byte public key.
func (m Message) FromID() [32]byte {
	var id [32]byte
	if len(m) >= 32 {
		copy(id[:], m[0:32])
	}
	return id
}

// ToIDsLen returns the number of recipients (N) at offset 32.
func (m Message) ToIDsLen() uint8 {
	if len(m) > 32 {
		return m[32]
	}
	return 0
}

// ToIDAt returns the i-th recipient ID. Returns zeroed array if out of bounds.
func (m Message) ToIDAt(i int) [32]byte {
	var id [32]byte
	n := int(m.ToIDsLen())
	if i >= 0 && i < n {
		start := 33 + (i * 32)
		if len(m) >= start+32 {
			// Optimized by compiler into a few MOV instructions on x86_64
			copy(id[:], m[start:start+32])
		}
	}
	return id
}

// Payload returns a slice pointing directly to the data payload.
func (m Message) Payload() []byte {
	n := int(m.ToIDsLen())
	offset := 33 + (n * 32) // Skip FromID + ToIDsLen + ToIDs
	// Need at least 4 bytes for DataLen
	if len(m) < offset+4 {
		return nil
	}
	// Read DataLen (4 bytes, Big-Endian)
	dataLen := int(binary.BigEndian.Uint32(m[offset : offset+4]))
	start := offset + 4
	if len(m) < start+dataLen {
		return nil
	}
	return m[start : start+dataLen]
}

// ZeroToIDs clears the recipient list in-place for privacy/bandwidth.
// Note: This must be called AFTER extracting recipient IDs but BEFORE Multicast.
func (m Message) ZeroToIDs() {
	n := int(m.ToIDsLen())
	if n == 0 {
		return
	}
	start := 33
	end := 33 + (n * 32)
	if len(m) >= end {
		// Compilers optimize this range-zeroing into a vectorized memclr
		targetArea := m[start:end]
		for i := range targetArea {
			targetArea[i] = 0
		}
	}
}
