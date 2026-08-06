package core_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/nguyenzung/relayer-server/internal/core"
)

// idFor builds a distinguishable [32]byte id for test fixtures.
func idFor(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

// buildMessage encodes a well-formed frame:
// FromID(32) | ToIDsLen(1) | ToIDs(N*32) | DataLen(4) | Payload.
func buildMessage(t *testing.T, fromID [32]byte, toIDs [][32]byte, payload []byte) core.Message {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(fromID[:])
	buf.WriteByte(byte(len(toIDs)))
	for _, id := range toIDs {
		buf.Write(id[:])
	}
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(payload)))
	buf.Write(dataLen[:])
	buf.Write(payload)
	return core.Message(buf.Bytes())
}

func TestMessage_FromID(t *testing.T) {
	from := idFor(0x11)

	t.Run("well-formed message", func(t *testing.T) {
		m := buildMessage(t, from, nil, nil)
		if got := m.FromID(); got != from {
			t.Fatalf("FromID() = %x, want %x", got, from)
		}
	})

	t.Run("exactly 32 bytes", func(t *testing.T) {
		m := core.Message(from[:])
		if got := m.FromID(); got != from {
			t.Fatalf("FromID() = %x, want %x", got, from)
		}
	})

	t.Run("truncated below 32 bytes returns zero value", func(t *testing.T) {
		m := core.Message(from[:31])
		if got := m.FromID(); got != ([32]byte{}) {
			t.Fatalf("FromID() = %x, want zero value", got)
		}
	})

	t.Run("empty message returns zero value", func(t *testing.T) {
		var m core.Message
		if got := m.FromID(); got != ([32]byte{}) {
			t.Fatalf("FromID() = %x, want zero value", got)
		}
	})
}

func TestMessage_ToIDsLen(t *testing.T) {
	t.Run("well-formed message", func(t *testing.T) {
		m := buildMessage(t, idFor(1), [][32]byte{idFor(2), idFor(3)}, nil)
		if got := m.ToIDsLen(); got != 2 {
			t.Fatalf("ToIDsLen() = %d, want 2", got)
		}
	})

	t.Run("exactly 32 bytes (no ToIDsLen byte present)", func(t *testing.T) {
		m := core.Message(make([]byte, 32))
		if got := m.ToIDsLen(); got != 0 {
			t.Fatalf("ToIDsLen() = %d, want 0", got)
		}
	})

	t.Run("33 bytes reads the ToIDsLen byte", func(t *testing.T) {
		raw := make([]byte, 33)
		raw[32] = 5
		m := core.Message(raw)
		if got := m.ToIDsLen(); got != 5 {
			t.Fatalf("ToIDsLen() = %d, want 5", got)
		}
	})
}

func TestMessage_ToIDAt(t *testing.T) {
	targets := [][32]byte{idFor(0xA1), idFor(0xA2), idFor(0xA3)}
	m := buildMessage(t, idFor(1), targets, []byte("payload"))

	t.Run("in-range indices return the matching id", func(t *testing.T) {
		for i, want := range targets {
			if got := m.ToIDAt(i); got != want {
				t.Fatalf("ToIDAt(%d) = %x, want %x", i, got, want)
			}
		}
	})

	t.Run("negative index returns zero value", func(t *testing.T) {
		if got := m.ToIDAt(-1); got != ([32]byte{}) {
			t.Fatalf("ToIDAt(-1) = %x, want zero value", got)
		}
	})

	t.Run("index == ToIDsLen returns zero value", func(t *testing.T) {
		if got := m.ToIDAt(len(targets)); got != ([32]byte{}) {
			t.Fatalf("ToIDAt(n) = %x, want zero value", got)
		}
	})

	t.Run("declared count exceeds actual bytes present", func(t *testing.T) {
		// ToIDsLen claims 3 recipients but the buffer is truncated after the
		// first one — ToIDAt(1) must not read past the slice.
		full := buildMessage(t, idFor(1), targets, nil)
		truncated := core.Message(full[:33+32]) // header + exactly one id
		if got := truncated.ToIDAt(1); got != ([32]byte{}) {
			t.Fatalf("ToIDAt(1) on truncated message = %x, want zero value", got)
		}
	})
}

func TestMessage_Payload(t *testing.T) {
	t.Run("well-formed message returns exact payload", func(t *testing.T) {
		want := []byte("hello world")
		m := buildMessage(t, idFor(1), [][32]byte{idFor(2)}, want)
		got := m.Payload()
		if !bytes.Equal(got, want) {
			t.Fatalf("Payload() = %q, want %q", got, want)
		}
	})

	t.Run("zero-length payload returns empty, non-nil slice", func(t *testing.T) {
		m := buildMessage(t, idFor(1), nil, nil)
		got := m.Payload()
		if got == nil {
			t.Fatalf("Payload() = nil, want empty slice")
		}
		if len(got) != 0 {
			t.Fatalf("Payload() length = %d, want 0", len(got))
		}
	})

	t.Run("message too short to contain DataLen returns nil", func(t *testing.T) {
		// Header (FromID + ToIDsLen=0) with no DataLen field at all.
		m := core.Message(make([]byte, 33))
		if got := m.Payload(); got != nil {
			t.Fatalf("Payload() = %v, want nil", got)
		}
	})

	t.Run("DataLen overstates the bytes actually present returns nil", func(t *testing.T) {
		full := buildMessage(t, idFor(1), nil, []byte("short"))
		// Truncate the buffer after DataLen but before the full payload.
		truncated := core.Message(full[:len(full)-2])
		if got := truncated.Payload(); got != nil {
			t.Fatalf("Payload() = %v, want nil", got)
		}
	})
}

func TestMessage_ZeroToIDs(t *testing.T) {
	t.Run("no-op when there are no recipients", func(t *testing.T) {
		m := buildMessage(t, idFor(1), nil, []byte("payload"))
		before := append(core.Message(nil), m...)
		m.ZeroToIDs()
		if !bytes.Equal(m, before) {
			t.Fatalf("ZeroToIDs() mutated a message with ToIDsLen == 0")
		}
	})

	t.Run("zeroes exactly the ToIDs region and nothing else", func(t *testing.T) {
		targets := [][32]byte{idFor(0xAA), idFor(0xBB)}
		payload := []byte("keep me")
		m := buildMessage(t, idFor(1), targets, payload)

		m.ZeroToIDs()

		if got := m.ToIDAt(0); got != ([32]byte{}) {
			t.Fatalf("ToIDAt(0) after ZeroToIDs = %x, want zero value", got)
		}
		if got := m.ToIDAt(1); got != ([32]byte{}) {
			t.Fatalf("ToIDAt(1) after ZeroToIDs = %x, want zero value", got)
		}
		if got := m.FromID(); got != idFor(1) {
			t.Fatalf("FromID changed by ZeroToIDs: got %x", got)
		}
		if got := m.Payload(); !bytes.Equal(got, payload) {
			t.Fatalf("Payload changed by ZeroToIDs: got %q, want %q", got, payload)
		}
	})

	t.Run("declared count exceeds actual bytes present is a safe no-op", func(t *testing.T) {
		full := buildMessage(t, idFor(1), []([32]byte){idFor(2), idFor(3)}, nil)
		truncated := core.Message(full[:33+32]) // ToIDsLen says 2, only 1 id present
		before := append(core.Message(nil), truncated...)

		truncated.ZeroToIDs() // must not panic or write out of range

		if !bytes.Equal(truncated, before) {
			t.Fatalf("ZeroToIDs() mutated a truncated message instead of no-op'ing")
		}
	})
}
