package core_test

import (
	"testing"
	"time"

	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

// fakeConnector implements core.Connector for tests that don't need a real
// network connection.
type fakeConnector struct {
	id     [32]byte
	accept bool
	pushed []core.OutMessage
	closed bool
}

func (f *fakeConnector) ID() [32]byte { return f.id }

func (f *fakeConnector) SafePush(msg core.OutMessage) bool {
	if !f.accept {
		return false
	}
	f.pushed = append(f.pushed, msg)
	return true
}

func (f *fakeConnector) Close() { f.closed = true }

var _ core.Connector = (*fakeConnector)(nil)

// mustPanic fails the test unless fn panics.
func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s: expected panic, got none", name)
		}
	}()
	fn()
}

func TestExtractTargets(t *testing.T) {
	self := idFor(0xFF)

	t.Run("excludes self, keeps the rest in order", func(t *testing.T) {
		other1, other2 := idFor(1), idFor(2)
		m := buildMessage(t, idFor(9), [][32]byte{other1, self, other2}, nil)

		var dst [core.MaxTargetsPerMessage][32]byte
		n := core.ExtractTargets(m, self, &dst)

		if n != 2 {
			t.Fatalf("n = %d, want 2", n)
		}
		if dst[0] != other1 || dst[1] != other2 {
			t.Fatalf("dst[:2] = %x, want [%x %x]", dst[:2], other1, other2)
		}
	})

	t.Run("excludes every occurrence of self", func(t *testing.T) {
		other := idFor(1)
		m := buildMessage(t, idFor(9), [][32]byte{self, self, other, self}, nil)

		var dst [core.MaxTargetsPerMessage][32]byte
		n := core.ExtractTargets(m, self, &dst)

		if n != 1 || dst[0] != other {
			t.Fatalf("n=%d dst[0]=%x, want n=1 dst[0]=%x", n, dst[0], other)
		}
	})

	t.Run("no recipients yields zero targets", func(t *testing.T) {
		m := buildMessage(t, idFor(9), nil, nil)

		var dst [core.MaxTargetsPerMessage][32]byte
		n := core.ExtractTargets(m, self, &dst)

		if n != 0 {
			t.Fatalf("n = %d, want 0", n)
		}
	})

	t.Run("no self present keeps all recipients", func(t *testing.T) {
		targets := [][32]byte{idFor(1), idFor(2), idFor(3)}
		m := buildMessage(t, idFor(9), targets, nil)

		var dst [core.MaxTargetsPerMessage][32]byte
		n := core.ExtractTargets(m, self, &dst)

		if n != len(targets) {
			t.Fatalf("n = %d, want %d", n, len(targets))
		}
		for i, want := range targets {
			if dst[i] != want {
				t.Fatalf("dst[%d] = %x, want %x", i, dst[i], want)
			}
		}
	})
}

func TestDeliverTo(t *testing.T) {
	t.Run("success retains one extra reference the recipient owns", func(t *testing.T) {
		dest := &fakeConnector{id: idFor(1), accept: true}
		m := buildMessage(t, idFor(9), nil, []byte("payload"))
		buf := mem.NewBuffer(4)
		recvTime := time.Now()

		ok := core.DeliverTo(dest, m, buf, recvTime)
		if !ok {
			t.Fatalf("DeliverTo() = false, want true")
		}
		if len(dest.pushed) != 1 {
			t.Fatalf("dest received %d messages, want 1", len(dest.pushed))
		}
		got := dest.pushed[0]
		if got.Buf != buf || got.RecvTime != recvTime {
			t.Fatalf("pushed OutMessage = %+v, want Buf=%p RecvTime=%v", got, buf, recvTime)
		}

		// Two references are now outstanding: the caller's original one and
		// the one DeliverTo retained on the recipient's behalf. Releasing
		// both should succeed; a third Release must panic (double free),
		// which is the only externally observable proof that Retain() ran.
		buf.Release()
		buf.Release()
		mustPanic(t, "Release after both refs freed", buf.Release)
	})

	t.Run("dropped push undoes the retain, leaving refcount unchanged", func(t *testing.T) {
		dest := &fakeConnector{id: idFor(1), accept: false}
		m := buildMessage(t, idFor(9), nil, nil)
		buf := mem.NewBuffer(4)

		ok := core.DeliverTo(dest, m, buf, time.Now())
		if ok {
			t.Fatalf("DeliverTo() = true, want false")
		}
		if len(dest.pushed) != 0 {
			t.Fatalf("dest received %d messages, want 0", len(dest.pushed))
		}

		// Only the caller's original reference should remain outstanding.
		buf.Release()
		mustPanic(t, "Release after the single ref is freed", buf.Release)
	})

	t.Run("safe to call repeatedly against the same buffer", func(t *testing.T) {
		destA := &fakeConnector{id: idFor(1), accept: true}
		destB := &fakeConnector{id: idFor(2), accept: true}
		m := buildMessage(t, idFor(9), nil, nil)
		buf := mem.NewBuffer(4)

		if !core.DeliverTo(destA, m, buf, time.Now()) {
			t.Fatalf("DeliverTo(destA) = false, want true")
		}
		if !core.DeliverTo(destB, m, buf, time.Now()) {
			t.Fatalf("DeliverTo(destB) = false, want true")
		}

		// Three references outstanding now: original + one per recipient.
		buf.Release()
		buf.Release()
		buf.Release()
		mustPanic(t, "Release after all three refs freed", buf.Release)
	})
}
