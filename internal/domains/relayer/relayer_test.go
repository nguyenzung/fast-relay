package relayer_test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/domains/relayer"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

// fakeConnector implements core.Connector for tests that don't need a real
// network connection.
type fakeConnector struct {
	id     [32]byte
	pushed []core.OutMessage
}

func (f *fakeConnector) ID() [32]byte { return f.id }

func (f *fakeConnector) SafePush(msg core.OutMessage) bool {
	f.pushed = append(f.pushed, msg)
	return true
}

func (f *fakeConnector) Close() {}

var _ core.Connector = (*fakeConnector)(nil)

func idFor(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

// buildOutMessage encodes a well-formed frame into a fresh mem.Buffer and
// wraps it as the core.OutMessage shape HandleMessage receives in
// production (see network.readMessageWithFixedFromID): FromID(32) |
// ToIDsLen(1) | ToIDs(N*32) | DataLen(4) | Payload.
func buildOutMessage(t *testing.T, fromID [32]byte, toIDs [][32]byte, payload []byte) core.OutMessage {
	t.Helper()
	var raw bytes.Buffer
	raw.Write(fromID[:])
	raw.WriteByte(byte(len(toIDs)))
	for _, id := range toIDs {
		raw.Write(id[:])
	}
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(payload)))
	raw.Write(dataLen[:])
	raw.Write(payload)

	buf := mem.NewBuffer(raw.Len())
	copy(buf.Bytes(), raw.Bytes())
	return core.OutMessage{RecvTime: time.Now(), Buf: buf}
}

// TestHandleMessage_RelaysBufferedFromIDUnchanged is a regression test for
// where FromID trust now lives: network.readMessageWithFixedFromID is the
// only place that ever writes FromID, so by the time HandleMessage runs,
// m.Msg().FromID() is already the caller's authenticated identity.
// HandleMessage must relay it exactly as received, not touch it - unlike
// before this refactor, when Relayer itself was responsible for stamping it.
func TestHandleMessage_RelaysBufferedFromIDUnchanged(t *testing.T) {
	r := relayer.NewRelayer()

	sender := &fakeConnector{id: idFor(0xAA)}
	recipient := &fakeConnector{id: idFor(0xBB)}
	r.OnConnect(sender.ID(), sender)
	r.OnConnect(recipient.ID(), recipient)

	// Simulates what network.readMessageWithFixedFromID would have already
	// stamped: FromID == sender's authenticated identity.
	m := buildOutMessage(t, sender.ID(), [][32]byte{recipient.ID()}, []byte("hello"))

	r.HandleMessage(sender, m)

	if len(recipient.pushed) != 1 {
		t.Fatalf("recipient received %d messages, want 1", len(recipient.pushed))
	}
	if got := recipient.pushed[0].Msg().FromID(); got != sender.ID() {
		t.Fatalf("delivered FromID = %x, want %x (unchanged from what was buffered)", got, sender.ID())
	}

	// Two references are now outstanding: the caller's original one (m.Buf,
	// matching what internal/network releases after HandleMessage returns)
	// and the one DeliverTo retained on the recipient's behalf (matching
	// what the recipient's own write pump releases once written). Releasing
	// both should succeed; a third Release must panic (double free), which
	// is the only externally observable proof that Retain() actually ran
	// and that we are not leaking the first one.
	m.Buf.Release()
	recipient.pushed[0].Buf.Release()
	mustPanic(t, "Release after both refs freed", m.Buf.Release)
}

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

// TestHandleMessage_SelfExclusionUsesConnectorIdentity confirms self
// exclusion is driven by from.ID() (the connector's authenticated identity),
// not msg.FromID() (the buffered bytes). The two are always equal in
// production (see network.readMessageWithFixedFromID), but ExtractTargets is
// called with from.ID() specifically, so this pins that down.
func TestHandleMessage_SelfExclusionUsesConnectorIdentity(t *testing.T) {
	r := relayer.NewRelayer()

	sender := &fakeConnector{id: idFor(0xAA)}
	other := &fakeConnector{id: idFor(0xBB)}
	r.OnConnect(sender.ID(), sender)
	r.OnConnect(other.ID(), other)

	// Deliberately mismatched: the buffered FromID claims to be `other`, but
	// the connector making the call is `sender`. Self-exclusion must still
	// key off sender.ID(), so a message targeting sender.ID() itself is
	// dropped (excluded) regardless of what the buffered FromID says.
	m := buildOutMessage(t, other.ID(), [][32]byte{sender.ID()}, []byte("hello"))
	defer m.Buf.Release()

	r.HandleMessage(sender, m)

	if len(sender.pushed) != 0 {
		t.Fatalf("sender received %d messages, want 0 (self-excluded via from.ID())", len(sender.pushed))
	}
}
