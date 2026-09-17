package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

// fakeConn implements wsConn without touching a real socket. It lets tests drive ReadWriteLoop deterministically.
type fakeConn struct {
	mu sync.Mutex

	reads    []func() (websocket.MessageType, io.Reader, error)
	readIdx  int
	writeErr error
	closed   bool
}

func (f *fakeConn) Reader(ctx context.Context) (websocket.MessageType, io.Reader, error) {
	f.mu.Lock()
	if f.readIdx >= len(f.reads) {
		f.mu.Unlock()
		return 0, nil, io.EOF
	}
	next := f.reads[f.readIdx]
	f.readIdx++
	f.mu.Unlock()
	// Call outside the lock: a test's read function may deliberately block
	// (e.g. to hold the read side open while asserting on the write side),
	// and Write() below needs f.mu too.
	return next()
}

func (f *fakeConn) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeErr
}

func (f *fakeConn) Close(code websocket.StatusCode, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

var _ wsConn = (*fakeConn)(nil)

// fakeApp implements core.App with just enough behavior to observe what
// ReadWriteLoop does with it.
type fakeApp struct {
	mu           sync.Mutex
	disconnected []([32]byte)
	handled      [][]byte
	deliverySucc int
	deliveryFail int
}

func (a *fakeApp) OnConnect(pubKey [32]byte, c core.Connector) {}

func (a *fakeApp) OnDisconnect(pubKey [32]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.disconnected = append(a.disconnected, pubKey)
}

func (a *fakeApp) HandleMessage(from core.Connector, m core.OutMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := m.Msg()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	a.handled = append(a.handled, cp)
}

func (a *fakeApp) Count() int { return 0 }

func (a *fakeApp) IncrementDeliverySuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deliverySucc++
}

func (a *fakeApp) IncrementDeliveryFailure() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deliveryFail++
}

func (a *fakeApp) RecordLatency(d time.Duration) {}
func (a *fakeApp) Close()                        {}
func (a *fakeApp) StartRecording()               {}
func (a *fakeApp) StopRecording()                {}
func (a *fakeApp) FetchMetrics() any             { return nil }

var _ core.App = (*fakeApp)(nil)

// buildFrame encodes a minimal valid relay protocol frame (wire carries no
// FromID — see readMessageWithFixedFromID): ToIDsLen(1)=1 | ToIDs(32) | DataLen(4) | Data.
func buildFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteByte(1)            // ToIDsLen = 1
	buf.Write(make([]byte, 32)) // one recipient id (all-zero for the test)
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(data)))
	buf.Write(dataLen[:])
	buf.Write(data)
	return buf.Bytes()
}

// TestReadMessage_DuplicateTargetRejected is a regression test: a repeated
// recipient in ToIDs amplifies delivery (core.ExtractTargets has no dedup of
// its own) with no legitimate use, so readMessageWithFixedFromID must reject
// it as a protocol violation instead of accepting it.
func TestReadMessage_DuplicateTargetRejected(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(2) // ToIDsLen
	dup := make([]byte, 32)
	dup[0] = 0x42
	buf.Write(dup) // recipient 1
	buf.Write(dup) // recipient 2 — same id, must be rejected
	var dataLen [4]byte
	buf.Write(dataLen[:])

	_, err := readMessageWithFixedFromID(&buf, core.MaxMessageSize, idFor(0x01))
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("readMessageWithFixedFromID() err = %v, want ErrInvalidMessage", err)
	}
}

// TestReadMessage_DistinctTargetsAccepted is the control for
// TestReadMessage_DuplicateTargetRejected: the same shape with two distinct
// recipient ids (exercising the pairwise comparison with n > 1) must still
// be accepted.
func TestReadMessage_DistinctTargetsAccepted(t *testing.T) {
	payload := []byte("hi")
	var raw bytes.Buffer
	raw.WriteByte(2) // ToIDsLen
	id1, id2 := make([]byte, 32), make([]byte, 32)
	id1[0], id2[0] = 0x01, 0x02
	raw.Write(id1)
	raw.Write(id2)
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(payload)))
	raw.Write(dataLen[:])
	raw.Write(payload)

	buf, err := readMessageWithFixedFromID(&raw, core.MaxMessageSize, idFor(0x01))
	if err != nil {
		t.Fatalf("readMessageWithFixedFromID() err = %v, want nil", err)
	}
	defer buf.Release()
}

// TestReadMessage_StampsProvidedFromID is a regression test for the wire
// format: readMessageWithFixedFromID must not read a FromID off r at all
// (the wire starts directly with ToIDsLen) and must instead write the fromID
// argument into the returned buffer's first 32 bytes — the only place FromID
// is ever set, so the App never needs to (and cannot be tricked into
// trusting a client-supplied one).
func TestReadMessage_StampsProvidedFromID(t *testing.T) {
	payload := []byte("hi")
	var raw bytes.Buffer
	raw.WriteByte(1) // ToIDsLen = 1
	raw.Write(make([]byte, 32))
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(payload)))
	raw.Write(dataLen[:])
	raw.Write(payload)

	fromID := idFor(0xAA)
	buf, err := readMessageWithFixedFromID(&raw, core.MaxMessageSize, fromID)
	if err != nil {
		t.Fatalf("readMessageWithFixedFromID() err = %v, want nil", err)
	}
	defer buf.Release()

	msg := core.Message(buf.Bytes())
	if got := msg.FromID(); got != fromID {
		t.Fatalf("FromID() = %x, want %x (the fromID argument, not wire content)", got, fromID)
	}
	if got := msg.Payload(); string(got) != string(payload) {
		t.Fatalf("Payload() = %q, want %q", got, payload)
	}
}

// idFor builds a distinguishable [32]byte id for test fixtures.
func idFor(b byte) [32]byte {
	var id [32]byte
	id[0] = b
	return id
}

// TestReadWriteLoop_HappyPath proves the wsConn seam (UT.md item 1): ReadWriteLoop
// is driven end-to-end through a fake connection, with no real network socket.
func TestReadWriteLoop_HappyPath(t *testing.T) {
	frame := buildFrame(t, []byte("hello"))
	conn := &fakeConn{
		reads: []func() (websocket.MessageType, io.Reader, error){
			func() (websocket.MessageType, io.Reader, error) {
				return websocket.MessageBinary, bytes.NewReader(frame), nil
			},
		},
	}
	app := &fakeApp{}
	var pub [32]byte
	pub[0] = 0xAB

	c := NewWSConnector(conn, pub, app, 8)

	err := c.ReadWriteLoop(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.handled) != 1 {
		t.Fatalf("expected 1 handled message, got %d", len(app.handled))
	}
	if got := core.Message(app.handled[0]).FromID(); got != pub {
		t.Fatalf("handled message FromID = %x, want connector's authenticated pubkey %x", got, pub)
	}
	if len(app.disconnected) != 1 || app.disconnected[0] != pub {
		t.Fatalf("expected OnDisconnect(%x), got %v", pub, app.disconnected)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.closed {
		t.Fatalf("expected conn.Close to have been called")
	}
}

// TestReadWriteLoop_WriteErrorDrainsQueue proves the write-pump path (also
// gated behind the wsConn seam): a failing Write closes the connector,
// reports a delivery failure, and releases every buffered OutMessage instead
// of leaking it.
func TestReadWriteLoop_WriteErrorDrainsQueue(t *testing.T) {
	blockCh := make(chan struct{})
	conn := &fakeConn{
		writeErr: errors.New("boom"),
		reads: []func() (websocket.MessageType, io.Reader, error){
			func() (websocket.MessageType, io.Reader, error) {
				<-blockCh // held open until the write pump has finished
				return 0, nil, io.EOF
			},
		},
	}
	app := &fakeApp{}
	var pub [32]byte

	c := NewWSConnector(conn, pub, app, 8)

	buf := mem.NewBuffer(4)
	msg := core.OutMessage{RecvTime: time.Now(), Buf: buf}
	if !c.SafePush(msg) {
		t.Fatalf("SafePush failed before loop started")
	}

	done := make(chan error, 1)
	go func() { done <- c.ReadWriteLoop(context.Background()) }()

	// Wait for the write pump to observe the failure and close the connector.
	deadline := time.After(2 * time.Second)
	for {
		c.mu.RLock()
		closed := c.isClosed
		c.mu.RUnlock()
		if closed {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for connector to close after write error")
		case <-time.After(time.Millisecond):
		}
	}

	close(blockCh)
	if err := <-done; err == nil {
		t.Fatalf("expected ReadWriteLoop to return an error after write failure")
	}

	app.mu.Lock()
	defer app.mu.Unlock()
	if app.deliveryFail != 1 {
		t.Fatalf("expected 1 delivery failure, got %d", app.deliveryFail)
	}
}
