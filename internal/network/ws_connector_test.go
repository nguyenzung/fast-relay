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

// fakeConn implements wsConn without touching a real socket. It lets
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

func (a *fakeApp) HandleMessage(from core.Connector, msg core.Message, buf *mem.Buffer, recvTime time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
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

// buildFrame encodes a minimal valid relay protocol frame:
// FromID(32) | ToIDsLen(1)=1 | ToIDs(32) | DataLen(4) | Data.
func buildFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(make([]byte, 32)) // FromID (unused by readMessage)
	buf.WriteByte(1)            // ToIDsLen = 1
	buf.Write(make([]byte, 32)) // one recipient id (all-zero for the test)
	var dataLen [4]byte
	binary.BigEndian.PutUint32(dataLen[:], uint32(len(data)))
	buf.Write(dataLen[:])
	buf.Write(data)
	return buf.Bytes()
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
	msg := core.OutMessage{Msg: core.Message(buf.Bytes()), RecvTime: time.Now(), Buf: buf}
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
