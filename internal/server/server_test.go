package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

// fakeApp implements core.App with just enough behavior for the handler
// tests below to observe what Server does with it, and to control what
// metricsHandler reports.
type fakeApp struct {
	mu            sync.Mutex
	connect       [][32]byte
	disconnect    [][32]byte
	count         int
	metrics       any
	closed        bool
	stopRecording bool
}

func (a *fakeApp) OnConnect(pubKey [32]byte, c core.Connector) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.connect = append(a.connect, pubKey)
}

func (a *fakeApp) OnDisconnect(pubKey [32]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.disconnect = append(a.disconnect, pubKey)
}

func (a *fakeApp) HandleMessage(from core.Connector, msg core.Message, buf *mem.Buffer, recvTime time.Time) {
}

func (a *fakeApp) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count
}

func (a *fakeApp) IncrementDeliverySuccess()     {}
func (a *fakeApp) IncrementDeliveryFailure()     {}
func (a *fakeApp) RecordLatency(d time.Duration) {}

func (a *fakeApp) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
}

func (a *fakeApp) StartRecording() {}

func (a *fakeApp) StopRecording() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopRecording = true
}

func (a *fakeApp) FetchMetrics() any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.metrics
}

func (a *fakeApp) connectedIDs() [][32]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][32]byte(nil), a.connect...)
}

func (a *fakeApp) disconnectedIDs() [][32]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][32]byte(nil), a.disconnect...)
}

func (a *fakeApp) wasClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

var _ core.App = (*fakeApp)(nil)

// mockAuth implements Authenticator with a canned result or error, and
// counts how many times Authenticate was invoked.
type mockAuth struct {
	result *AuthResult
	err    error
	calls  atomic.Int32
}

func (m *mockAuth) Authenticate(r *http.Request) (*AuthResult, error) {
	m.calls.Add(1)
	return m.result, m.err
}

// mockRegistrar implements Registrar with a canned result or error, and
// counts how many times Register was invoked.
type mockRegistrar struct {
	result *AuthResult
	err    error
	calls  atomic.Int32
}

func (m *mockRegistrar) Register(r *http.Request) (*AuthResult, error) {
	m.calls.Add(1)
	return m.result, m.err
}

// newTestServer builds a Server wired to the given fakes and registers
// cleanup so its background goroutines (cpuSampler, latency worker via app)
// don't leak across tests. Do not use it in tests that call Shutdown
// themselves (e.g. TestShutdown_WithOpenWebSocket) — Shutdown is not
// idempotent (it unconditionally closes stopSampler) and a second call
// would panic.
func newTestServer(t *testing.T, auth Authenticator, reg Registrar, app *fakeApp) *Server {
	t.Helper()
	s := NewServer(":0", 8, auth, reg, app)
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })
	return s
}

// waitForCondition polls cond until it returns true or timeout elapses,
// failing the test in the latter case. Needed wherever the assertion
// depends on work happening on a connection's own goroutine (e.g. the
// wsHandler goroutine spawned per accepted connection) rather than
// synchronously within the calling test goroutine.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestWSHandler_AuthFailure(t *testing.T) {
	authErr := errors.New("missing pub")
	app := &fakeApp{}
	auth := &mockAuth{err: authErr}
	s := newTestServer(t, auth, nil, app)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	s.wsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if got := strings.TrimSpace(w.Body.String()); got != authErr.Error() {
		t.Fatalf("body = %q, want %q", got, authErr.Error())
	}
	if len(app.connect) != 0 {
		t.Fatalf("OnConnect called %d times on auth failure, want 0", len(app.connect))
	}
	if got := auth.calls.Load(); got != 1 {
		t.Fatalf("Authenticate called %d times, want exactly 1", got)
	}
}

// TestWSHandler_AuthSuccess_ConnectDisconnect drives wsHandler through a real
// WebSocket upgrade (httptest.NewServer + a real client Dial — httptest's
// ResponseRecorder can't hijack, so the auth-success path needs an actual
// hijackable connection). It verifies the full lifecycle: successful auth
// registers the connector via OnConnect, and once the client closes the
// connection, ReadWriteLoop's defer chain fires OnDisconnect for the same
// pubKey.
func TestWSHandler_AuthSuccess_ConnectDisconnect(t *testing.T) {
	var pubKey [32]byte
	pubKey[0] = 0xCD
	app := &fakeApp{}
	auth := &mockAuth{result: &AuthResult{PubKey: pubKey, UserID: "u1"}}
	s := newTestServer(t, auth, nil, app)

	ts := httptest.NewServer(http.HandlerFunc(s.wsHandler))
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool { return len(app.connectedIDs()) == 1 })
	if got := app.connectedIDs()[0]; got != pubKey {
		t.Fatalf("OnConnect pubKey = %x, want %x", got, pubKey)
	}
	if got := auth.calls.Load(); got != 1 {
		t.Fatalf("Authenticate called %d times, want exactly 1", got)
	}

	if err := conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("client close: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool { return len(app.disconnectedIDs()) == 1 })
	if got := app.disconnectedIDs()[0]; got != pubKey {
		t.Fatalf("OnDisconnect pubKey = %x, want %x", got, pubKey)
	}
}

// TestShutdown_WithOpenWebSocket verifies that Shutdown returns promptly even
// while a client's WebSocket connection is still open. A WebSocket upgrade
// hijacks the underlying TCP connection, so it falls outside what
// http.Server tracks as an "active request" — Shutdown must not block on it.
// This exercises Server.Start's actual *http.Server (via a manually attached
// listener) rather than a bare handler, since that's what makes Shutdown's
// interaction with a live hijacked connection observable.
func TestShutdown_WithOpenWebSocket(t *testing.T) {
	var pubKey [32]byte
	pubKey[0] = 0xEE
	app := &fakeApp{}
	auth := &mockAuth{result: &AuthResult{PubKey: pubKey, UserID: "u1"}}
	s := NewServer("127.0.0.1:0", 8, auth, nil, app)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.srv.Serve(ln) }()

	wsURL := "ws://" + ln.Addr().String() + "/"
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusInternalError, "test cleanup")

	waitForCondition(t, 2*time.Second, func() bool { return len(app.connectedIDs()) == 1 })

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(shutdownCtx) }()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Shutdown did not return within 2s while a WebSocket connection was open " +
			"(a hijacked connection must not block Shutdown)")
	}

	if !app.wasClosed() {
		t.Fatalf("app.Close() was not called by Shutdown")
	}

	<-serveDone // Serve must return (http.ErrServerClosed) once Shutdown closed the listener
}

func TestRegisterHandler_Success(t *testing.T) {
	var pubKey [32]byte
	pubKey[0] = 0xAB
	app := &fakeApp{}
	reg := &mockRegistrar{result: &AuthResult{PubKey: pubKey, UserID: "u1"}}
	s := newTestServer(t, nil, reg, app)

	req := httptest.NewRequest(http.MethodPost, "/register", nil)
	w := httptest.NewRecorder()

	s.registerHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	want := map[string]string{
		"status":  "success",
		"pub_key": hex.EncodeToString(pubKey[:]),
	}
	if got["status"] != want["status"] || got["pub_key"] != want["pub_key"] {
		t.Fatalf("body = %+v, want %+v", got, want)
	}
	if got := reg.calls.Load(); got != 1 {
		t.Fatalf("Register called %d times, want exactly 1", got)
	}
}

func TestRegisterHandler_Error(t *testing.T) {
	app := &fakeApp{}
	reg := &mockRegistrar{err: errors.New("boom")}
	s := newTestServer(t, nil, reg, app)

	req := httptest.NewRequest(http.MethodPost, "/register", nil)
	w := httptest.NewRecorder()

	s.registerHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "internal server error" {
		t.Fatalf("body = %q, want %q", got, "internal server error")
	}
	if got := reg.calls.Load(); got != 1 {
		t.Fatalf("Register called %d times, want exactly 1", got)
	}
}

func TestMetricsHandler(t *testing.T) {
	app := &fakeApp{
		count:   7,
		metrics: map[string]any{"processed": float64(42)},
	}
	s := newTestServer(t, nil, nil, app)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	s.metricsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}

	if ac, ok := got["active_connections"].(float64); !ok || int(ac) != app.count {
		t.Fatalf("active_connections = %v, want %d", got["active_connections"], app.count)
	}
	for _, key := range []string{"goroutines", "alloc_bytes", "cpu_percent", "uptime_seconds", "app_metrics"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("response missing key %q: %+v", key, got)
		}
	}
	appMetrics, ok := got["app_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("app_metrics = %v (%T), want map[string]any", got["app_metrics"], got["app_metrics"])
	}
	if processed, ok := appMetrics["processed"].(float64); !ok || processed != 42 {
		t.Fatalf("app_metrics.processed = %v, want 42", appMetrics["processed"])
	}
}

// TestMetricsHandler_MarshalFailure verifies that when FetchMetrics returns
// a value json.Marshal cannot encode (here, a func value), the handler
// reports a proper 500 instead of silently succeeding with an empty body.
func TestMetricsHandler_MarshalFailure(t *testing.T) {
	app := &fakeApp{
		count:   1,
		metrics: func() {}, // json.Marshal rejects func values
	}
	s := newTestServer(t, nil, nil, app)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	s.metricsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "internal server error" {
		t.Fatalf("body = %q, want %q", got, "internal server error")
	}
}
