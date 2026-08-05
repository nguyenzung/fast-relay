// Package e2e runs the relayer server in-process against real WebSocket
// clients to verify end-to-end message correctness AND liveness under churn
// (clients repeatedly connecting/disconnecting) - not just "some messages
// got through".
//
// Run explicitly (it is ~2 minutes and heavier than a unit test):
//
//	go test ./e2e/... -run TestChurnCorrectness -v -timeout 5m
//
// Skip it (e.g. as part of a fast default `go test ./...`) with:
//
//	go test -short ./...
package e2e

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/domains"
	"github.com/nguyenzung/relayer-server/internal/server"
)

const (
	churnClientCount = 200
	churnDuration    = 2 * time.Minute
	churnMsgRate     = 2 // messages/sec per client while online
	churnOutBuf      = 64
	churnOnMin       = 5 * time.Second
	churnOnMax       = 15 * time.Second
	churnOffMin      = 2 * time.Second
	churnOffMax      = 5 * time.Second
	churnPayloadLen  = 64 // [senderIdx:4][seq:8][random:44][crc32:4]

	// Health/liveness floors. Deliberately loose to avoid flakiness under
	// machine load, but tight enough to catch order-of-magnitude
	// regressions - most clients can't connect, reconnect is broken, most
	// messages are silently dropped, or the registry leaks. A real 3min run
	// observed ~100% connect rate, ~9x (re)connects/client, and ~75%
	// delivery ratio; the floors below sit well under those baselines.
	minConnectRate     = 0.95 // fraction of clients that must connect at least once
	minReconnectFactor = 1.3  // total successful (re)connects must be >= this * client count
	minDeliveryRatio   = 0.25 // recv/sent floor

	healthCheckInterval = 5 * time.Second
	healthCheckFailMax  = 3 // consecutive /metrics failures before declaring the server unresponsive
)

// sentKey identifies one sent message by its sender and per-sender sequence
// number, so a receiver can look up exactly what was sent to it.
type sentKey struct {
	senderIdx uint32
	seq       uint64
}

// sentRecord is what the sender promised: who the message was actually
// addressed to. delivered guards against double-counting a duplicate.
type sentRecord struct {
	targetIdx uint32
	delivered atomic.Bool
}

// TestChurnCorrectness runs ~1000 clients against a real relayer server for
// ~2 minutes, each cycling online/offline (churn) while exchanging targeted
// messages, and verifies both:
//
//  1. Liveness - the server stays up and responsive for the whole run, most
//     clients can connect and reconnect repeatedly, and most sent messages
//     actually get delivered. This does NOT require zero loss (churn causes
//     real, expected loss) but catches "the system is effectively dead"
//     regressions that a bare "recv > 0" check would miss.
//  2. Correctness - every message actually received by a client: was not
//     corrupted in transit (checksum), genuinely came from the FromID it
//     claims (identity), was actually addressed to this recipient and not
//     some other client (no misrouting), and was delivered exactly once (no
//     duplicate delivery).
//
// It also asserts the connector registry drains back to zero after all
// clients disconnect (no leak) and that server Shutdown succeeds.
func TestChurnCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy churn e2e test in -short mode")
	}
	testStart := time.Now()

	addr, serverDied, shutdown := startTestServer(t, churnOutBuf)
	defer shutdown()

	pubs := make([][32]byte, churnClientCount)
	pubsHex := make([]string, churnClientCount)
	for i := range pubs {
		s := fmt.Sprintf("E2ECHURN-%d", i)
		var b [32]byte
		copy(b[:], s)
		pubs[i] = b
		pubsHex[i] = hex.EncodeToString(b[:])
	}

	var totalSent, totalRecv atomic.Uint64
	var totalConnectSuccess atomic.Uint64
	connectedOnce := make([]atomic.Bool, churnClientCount)
	var sentMessages sync.Map // sentKey -> *sentRecord

	var failureCount atomic.Uint64
	var mu sync.Mutex
	var failures []string
	recordFailure := func(format string, args ...interface{}) {
		failureCount.Add(1)
		mu.Lock()
		defer mu.Unlock()
		if len(failures) < 30 {
			failures = append(failures, fmt.Sprintf(format, args...))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), churnDuration)

	var wg sync.WaitGroup
	wg.Add(churnClientCount)
	// Safety net: if the test fails fast (t.Fatalf from the select below),
	// these still run in this order (LIFO: cancel, then wg.Wait, then the
	// shutdown deferred above) so clients unwind and the server is stopped
	// cleanly instead of leaking goroutines/connections past the test.
	defer wg.Wait()
	defer cancel()

	healthFail := watchServerHealth(ctx, addr)

	for i := 0; i < churnClientCount; i++ {
		i := i
		go func() {
			defer wg.Done()
			runChurnCorrectnessClient(ctx, addr, pubs, pubsHex, i, &sentMessages, &totalSent, &totalRecv, &totalConnectSuccess, &connectedOnce[i], recordFailure)
		}()
		if i%100 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	select {
	case <-ctx.Done():
	case err := <-serverDied:
		t.Fatalf("server process died mid-test: %v", err)
	case err := <-healthFail:
		t.Fatalf("server became unresponsive mid-test: %v", err)
	}
	wg.Wait()

	sentN := totalSent.Load()
	recvN := totalRecv.Load()
	connects := totalConnectSuccess.Load()

	neverConnected := 0
	for i := range connectedOnce {
		if !connectedOnce[i].Load() {
			neverConnected++
		}
	}
	connectRate := float64(churnClientCount-neverConnected) / float64(churnClientCount)
	reconnectFactor := float64(connects) / float64(churnClientCount)

	deliveryRatio := 0.0
	if sentN > 0 {
		deliveryRatio = float64(recvN) / float64(sentN)
	}

	// Registry-leak check: every client has closed its connection by the
	// time wg.Wait() returned above. Give the server a short grace period to
	// process the resulting OnDisconnect calls, then verify the registry
	// actually drained instead of leaking entries.
	activeAfter := -1.0
	drained := false
	drainDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(drainDeadline) {
		if m, err := fetchMetrics(addr); err == nil {
			if v, ok := m["active_connections"]; ok {
				activeAfter = v
				if v == 0 {
					drained = true
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// --- Stats summary (printed regardless of pass/fail) ---
	t.Logf("=== churn e2e stats ===")
	t.Logf("clients=%d configured_duration=%s wall_elapsed=%s", churnClientCount, churnDuration, time.Since(testStart).Round(time.Second))
	t.Logf("sent=%d recv=%d delivery_ratio=%.1f%% (floor %.0f%%)", sentN, recvN, deliveryRatio*100, minDeliveryRatio*100)
	t.Logf("connect_rate=%.1f%% (%d/%d clients connected at least once, floor %.0f%%)", connectRate*100, churnClientCount-neverConnected, churnClientCount, minConnectRate*100)
	t.Logf("total_connects=%d (%.2fx client count, floor %.1fx)", connects, reconnectFactor, minReconnectFactor)
	t.Logf("registry_drained=%v active_connections_after=%.0f (grace period %s)", drained, activeAfter, 5*time.Second)
	t.Logf("correctness_failures=%d", failureCount.Load())

	// --- Liveness assertions ---

	if connectRate < minConnectRate {
		t.Errorf("only %.1f%% of clients ever connected successfully (want >= %.0f%%); %d/%d client(s) never connected even once",
			connectRate*100, minConnectRate*100, neverConnected, churnClientCount)
	}
	if reconnectFactor < minReconnectFactor {
		t.Errorf("too few successful (re)connects across the fleet: %d (want >= %.0f, %.1fx client count) - reconnect/churn cycling may be broken",
			connects, float64(churnClientCount)*minReconnectFactor, minReconnectFactor)
	}
	if sentN > 0 && deliveryRatio < minDeliveryRatio {
		t.Errorf("delivery ratio too low: recv/sent=%.3f (recv=%d sent=%d), want >= %.2f - suggests most messages are not being routed, not just expected churn loss",
			deliveryRatio, recvN, sentN, minDeliveryRatio)
	}
	if recvN == 0 {
		t.Fatalf("no messages were received at all - test harness is likely broken (server never started, or all dials failed)")
	}
	if !drained {
		t.Errorf("connector registry did not drain after all clients disconnected: active_connections=%.0f after 5s grace period (possible leak in OnDisconnect)", activeAfter)
	}

	// --- Correctness assertions ---

	mu.Lock()
	defer mu.Unlock()
	if failureCount.Load() > 0 {
		for _, f := range failures {
			t.Error(f)
		}
		t.Errorf("%d correctness failures detected among %d received messages (showing up to %d)", failureCount.Load(), recvN, len(failures))
	}
}

// runChurnCorrectnessClient repeatedly connects, stays online sending at
// churnMsgRate for a random duration, then disconnects for a random
// duration, until ctx is cancelled. Mirrors cmd/churntest's churn pattern.
func runChurnCorrectnessClient(
	ctx context.Context,
	addr string,
	pubs [][32]byte,
	pubsHex []string,
	idx int,
	sentMessages *sync.Map,
	totalSent, totalRecv, totalConnectSuccess *atomic.Uint64,
	connectedOnce *atomic.Bool,
	recordFailure func(string, ...interface{}),
) {
	r := rand.New(rand.NewSource(int64(idx) + 1))
	n := len(pubs)
	pub := pubs[idx]
	pubHex := pubsHex[idx]
	var seq uint64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		u := url.URL{Scheme: "ws", Host: addr, Path: "/"}
		q := u.Query()
		q.Set("pub", pubHex)
		u.RawQuery = q.Encode()

		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := websocket.Dial(dctx, u.String(), nil)
		dcancel()
		if err != nil {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		totalConnectSuccess.Add(1)
		connectedOnce.Store(true)

		onDuration := churnOnMin + time.Duration(r.Int63n(int64(churnOnMax-churnOnMin)+1))
		onlineCtx, onlineCancel := context.WithCancel(ctx)

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			for {
				mt, data, err := conn.Read(onlineCtx)
				if err != nil {
					return
				}
				if mt != websocket.MessageBinary {
					continue
				}
				verifyReceived(idx, pubs, data, sentMessages, recordFailure)
				totalRecv.Add(1)
			}
		}()

		interval := time.Second / time.Duration(churnMsgRate)
		ticker := time.NewTicker(interval)
		onTimer := time.NewTimer(onDuration)
	sendLoop:
		for {
			select {
			case <-ctx.Done():
				break sendLoop
			case <-onTimer.C:
				break sendLoop
			case <-ticker.C:
				if n <= 1 {
					continue
				}
				j := r.Intn(n - 1)
				targetIdx := j
				if j >= idx {
					targetIdx = j + 1
				}
				target := pubs[targetIdx]

				mySeq := seq
				seq++

				payload := make([]byte, churnPayloadLen)
				binary.BigEndian.PutUint32(payload[0:4], uint32(idx))
				binary.BigEndian.PutUint64(payload[4:12], mySeq)
				for k := 12; k < churnPayloadLen-4; k++ {
					payload[k] = byte(r.Intn(256))
				}
				crc := crc32.ChecksumIEEE(payload[:churnPayloadLen-4])
				binary.BigEndian.PutUint32(payload[churnPayloadLen-4:], crc)

				// Record intent BEFORE writing to the socket, so a receiver
				// can never observe the message before its record exists.
				sentMessages.Store(sentKey{senderIdx: uint32(idx), seq: mySeq}, &sentRecord{targetIdx: uint32(targetIdx)})

				var buf []byte
				buf = append(buf, pub[:]...)
				buf = append(buf, 1)
				buf = append(buf, target[:]...)
				var lenb [4]byte
				binary.BigEndian.PutUint32(lenb[:], uint32(len(payload)))
				buf = append(buf, lenb[:]...)
				buf = append(buf, payload...)

				if err := conn.Write(ctx, websocket.MessageBinary, buf); err != nil {
					break sendLoop
				}
				totalSent.Add(1)
			}
		}

		onlineCancel()
		ticker.Stop()
		_ = conn.Close(websocket.StatusNormalClosure, "churn cycle off")
		<-readDone

		offDuration := churnOffMin + time.Duration(r.Int63n(int64(churnOffMax-churnOffMin)+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(offDuration):
		}
	}
}

// verifyReceived checks one inbound frame against the record its sender
// stored when it was sent, reporting any mismatch via recordFailure.
func verifyReceived(myIdx int, pubs [][32]byte, data []byte, sentMessages *sync.Map, recordFailure func(string, ...interface{})) {
	msg := core.Message(data)
	payload := msg.Payload()
	if len(payload) != churnPayloadLen {
		recordFailure("client %d: received frame with unexpected payload length %d (want %d)", myIdx, len(payload), churnPayloadLen)
		return
	}

	body := payload[:churnPayloadLen-4]
	wantCRC := binary.BigEndian.Uint32(payload[churnPayloadLen-4:])
	if crc32.ChecksumIEEE(body) != wantCRC {
		recordFailure("client %d: checksum mismatch - payload corrupted in transit", myIdx)
		return
	}

	senderIdx := binary.BigEndian.Uint32(payload[0:4])
	seq := binary.BigEndian.Uint64(payload[4:12])

	if int(senderIdx) >= len(pubs) {
		recordFailure("client %d: received message claiming out-of-range senderIdx=%d", myIdx, senderIdx)
		return
	}
	if msg.FromID() != pubs[senderIdx] {
		recordFailure("client %d: FromID does not match claimed senderIdx=%d (identity mismatch)", myIdx, senderIdx)
		return
	}

	v, ok := sentMessages.Load(sentKey{senderIdx: senderIdx, seq: seq})
	if !ok {
		recordFailure("client %d: received message (sender=%d seq=%d) with no matching sent record - phantom message", myIdx, senderIdx, seq)
		return
	}
	rec := v.(*sentRecord)
	if rec.targetIdx != uint32(myIdx) {
		recordFailure("client %d: MISROUTED - received message intended for client %d (sender=%d seq=%d)", myIdx, rec.targetIdx, senderIdx, seq)
		return
	}
	if !rec.delivered.CompareAndSwap(false, true) {
		recordFailure("client %d: received duplicate delivery of message sender=%d seq=%d", myIdx, senderIdx, seq)
		return
	}
}

// watchServerHealth polls /metrics every healthCheckInterval and reports on
// the returned channel if it fails healthCheckFailMax times in a row -
// catching a server that hangs/stops responding without its process exiting
// (which serverDied from startTestServer would not detect).
func watchServerHealth(ctx context.Context, addr string) <-chan error {
	fail := make(chan error, 1)
	go func() {
		consecutive := 0
		ticker := time.NewTicker(healthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := fetchMetrics(addr); err != nil {
					consecutive++
				} else {
					consecutive = 0
				}
				if consecutive >= healthCheckFailMax {
					select {
					case fail <- fmt.Errorf("/metrics unresponsive for %d consecutive checks (%s apart): server appears hung", consecutive, healthCheckInterval):
					default:
					}
					return
				}
			}
		}
	}()
	return fail
}

func fetchMetrics(addr string) (map[string]float64, error) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	// "app_metrics" (and any other App-defined field) is opaque and not
	// necessarily numeric - keep only the flat numeric fields this test
	// actually asserts on (e.g. "active_connections").
	m := make(map[string]float64, len(raw))
	for k, v := range raw {
		if f, ok := v.(float64); ok {
			m[k] = f
		}
	}
	return m, nil
}

// startTestServer boots a real server.Server (backed by a fresh
// domains.Relayer, the default core.App) on an OS-assigned local port.
// serverDied fires if the server's Start() goroutine exits with an
// unexpected error at any point during the test, not just at startup.
func startTestServer(t *testing.T, outBuf int) (addr string, serverDied <-chan error, shutdown func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a local port: %v", err)
	}
	addr = ln.Addr().String()
	_ = ln.Close()

	app := domains.NewRelayer()
	srv := server.NewServer(addr, outBuf, nil, nil, app)

	died := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			died <- err
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		select {
		case err := <-died:
			t.Fatalf("server failed to start: %v", err)
		default:
		}
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			_ = resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("server did not become ready on %s within timeout", addr)
	}

	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("server shutdown failed or hung: %v", err)
		}
	}
	return addr, died, shutdown
}
