package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// High-performance load test tool for the WebSocket relayer.
// Launches N concurrent connectors (C1..CN), each sends M messages/sec to random target.

func main() {
	addr := flag.String("addr", "localhost:8080", "server address host:port")
	n := flag.Int("n", 22000, "number of concurrent connectors")
	m := flag.Int("m", 5, "messages per second per connector")
	dur := flag.Duration("duration", 0, "test duration (0 = until SIGINT)")
	outBuf := flag.Int("outbuf", 64, "per-connection outbound buffer size")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	flag.Parse()

	log.Printf("loadtest start addr=%s N=%d M=%d outBuf=%d seed=%d", *addr, *n, *m, *outBuf, *seed)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("signal received, initiating shutdown")
		cancel()
	}()

	// Prepare pub keys
	pubs := make([][32]byte, *n)
	pubsStr := make([]string, *n)
	for i := 0; i < *n; i++ {
		// create a 32-byte id with human prefix, encode as hex for server
		s := fmt.Sprintf("C%d", i+1)
		var b [32]byte
		copy(b[:], []byte(s))
		pubs[i] = b
		pubsStr[i] = hex.EncodeToString(b[:])
	}

	var totalSent atomic.Uint64
	var totalRecv atomic.Uint64
	var totalErr atomic.Uint64

	wg := sync.WaitGroup{}
	wg.Add(*n)

	// start connectors with jitter to avoid thundering herd
	for i := 0; i < *n; i++ {
		i := i
		go func() {
			defer wg.Done()
			runConnector(ctx, *addr, pubs, pubsStr, i, *m, *outBuf, *seed, &totalSent, &totalRecv, &totalErr)
		}()
		// small stagger between goroutine spawns to reduce scheduler spike
		if i%100 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// summary ticker
	summaryTicker := time.NewTicker(1 * time.Second)
	defer summaryTicker.Stop()
	prevSent := uint64(0)
	prevRecv := uint64(0)
	prevErr := uint64(0)
	start := time.Now()

	// metrics poll ticker: every 10s fetch server /metrics and print comparison
	metricsTicker := time.NewTicker(10 * time.Second)
	defer metricsTicker.Stop()

	loopDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(loopDone)
				return
			case <-summaryTicker.C:
				s := totalSent.Load()
				r := totalRecv.Load()
				e := totalErr.Load()
				log.Printf("goroutines=%d sent_total=%d sent_s=%d recv_total=%d recv_s=%d err_total=%d err_s=%d active_conn_approx=%d",
					runtime.NumGoroutine(),
					s, s-prevSent,
					r, r-prevRecv,
					e, e-prevErr,
					*n, // approx active connectors
				)
				prevSent = s
				prevRecv = r
				prevErr = e
			case <-metricsTicker.C:
				// fetch server metrics
				url := fmt.Sprintf("http://%s/metrics", *addr)
				m, err := fetchMetrics(url)
				if err != nil {
					log.Printf("metrics fetch error: %v", err)
					continue
				}
				// server metrics: processed_messages, delivered_messages, no_recipient_messages
				processed := uint64(0)
				delivered := uint64(0)
				noRecip := uint64(0)
				if v, ok := m["processed_messages"]; ok {
					processed = uint64(v)
				}
				if v, ok := m["delivered_messages"]; ok {
					delivered = uint64(v)
				}
				if v, ok := m["no_recipient_messages"]; ok {
					noRecip = uint64(v)
				}
				localSent := totalSent.Load()
				localRecv := totalRecv.Load()
				localErr := totalErr.Load()
				// Expectations and deltas
				expectedDelivered := uint64(0)
				if processed >= noRecip {
					expectedDelivered = processed - noRecip
				}
				networkDelta := int64(localSent) - int64(processed) // positive => client sent more than server processed
				deliveryDelta := int64(expectedDelivered) - int64(delivered)
				recvDelta := int64(localRecv) - int64(delivered)
				log.Printf("METRICS: server_processed=%d server_delivered=%d server_no_recipient=%d | local_sent=%d local_recv=%d local_err=%d | expected_delivered=%d delivery_delta=%d network_delta(local_sent-processed)=%d recv_delta(local_recv-server_delivered)=%d",
					processed, delivered, noRecip, localSent, localRecv, localErr, expectedDelivered, deliveryDelta, networkDelta, recvDelta)
			}
		}
	}()

	// optional duration
	if *dur > 0 {
		select {
		case <-time.After(*dur):
			log.Println("duration elapsed, shutting down")
			cancel()
		case <-ctx.Done():
		}
	} else {
		<-loopDone
	}

	// wait connectors to finish
	wg.Wait()
	// final summary
	d := time.Since(start).Seconds()
	sent := totalSent.Load()
	recv := totalRecv.Load()
	err := totalErr.Load()
	log.Printf("finished elapsed=%.1fs sent=%d recv=%d err=%d sent/s=%.1f recv/s=%.1f",
		d, sent, recv, err, float64(sent)/d, float64(recv)/d)
}

func fetchMetrics(url string) (map[string]float64, error) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	var body struct {
		AppMetrics map[string]float64 `json:"app_metrics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.AppMetrics, nil
}

func runConnector(ctx context.Context, addr string, pubs [][32]byte, pubsStr []string, idx int, m int, outBuf int, seed int64,
	totalSent *atomic.Uint64, totalRecv *atomic.Uint64, totalErr *atomic.Uint64) {
	idHex := pubsStr[idx]
	r := rand.New(rand.NewSource(seed + int64(idx)))

	// startup jitter to avoid thundering herd: up to 200ms random delay
	jitter := time.Duration(r.Intn(200)) * time.Millisecond
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}

	// build url
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws"}
	q := u.Query()
	q.Set("pub", idHex)
	u.RawQuery = q.Encode()

	// dial with timeout
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dctx, u.String(), nil)
	dcancel()
	if err != nil {
		log.Printf("dial failed id=%s err=%v", idHex, err)
		totalErr.Add(1)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "loadtest closing")

	// reader
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			mt, data, err := conn.Read(readCtx)
			if err != nil {
				if readCtx.Err() != nil {
					return
				}
				totalErr.Add(1)
				return
			}
			if mt != websocket.MessageBinary {
				continue
			}
			// minimal parse
			if len(data) < 37 {
				continue
			}
			var from [32]byte
			copy(from[:], data[0:32])
			nTo := int(data[32])
			dataLenOff := 33 + nTo*32
			if dataLenOff+4 > len(data) {
				continue
			}
			payloadLen := int(binary.BigEndian.Uint32(data[dataLenOff : dataLenOff+4]))
			if dataLenOff+4+payloadLen > len(data) {
				continue
			}
			totalRecv.Add(1)
		}
	}()

	// writer
	if m <= 0 {
		// no sending: wait for context
		<-ctx.Done()
		readCancel()
		wg.Wait()
		return
	}

	interval := time.Second / time.Duration(m)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Reuse payload buffer and message struct to avoid allocations (reduce GC pressure)
	const payloadLen = 16
	payload := make([]byte, payloadLen)

	n := len(pubs)

	for {
		select {
		case <-ctx.Done():
			readCancel()
			wg.Wait()
			return
		case <-ticker.C:
			// fill payload in-place
			for i := 0; i < payloadLen; i++ {
				payload[i] = byte('a' + r.Intn(26))
			}

			// pick random target excluding self
			i := r.Intn(n - 1)
			var targetIdx int
			if i >= idx {
				targetIdx = i + 1
			} else {
				targetIdx = i
			}

			// targeted message (nTo=0 broadcast is not supported by the server)
			var buf []byte
			fromKey := pubs[idx]
			buf = append(buf, fromKey[:]...)
			buf = append(buf, 1)
			// use raw bytes from pubs slice
			target := pubs[targetIdx]
			buf = append(buf, target[:]...)
			var lenb [4]byte
			binary.BigEndian.PutUint32(lenb[:], uint32(payloadLen))
			buf = append(buf, lenb[:]...)
			buf = append(buf, payload...)
			if err := conn.Write(ctx, websocket.MessageBinary, buf); err != nil {
				totalErr.Add(1)
				return
			}
			totalSent.Add(1)
		}
	}
}
