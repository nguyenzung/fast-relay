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

// Churn load test: clients randomly go on/offline. Messages variable length 512..4096 bytes.
// Usage: go run cmd/churntest -addr localhost:8080 -n 100 -m 50 -on 5s -off 5s -duration 1m

func main() {
	addr := flag.String("addr", "localhost:8080", "server address host:port")
	n := flag.Int("n", 25000, "number of concurrent connectors")
	m := flag.Int("m", 5, "messages per second per connector when online")
	dur := flag.Duration("duration", 0, "test duration (0 = until SIGINT)")
	outBuf := flag.Int("outbuf", 64, "per-connection outbound buffer size")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	onMin := flag.Duration("on-min", 120*time.Second, "minimum online duration per cycle")
	onMax := flag.Duration("on-max", 240*time.Second, "maximum online duration per cycle")
	offMin := flag.Duration("off-min", 10*time.Second, "minimum offline duration per cycle")
	offMax := flag.Duration("off-max", 60*time.Second, "maximum offline duration per cycle")
	flag.Parse()

	log.Printf("churn loadtest start addr=%s N=%d M=%d on=%s..%s off=%s..%s duration=%s seed=%d", *addr, *n, *m, onMin.String(), onMax.String(), offMin.String(), offMax.String(), dur.String(), *seed)

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

	for i := 0; i < *n; i++ {
		i := i
		go func() {
			defer wg.Done()
			runChurnClient(ctx, *addr, pubs, pubsStr, i, *m, *outBuf, *seed+int64(i), *onMin, *onMax, *offMin, *offMax, &totalSent, &totalRecv, &totalErr)
		}()
		if i%100 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// periodic stats
	summaryTicker := time.NewTicker(1 * time.Second)
	metricsTicker := time.NewTicker(10 * time.Second)
	defer summaryTicker.Stop()
	defer metricsTicker.Stop()

	prevSent := uint64(0)
	prevRecv := uint64(0)
	prevErr := uint64(0)
	start := time.Now()

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
				log.Printf("goroutines=%d sent_total=%d sent_s=%d recv_total=%d recv_s=%d err_total=%d err_s=%d",
					runtime.NumGoroutine(),
					s, s-prevSent,
					r, r-prevRecv,
					e, e-prevErr,
				)
				prevSent = s
				prevRecv = r
				prevErr = e
			case <-metricsTicker.C:
				url := fmt.Sprintf("http://%s/metrics", *addr)
				m, err := fetchMetrics(url)
				if err != nil {
					log.Printf("metrics fetch error: %v", err)
					continue
				}
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
				log.Printf("METRICS: server_processed=%d server_delivered=%d server_no_recipient=%d | local_sent=%d local_recv=%d local_err=%d",
					processed, delivered, noRecip, totalSent.Load(), totalRecv.Load(), totalErr.Load())
			}
		}
	}()

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

	wg.Wait()
	d := time.Since(start).Seconds()
	log.Printf("finished elapsed=%.1fs sent=%d recv=%d err=%d sent/s=%.1f recv/s=%.1f",
		d, totalSent.Load(), totalRecv.Load(), totalErr.Load(), float64(totalSent.Load())/d, float64(totalRecv.Load())/d)
}

// runChurnClient repeatedly connects, stays online for onDur (sends at rate m), then disconnects for offDur, repeating until ctx cancelled.
func runChurnClient(ctx context.Context, addr string, pubs [][32]byte, pubsStr []string, idx int, m int, outBuf int, seed int64, onMin, onMax, offMin, offMax time.Duration,
	totalSent, totalRecv, totalErr *atomic.Uint64) {
	r := rand.New(rand.NewSource(seed))
	n := len(pubs)
	if idx < 0 || idx >= n {
		return
	}
	pub := pubs[idx]
	pubHex := pubsStr[idx]
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// connect
		u := url.URL{Scheme: "ws", Host: addr, Path: "/"}
		q := u.Query()
		q.Set("pub", pubHex)
		u.RawQuery = q.Encode()
		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		conn, _, err := websocket.Dial(dctx, u.String(), nil)
		dcancel()
		if err != nil {
			totalErr.Add(1)
			// wait before retrying (random up to onMin)
			retryWait := time.Duration(0)
			if onMin > 0 {
				retryWait = time.Duration(r.Int63n(int64(onMin)))
			}
			select {
			case <-time.After(retryWait):
			case <-ctx.Done():
				return
			}
			continue
		}

		// while online: send messages at rate m for approximately onDur
		// sample online duration uniformly between onMin and onMax
		onDuration := onMin
		if onMax > onMin {
			delta := int64(onMax - onMin)
			onDuration = onMin + time.Duration(r.Int63n(delta+1))
		}
		onlineCtx, onlineCancel := context.WithCancel(ctx)
		// reader to count incoming
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
				// basic validation and count
				if len(data) < 37 {
					continue
				}
				dataLenOff := 33 + int(data[32])*32
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

		interval := time.Second / time.Duration(m)
		ticker := time.NewTicker(interval)
		onTimer := time.NewTimer(onDuration)
		loopDone := false
		for !loopDone {
			select {
			case <-ctx.Done():
				loopDone = true
			case <-onTimer.C:
				loopDone = true
			case <-ticker.C:
				// random payload size 512..2048
				sz := 512 + int(r.Int63n(2048-512+1))
				payload := make([]byte, sz)
				// pick a random existing target excluding self
				if n <= 1 {
					// no target available
					continue
				}
				j := r.Intn(n - 1)
				var targetIdx int
				if j >= idx {
					targetIdx = j + 1
				} else {
					targetIdx = j
				}
				target := pubs[targetIdx]

				// build targeted message: FromID(32), ToIDsLen=1, ToID(32), DataLen(4), Data
				var buf []byte
				buf = append(buf, pub[:]...)
				buf = append(buf, 1)
				buf = append(buf, target[:]...)
				var lenb [4]byte
				binary.BigEndian.PutUint32(lenb[:], uint32(len(payload)))
				buf = append(buf, lenb[:]...)
				buf = append(buf, payload...)
				if err := conn.Write(ctx, websocket.MessageBinary, buf); err != nil {
					totalErr.Add(1)
					loopDone = true
					break
				}
				totalSent.Add(1)
			}
		}

		// leave online
		onlineCancel()
		ticker.Stop()
		_ = conn.Close(websocket.StatusNormalClosure, "churn cycle off")
		<-readDone // wait reader goroutine exit

		// sleep offline for random duration between offMin and offMax
		offDuration := offMin
		if offMax > offMin {
			delta := int64(offMax - offMin)
			offDuration = offMin + time.Duration(r.Int63n(delta+1))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(offDuration):
		}
	}
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
	var m map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}
