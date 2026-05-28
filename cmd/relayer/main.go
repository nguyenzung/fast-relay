package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/network"
	"nhooyr.io/websocket"
)

// Server wraps HTTP server and relayer and exposes monitoring endpoints.
type Server struct {
	rel       *core.Relayer
	h         *http.ServeMux
	srv       *http.Server
	outBuf    int
	startTime time.Time
	// recent CPU percent stored as bits in atomic Uint64
	cpuRecentBits atomic.Uint64
	stopSampler   chan struct{}
}

func NewServer(addr string, outBuf int) *Server {
	rel := core.NewRelayer()
	h := http.NewServeMux()
	s := &Server{rel: rel, h: h, outBuf: outBuf, startTime: time.Now(), stopSampler: make(chan struct{})}

	h.HandleFunc("/", s.wsHandler)
	h.HandleFunc("/metrics", s.metricsHandler)

	s.srv = &http.Server{Addr: addr, Handler: h}
	// start CPU sampler with 200ms interval
	go s.cpuSampler(200 * time.Millisecond)
	return s
}

func (s *Server) cpuSampler(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// initialize previous counters
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	prevTotal := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 + float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	prevTime := time.Now()

	for {
		select {
		case <-ticker.C:
			var ru2 syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru2); err != nil {
				continue
			}
			total := float64(ru2.Utime.Sec) + float64(ru2.Utime.Usec)/1e6 + float64(ru2.Stime.Sec) + float64(ru2.Stime.Usec)/1e6
			now := time.Now()
			deltaCPU := total - prevTotal
			deltaWall := now.Sub(prevTime).Seconds()
			var percent float64
			if deltaWall > 0 {
				percent = 100.0 * (deltaCPU / deltaWall)
				if percent < 0 {
					percent = 0
				}
			}
			s.cpuRecentBits.Store(math.Float64bits(percent))
			prevTotal = total
			prevTime = now
		case <-s.stopSampler:
			return
		}
	}
}

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	pub := r.URL.Query().Get("pub")
	if pub == "" {
		http.Error(w, "missing pub", http.StatusBadRequest)
		return
	}
	// Require hex-encoded 32-byte pubkey (64 hex chars)
	pubBytes, err := hex.DecodeString(pub)
	if err != nil {
		http.Error(w, "invalid pub (expect 64 hex chars)", http.StatusBadRequest)
		return
	}
	var pubKey [32]byte
	copy(pubKey[:], pubBytes)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		log.Printf("ws accept error: %v", err)
		return
	}
	// nhooyr Conn supports SetReadLimit directly
	const readLimit = 512 * 1024
	conn.SetReadLimit(readLimit)

	c := network.NewWSConnector(conn, pubKey, s.rel, s.outBuf)
	s.rel.Register(pubKey, c)

	if err := c.ReadLoop(r.Context()); err != nil {
		// log.Printf("ReadLoop exited for pub=%s err=%v", pub, err)
	} else {
		// log.Printf("ReadLoop exited normally for pub=%s", pub)
	}
}

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// get process CPU seconds via getrusage (accumulated)
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	userSec := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sysSec := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	totalCPU := userSec + sysSec

	uptime := time.Since(s.startTime).Seconds()
	cpuPercent := 0.0
	if uptime > 0 {
		cpuPercent = 100.0 * (totalCPU / uptime)
	}
	cpuRecent := math.Float64frombits(s.cpuRecentBits.Load())
	cpuRecentPerCPU := 0.0
	if runtime.NumCPU() > 0 {
		cpuRecentPerCPU = cpuRecent / float64(runtime.NumCPU())
	}

	m := map[string]interface{}{
		"active_connections":         s.rel.Count(),
		"goroutines":                 runtime.NumGoroutine(),
		"alloc_bytes":                ms.Alloc,
		"total_alloc_bytes":          ms.TotalAlloc,
		"sys_bytes":                  ms.Sys,
		"heap_objects":               ms.HeapObjects,
		"processed_messages":         s.rel.Processed(),
		"delivered_messages":         s.rel.Delivered(),
		"no_recipient_messages":      s.rel.NoRecipient(),
		"cpu_seconds":                totalCPU,
		"cpu_percent":                cpuPercent,
		"cpu_recent_percent":         cpuRecent,
		"cpu_recent_percent_per_cpu": cpuRecentPerCPU,
		"num_cpu":                    runtime.NumCPU(),
		"uptime_seconds":             uptime,
	}

	// add latency snapshot (ms)
	if cnt, meanMs, stdMs, p50, p95, p99 := s.rel.LatencySnapshot(); cnt > 0 {
		m["latency_count"] = cnt
		m["latency_mean_ms"] = meanMs
		m["latency_std_ms"] = stdMs
		m["latency_p50_ms"] = p50
		m["latency_p95_ms"] = p95
		m["latency_p99_ms"] = p99
	} else {
		m["latency_count"] = 0
		m["latency_mean_ms"] = 0.0
		m["latency_std_ms"] = 0.0
		m["latency_p50_ms"] = 0.0
		m["latency_p95_ms"] = 0.0
		m["latency_p99_ms"] = 0.0
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m)
}

func (s *Server) Start() error {
	log.Printf("starting server on %s", s.srv.Addr)
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	// stop sampler
	close(s.stopSampler)
	// close relayer to flush latency worker and other background tasks
	if s.rel != nil {
		s.rel.Close()
	}
	return s.srv.Shutdown(ctx)
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	outBuf := flag.Int("outbuf", 64, "per-connection outbound buffer size")
	flag.Parse()

	srv := NewServer(*addr, *outBuf)

	// Start server
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown error: %v", err)
	}
	log.Println("server stopped")
}
