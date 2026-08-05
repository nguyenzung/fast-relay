package domains

import (
	"log"
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nguyenzung/relayer-server/internal/core"
	"github.com/nguyenzung/relayer-server/internal/mem"
)

// Relayer manages a global registry of connectors keyed by pubKey.
// It uses sync.Map for lock-free reads and atomic counters for high-performance metrics.
type Relayer struct {
	connectors sync.Map
	processed  atomic.Uint64 // total messages received by the relayer
	delivered  atomic.Uint64 // total messages successfully pushed to target connections
	noRecip    atomic.Uint64 // messages dropped because no recipient was found
	logger     *slog.Logger

	// latency statistics (protected by latencyMu)
	latencyMu        sync.Mutex
	latencyCount     uint64   // total recorded latencies (matches delivered count)
	latencyMean      float64  // mean in microseconds
	latencyM2        float64  // sum of squares of differences for variance (Welford)
	latencySamples   []uint64 // reservoir sample in microseconds for percentile estimation
	latencyReservoir int      // max reservoir size

	// channel-based latency ingestion
	latencyCh chan uint64
	latencyWg sync.WaitGroup
}

// NewRelayer constructs a Relayer.
func NewRelayer() *Relayer {
	r := &Relayer{
		logger:           slog.New(slog.NewTextHandler(log.Writer(), nil)),
		latencyReservoir: 65536 << 2, // default reservoir for percentile estimation
		latencyCh:        make(chan uint64, 65536),
	}
	// start background latency aggregation worker
	r.startLatencyWorker()
	return r
}

// startLatencyWorker starts background worker to aggregate latencies from channel.
func (r *Relayer) startLatencyWorker() {
	r.latencyWg.Add(1)
	go func() {
		defer r.latencyWg.Done()
		for us := range r.latencyCh {
			r.latencyMu.Lock()
			// Welford online algorithm
			r.latencyCount++
			count := float64(r.latencyCount)
			delta := float64(us) - r.latencyMean
			r.latencyMean += delta / count
			r.latencyM2 += delta * (float64(us) - r.latencyMean)
			// reservoir sampling
			if len(r.latencySamples) < r.latencyReservoir {
				r.latencySamples = append(r.latencySamples, us)
			} else {
				// deterministic rand is fine for reservoir replacement
				randIdx := rand.Int63n(int64(r.latencyCount))
				if int(randIdx) < len(r.latencySamples) {
					r.latencySamples[int(randIdx)] = us
				}
			}
			r.latencyMu.Unlock()
		}
	}()
}

// Close stops background workers and flushes latency channel.
func (r *Relayer) Close() {
	// close latency channel to stop worker
	close(r.latencyCh)
	r.latencyWg.Wait()
}

// StartRecording implements core.Metrics.
//
// TODO: Relayer already starts its latency worker unconditionally in
// NewRelayer; wire that startup here instead once callers are expected to
// drive it explicitly via StartRecording.
func (r *Relayer) StartRecording() {}

// StopRecording implements core.Metrics.
//
// TODO: Relayer already stops its latency worker in Close; wire that
// shutdown here instead once callers are expected to drive it explicitly
// via StopRecording.
func (r *Relayer) StopRecording() {}

// RelayerMetrics is Relayer's snapshot shape, as returned by FetchMetrics.
type RelayerMetrics struct {
	Processed   uint64 `json:"processed_messages"`
	Delivered   uint64 `json:"delivered_messages"`
	NoRecipient uint64 `json:"no_recipient_messages"`

	LatencyCount  uint64  `json:"latency_count"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`
	LatencyStdMs  float64 `json:"latency_std_ms"`
	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
}

// FetchMetrics implements core.Metrics.
func (r *Relayer) FetchMetrics() any {
	count, meanMs, stdMs, p50, p95, p99 := r.LatencySnapshot()
	return RelayerMetrics{
		Processed:   r.Processed(),
		Delivered:   r.Delivered(),
		NoRecipient: r.NoRecipient(),

		LatencyCount:  count,
		LatencyMeanMs: meanMs,
		LatencyStdMs:  stdMs,
		LatencyP50Ms:  p50,
		LatencyP95Ms:  p95,
		LatencyP99Ms:  p99,
	}
}

// OnConnect adds a connector into the global registry.
func (r *Relayer) OnConnect(pubKey [32]byte, c core.Connector) {
	r.logger.Debug("Registering connector", "pub", pubKey)
	r.connectors.Store(pubKey, c)
}

// OnDisconnect removes a connector from the global registry.
func (r *Relayer) OnDisconnect(pubKey [32]byte) {
	r.logger.Debug("Unregistering connector", "pub", pubKey)
	r.connectors.Delete(pubKey)
}

// GetConnectorByKey returns the Connector registered under pubKey.
func (r *Relayer) GetConnectorByKey(pubKey [32]byte) (core.Connector, bool) {
	v, ok := r.connectors.Load(pubKey)
	if !ok {
		return nil, false
	}
	return v.(core.Connector), true
}

// Count returns the approximate number of registered connectors.
func (r *Relayer) Count() int {
	count := 0
	r.connectors.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

// HandleMessage implements core.App: it is the default targeted-multicast
// routing decision, composed from shared protocol primitives
// (core.ExtractTargets, core.DeliverTo - see internal/core/protocol.go) plus
// Relayer's own policy: self is excluded via ExtractTargets; if no targets
// remain, the message is dropped without incrementing processed. Otherwise
// the recipient list is zeroed in-place (privacy, Relayer-specific) and the
// message is pushed to each resolved connector via DeliverTo. A different
// App can reuse the same two primitives with its own policy on top.
func (r *Relayer) HandleMessage(from core.Connector, msg core.Message, buf *mem.Buffer, recvTime time.Time) {
	var targets [core.MaxTargetsPerMessage][32]byte
	targetN := core.ExtractTargets(msg, from.ID(), &targets)

	if targetN == 0 {
		return
	}

	r.IncrementProcessed()
	// ZeroToIDs zeros m[33:33+N*32] in-place. ToIDsLen (m[32]) and
	// DataLen/Data offsets are untouched, so the protocol layout stays valid.
	msg.ZeroToIDs()

	matched := 0
	for _, target := range targets[:targetN] {
		if dest, ok := r.GetConnectorByKey(target); ok {
			if core.DeliverTo(dest, msg, buf, recvTime) {
				matched++
			} else {
				r.IncrementDeliveryFailure()
			}
		}
	}

	if matched == 0 {
		r.IncrementDeliveryFailure()
	}
}

// --- Atomic Counter Helpers for Hot Path ---

// IncrementProcessed adds 1 to the processed message counter.
func (r *Relayer) IncrementProcessed() { r.processed.Add(1) }

// IncrementDeliverySuccess implements core.App.
func (r *Relayer) IncrementDeliverySuccess() { r.delivered.Add(1) }

// IncrementDeliveryFailure implements core.App.
func (r *Relayer) IncrementDeliveryFailure() { r.noRecip.Add(1) }

// --- Metrics Accessors ---

func (r *Relayer) Processed() uint64   { return r.processed.Load() }
func (r *Relayer) Delivered() uint64   { return r.delivered.Load() }
func (r *Relayer) NoRecipient() uint64 { return r.noRecip.Load() }

// RecordLatency records a delivery latency (duration from receive -> deliver).
// The value is stored in microseconds.
func (r *Relayer) RecordLatency(d time.Duration) {
	us := uint64(d.Microseconds())
	select {
	case r.latencyCh <- us:
	default:
		// channel is full, drop the latency record
	}
}

// LatencySnapshot returns a snapshot of latency statistics: count, mean(ms), stddev(ms), p50(ms), p95(ms), p99(ms).
func (r *Relayer) LatencySnapshot() (count uint64, meanMs float64, stdMs float64, p50Ms float64, p95Ms float64, p99Ms float64) {
	// copy under lock to minimize critical section
	r.latencyMu.Lock()
	localCount := r.latencyCount
	localMean := r.latencyMean
	localM2 := r.latencyM2
	samplesCopy := make([]uint64, len(r.latencySamples))
	copy(samplesCopy, r.latencySamples)
	r.latencyMu.Unlock()

	if localCount == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	count = localCount
	meanMs = localMean / 1000.0
	var variance float64
	if localCount > 1 {
		variance = localM2 / float64(localCount-1)
	}
	stdMs = math.Sqrt(variance) / 1000.0

	// percentiles: use interpolated quantile (approximate Hyndman & Fan type 7)
	if len(samplesCopy) == 0 {
		return count, meanMs, stdMs, 0, 0, 0
	}
	sort.Slice(samplesCopy, func(i, j int) bool { return samplesCopy[i] < samplesCopy[j] })
	n := float64(len(samplesCopy))
	quantile := func(q float64) float64 {
		// q in (0,1)
		if n == 0 {
			return 0
		}
		// position using R-7: h = 1 + (n-1)*q
		h := 1.0 + (n-1.0)*q
		hf := math.Floor(h)
		hi := int(hf) - 1 // zero-based index for lower
		if hi < 0 {
			return float64(samplesCopy[0])
		}
		if hi >= len(samplesCopy)-1 {
			return float64(samplesCopy[len(samplesCopy)-1])
		}
		lower := float64(samplesCopy[hi])
		upper := float64(samplesCopy[hi+1])
		return lower + (h-hf)*(upper-lower)
	}

	p50Ms = quantile(0.50) / 1000.0
	p95Ms = quantile(0.95) / 1000.0
	p99Ms = quantile(0.99) / 1000.0
	return count, meanMs, stdMs, p50Ms, p95Ms, p99Ms
}
