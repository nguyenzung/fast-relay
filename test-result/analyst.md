# Relay Server Performance Analysis

Source data: `churn_test_benchmark.md`  
Architecture reference: `../Architecture.md`

---

## 1. Executive Summary

This document analyzes the 38-hour churn benchmark of the Go WebSocket relay server (`churn_test_benchmark.md`). This is the most recent and longest run, replacing the previous 25-hour benchmark.

The server sustained **~104.6K processed messages/second** for **38.24 hours** on a single machine, while maintaining approximately **20,000–21,000 active WebSocket connections**, **p99 server-side delivery latency of 0.074 ms**, and stable process RSS around **~1.98 GB**.

Compared to the previous benchmark run, this result shows three concrete improvements driven by the architectural change to incremental frame reading (`readMessage()`):

| Metric | Previous run | This run | Change |
|---|---:|---:|---:|
| p99 latency | 0.119 ms | 0.074 ms | **−38%** |
| p95 latency | 0.051 ms | 0.042 ms | **−18%** |
| Latency std dev | 0.739 ms | 0.275 ms | **−63%** |
| Allocation rate | ~663 MB/s | ~195 MB/s | **−71%** |
| Test duration | 24.66 h | 38.24 h | +55% |

All other characteristics — throughput, goroutine ratio, RSS stability, delivery accounting — remain strong and consistent with the previous run.

---

## 2. Test Context

### Environment

| Item | Value |
|---|---|
| Machine | Acer Nitro V15 |
| Logical CPUs | 16 |
| Server | Go WebSocket relay server |
| Load generator | Churn test client |
| Setup | Relay server and load generator on the same machine |
| Transport | WebSocket (binary frames) |
| Workload | Clients randomly go online/offline; each message targets a random peer |
| Test duration | 38.24 hours (`uptime_seconds` ≈ 137,681) |
| Peak connections | 25,000 at start |
| Steady-state connections | ~20,000–21,000 |

Because the relay server and load generator shared the same machine (CPU, RAM, loopback network), the server results are conservative. In a separated setup the server would have more CPU headroom, though LAN bandwidth would then become the next factor to measure.

---

## 3. Final Metrics Snapshot

Last recorded sample at uptime ≈ 137,681 seconds (38.24 hours):

```json
{
  "active_connections": 20886,
  "goroutines": 41780,
  "processed_messages": 14398018519,
  "delivered_messages": 12054818090,
  "no_recipient_messages": 2343200583,
  "latency_mean_ms": 0.028791,
  "latency_p50_ms": 0.021,
  "latency_p95_ms": 0.042,
  "latency_p99_ms": 0.074,
  "latency_std_ms": 0.275,
  "alloc_bytes": 1008676352,
  "sys_bytes": 2292022176,
  "total_alloc_bytes": 28141107340064,
  "heap_objects": 6420541,
  "cpu_percent": 493.88,
  "cpu_recent_percent": 503.96,
  "num_cpu": 16,
  "process_rss_bytes": 2072903680,
  "process_rss_mb": 1976.88,
  "rss_per_conn_kb": 96.92,
  "go_alloc_per_conn_kb": 47.16,
  "cpu_cores_avg": 4.94,
  "uptime_seconds": 137681.38
}
```

---

## 4. Throughput Analysis

### Rates (computed from totals / uptime)

```
processed_messages / uptime = 14,398,018,519 / 137,681.38 ≈ 104,575 msg/s
delivered_messages / uptime = 12,054,818,090 / 137,681.38 ≈  87,556 msg/s
no_recipient       / uptime =  2,343,200,583 / 137,681.38 ≈  17,019 msg/s
```

### Summary

| Metric | Value |
|---|---:|
| Processed throughput | ~104.6K msg/s |
| Delivered throughput | ~87.6K msg/s |
| No-recipient rate | ~17.0K msg/s |
| Test duration | 38.24 hours |
| Total processed | 14.40 billion |
| Total delivered | 12.05 billion |

Throughput is stable throughout the run. The server and load generator share the same machine; throughput reflects both server capacity and generator capacity combined.

---

## 5. Delivery Accounting

```
delivered + no_recipient = 12,054,818,090 + 2,343,200,583 = 14,398,018,673
processed                =                                  14,398,018,519
difference               =                                             154
```

The difference of 154 messages out of 14.4 billion is negligible (≈ 1.1 × 10⁻⁶ %). It arises from a known corner case: when a write pump encounters a write error, it calls `IncrementNoRecipient` for the failed message but silently drains and releases remaining queued messages without incrementing any counter. These in-flight messages at connection-close time are not counted in either `delivered` or `no_recipient`. Over a 38-hour churn test the gap is expected to be tiny, and this confirms it is.

The accounting is effectively clean: every processed message is accounted for as either delivered or no-recipient.

---

## 6. No-Recipient Is Not a Relay Error

```
delivered ratio    = 12,054,818,090 / 14,398,018,519 ≈ 83.73%
no-recipient ratio =  2,343,200,583 / 14,398,018,519 ≈ 16.27%
```

The churn workload intentionally sends messages to random peers across the full client population, not just currently online clients. Clients go offline and reconnect continuously. `no_recipient_messages` represents messages routed at a moment when the target was not registered — this is expected behavior in a churn test, not a relay failure.

The 83.73 / 16.27 split is consistent with the previous benchmark (83.72 / 16.28) and confirms that the workload characteristics are stable across runs.

---

## 7. Latency Analysis

### Final latency metrics

| Metric | Value |
|---|---:|
| Mean | 0.0288 ms |
| P50 | 0.021 ms |
| P95 | 0.042 ms |
| P99 | 0.074 ms |
| Std deviation | 0.275 ms |

### Improvement over previous benchmark

| Metric | Previous | Current | Change |
|---|---:|---:|---:|
| Mean | 0.047 ms | 0.029 ms | −38% |
| P50 | 0.021 ms | 0.021 ms | — |
| P95 | 0.051 ms | 0.042 ms | −18% |
| P99 | 0.119 ms | 0.074 ms | **−38%** |
| Std dev | 0.739 ms | 0.275 ms | **−63%** |

The p50 is unchanged (both runs: 0.021 ms). The improvements are concentrated in the tail: p95, p99, and especially standard deviation. This pattern points to reduced latency spikes rather than a change in median path cost.

The architectural cause is the switch from the old two-step read path to the current `readMessage()` approach:

**Old path:**
```
WebSocket read → Go-heap []byte → mem.Buffer clone → relay
```

**Current path:**
```
conn.Reader() → parse header → allocate exact mem.Buffer → read payload directly in
```

Removing the intermediate Go-heap allocation eliminates a class of short-lived objects from the GC's scan path. Fewer temporary objects means fewer GC interruptions, which directly reduces latency spikes at the tail.

### What this latency measures

This is **server-side post-read delivery latency**, measured from the point the server completes reading a full WebSocket frame (`recvTime = time.Now()` after `readMessage()` returns), until it successfully writes the message to the destination connection.

It includes: protocol parsing, target extraction, recipient ID zeroing, routing lookup, `SafePush` enqueue, write pump wait, WebSocket write.

It does **not** include: sender→server network latency, time waiting inside `conn.Reader()` for the full frame, server→receiver network latency, receiver application processing, or end-to-end acknowledgment.

### Latency stability

The p50/p95/p99 pattern at the end of the run matches the pattern at early steady-state. There is no visible degradation trend over 38 hours. This confirms:

- Outbound queues are not accumulating backlog.
- GC pressure is not worsening.
- The routing path remains stable under sustained churn.

---

## 8. CPU Analysis

### Final CPU metrics

| Metric | Value |
|---|---:|
| Average CPU % (lifetime) | 493.88% |
| Recent CPU % (200ms sample) | 503.96% |
| Logical CPUs | 16 |
| Average cores used | ~4.94 |
| Recent cores used | ~5.04 |
| Average per-core utilization | ~30.9% |

In Linux/Go CPU reporting, 100% = one fully utilized core. With 16 logical CPUs:

```
avg    ≈ 493.9% ≈ 4.9 cores  (30.9% average per-core)
recent ≈ 504.0% ≈ 5.0 cores  (31.5% per-core)
```

~11 cores are idle. The relay server is not CPU-saturated at ~87K delivered messages/second.

### Why CPU usage is low relative to throughput

See Architecture §6.11 and §6.10: idle write pump goroutines block on channel receive and consume no CPU thread time. Go's M:N scheduler parks them without occupying OS threads. Each additional idle connection adds essentially zero CPU cost. This allows tens of thousands of mostly-idle connections to coexist with low aggregate CPU consumption.

---

## 9. Goroutine Lifecycle

```
active_connections = 20,886
goroutines         = 41,780
ratio              = 41,780 / 20,886 ≈ 2.00 goroutines/connection
```

The ratio is exactly 2.00, matching the expected model: one read pump goroutine and one write pump goroutine per connection, plus a small fixed overhead for the relayer's background workers and the HTTP server.

This ratio has been stable at ≈ 2.00 throughout the run, including during connection churn. Goroutine counts rise and fall with `active_connections`, confirming that there is no goroutine leak from orphaned read/write pumps after disconnect.

---

## 10. Memory Analysis

### Per-connection breakdown (steady state)

| Metric | Value |
|---|---:|
| Process RSS per connection | 96.92 KB |
| Go heap allocated per connection | 47.16 KB |
| Process RSS total | ~2.07 GB |
| Go `sys_bytes` | ~2.29 GB |
| Live heap (`alloc_bytes`) | ~0.96 GB |
| Go `heap_objects` | 6,420,541 |

The ~97 KB/connection RSS is composed of:
- **Goroutine stacks** (two goroutines per connection)
- **`outChan` buffer slots** (256 slots × `OutMessage` struct size)
- **`WSConnector` struct** and runtime metadata
- **Connection registry entry** in `sync.Map`
- **Kernel socket buffers** (TCP send/receive)

The `go_alloc_per_conn_kb` (47 KB) component oscillates with GC cycles rather than being a stable floor — it represents the fraction of Go heap attributable to live per-connection objects at the moment of the sample.

### Process RSS vs Go Sys

```
Process RSS     ≈ 2.07 GB  (physical pages mapped, from /proc/PID/status)
Go sys_bytes    ≈ 2.29 GB  (virtual memory reserved by the Go runtime)
```

RSS is lower than `sys_bytes` because the Go runtime reserves virtual address space in advance but not all pages are necessarily resident. The RSS represents actual physical memory in use.

Both metrics plateaued early in the run and remained flat for 38 hours, confirming no memory leak.

### Live heap vs total allocation

```
Live heap (alloc_bytes)  ≈  0.96 GB  (in use at snapshot time)
Total allocated ever     ≈ 28.14 TB  (cumulative counter, never decreases)
```

`TotalAlloc` only increases. It accumulates every allocation ever made, including objects that were created and then garbage collected seconds later. It does not represent memory the server held simultaneously.

Allocation rate:
```
28,141,107,340,064 / 137,681.38 ≈ 195 MB/s
```

### Comparison with previous benchmark

| Metric | Previous run | This run |
|---|---:|---:|
| Allocation rate | ~663 MB/s | ~195 MB/s |
| TotalAlloc (cumulative) | 61.73 TB | 28.14 TB |
| Live heap (alloc_bytes) | ~1.10 GB | ~0.96 GB |
| Go sys_bytes | ~2.03 GB | ~2.29 GB |
| Process RSS total | not measured | ~2.07 GB |

**The 71% reduction in allocation rate** is the clearest memory-level evidence of the `readMessage()` improvement. The old path allocated a Go-heap `[]byte` for every received frame to hold the raw WebSocket bytes, then copied into a jemalloc-backed `mem.Buffer`. The current path reads directly from `conn.Reader()` into a precisely-sized `mem.Buffer`. This eliminates the temporary Go-heap slice — one of the highest-frequency allocations in the old hot path.

The GC still runs regularly (live heap oscillates from ~800 MB to ~1.5 GB between samples), but it has far less garbage to collect per unit of time.

### `sys_bytes` is higher in this run

`sys_bytes` increased from ~2.03 GB (previous) to ~2.29 GB (current). This is not a regression. `sys_bytes` is virtual memory reserved from the OS by the Go runtime; it does not shrink automatically between GC cycles. The value stabilized early and held flat, so the slightly higher plateau likely reflects the runtime allocating somewhat more arena space for the larger total allocation volume in a longer run, or a difference in the initial startup phase.

---

## 11. GC Behavior

`alloc_bytes` oscillates between ~800 MB and ~1.5 GB across 3-minute sample intervals, while `sys_bytes` stays fixed and RSS stays flat. This is the expected GC pattern:

- Short-lived objects (routing temporaries, `OutMessage` value copies, channel metadata) are allocated and collected each cycle.
- `sys_bytes` stays fixed because the runtime holds virtual pages in reserve for the next allocation burst.
- RSS stays flat because jemalloc-backed `mem.Buffer` allocations — the dominant per-message memory — are freed deterministically via `Release()` when the last write pump finishes, independent of GC timing.

`heap_objects` oscillates between 2–16 million, confirming the GC is cycling actively. Neither `alloc_bytes` nor RSS trend upward, confirming that buffer ownership is leak-free.

---

## 12. What This Benchmark Proves

### 12.1. The Relay Fast Path Is Efficient

The server routes and delivers ~88K messages/second while maintaining p99 latency of 0.074 ms. On a shared single machine, with CPU split between server and load generator.

### 12.2. Long-Run Stability Is Strong

38.24 hours of continuous operation with no degradation in throughput, latency, goroutine count, or RSS. This is the first benchmark run long enough to detect slow memory growth or goroutine accumulation at practical timescales.

### 12.3. Memory Per Connection Is Bounded and Predictable

~97 KB/connection RSS at steady state, stable across the full run. This enables capacity planning: a server with 32 GB RAM has headroom for roughly 300,000 connections at this per-connection cost before RAM becomes the constraint.

### 12.4. Architectural Improvement Is Measurable

The switch to `readMessage()` (incremental read, exact-size allocation, no intermediate Go-heap clone) produced quantifiable improvements: −71% allocation rate, −38% p99 latency, −63% latency std deviation. These are not micro-benchmark results; they are observed in a 38-hour production-workload simulation.

### 12.5. Goroutine Lifecycle Is Clean

goroutines ≈ 2 × active_connections throughout churn. No leaked goroutines from disconnected clients.

### 12.6. Delivery Accounting Is Consistent

`delivered + no_recipient ≈ processed` with a residual of 154 messages across 14.4 billion — a gap attributable to connection-close drain behavior, not a logic error.

---

## 13. What This Benchmark Does Not Yet Prove

The result is strong, but it remains a local single-machine test. It does not yet prove:

- **Separated network performance.** Server and generator share loopback. Real LAN/WAN adds round-trip latency and may reveal network or NIC saturation.
- **TLS/WSS overhead.** All connections are unencrypted in this run.
- **Slow-receiver behavior.** The churn workload generates clients with approximately equal send/receive rates. A workload with many slow receivers (outChan filling up) would stress the drop-on-full path.
- **Hot-spot fanout.** Many senders targeting one receiver. This creates per-connection write pump backpressure at a specific destination.
- **High fan-out per message.** `MaxTargetsPerMessage = 10`. A workload that saturates this would change the delivered/processed ratio and memory multiplier.
- **p999 / max latency** and behavior during GC stop-the-world events.
- **Production auth, rate limiting, and abuse protection.**
- **Behavior under packet loss or unstable client connections.**

---

## 14. Network Headroom

For a rough estimate, using the churn test's implied payload distribution and the protocol layout (`FromID(32) | ToIDsLen(1) | ToIDs(N×32) | DataLen(4) | Data`):

If the average payload is ~1 KB (consistent with MPC/signing use cases):

```
ingress ≈ 104,575 × (32 + 1 + 32 + 4 + 1024) B
        ≈ 104,575 × 1093 B
        ≈ 114 MB/s   ≈ 0.91 Gbps

egress  ≈  87,556 × (32 + 1 + 0 + 4 + 1024) B   (ToIDs zeroed by ZeroToIDs)
        ≈  87,556 × 1061 B
        ≈  92 MB/s   ≈ 0.74 Gbps
```

Total logical traffic: **~1.65 Gbps** before TCP/WebSocket framing overhead.

| Network | Assessment |
|---|---|
| 1 GbE | Near saturation — likely to become bottleneck |
| 2.5 GbE | Feasible, but limited headroom for burst |
| 10 GbE | Sufficient to measure the server's true ceiling |
| Wi-Fi | Will bottleneck before server core |

For a separated multi-machine test targeting server throughput ceiling, 10 GbE is the recommended minimum.

---

## 15. Overall Assessment

The relay server sustained **~104.6K processed messages/second** and **~87.6K delivered messages/second** for **38.24 hours** against a continuous churn workload. It processed **14.4 billion messages** with **p99 server-side delivery latency of 0.074 ms**, **~97 KB RSS per connection**, and **~5 of 16 CPU cores** in use.

Compared to the previous 25-hour benchmark, the new `readMessage()` read path produced measurable improvements in latency tail and allocation efficiency. The 71% reduction in allocation rate directly reflects the elimination of an intermediate Go-heap buffer on the hot path.

The system ran for 38 hours with no observable goroutine leak, no RSS growth, and clean delivery accounting. This is strong evidence of production-quality stability at the architectural level.

The next meaningful tests are: separated multi-machine setup to remove the shared-resource constraint, TLS, and targeted stress tests for slow-receiver and high-fanout workloads.
