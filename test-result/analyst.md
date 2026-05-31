# Relay Server Performance Analysis

## 1. Executive Summary

This document analyzes the long-running churn benchmark of the Go WebSocket relay server.

The benchmark shows that the relay server sustained **~104.5K processed messages/second** for nearly **25 hours** on a single Acer Nitro V15 laptop, while maintaining approximately **20K–22K active WebSocket connections**, **p99 server-side delivery latency around 0.119 ms**, and stable memory usage around **~1 GB live heap / ~2 GB Go Sys memory**.

The most important conclusion is that the server demonstrates strong long-run stability:

- Throughput stayed above **100K processed messages/sec**.
- Latency did not degrade over time.
- Goroutine count stayed close to **2 goroutines per active connection**.
- Go `Sys` memory plateaued around **2.03 GB**.
- Live heap stayed around **~1 GB**.
- `TotalAlloc` reached **~61.73 TB**, but this represents cumulative allocation churn, not live memory usage.
- No obvious memory leak or goroutine leak was observed during the benchmark window.

This is no longer just a short load test. It is an endurance benchmark showing stable behavior under sustained churn.

---

## 2. Test Context

### Environment

| Item | Value |
|---|---:|
| Machine | Acer Nitro V15 |
| CPU | Intel Core i7 |
| RAM | 24 GB |
| Logical CPUs | 16 |
| Server | Go WebSocket relay server |
| Load generator | Churn test client |
| Setup | Relay server and load generator running on the same machine |
| Transport | WebSocket |
| Workload | Clients randomly go online/offline and send messages to random targets |
| Payload | 512B–2KB/message |
| Test duration | ~24.66 hours |

Because the relay server and churn load generator were running on the same laptop, the result is conservative in some ways. The server and generator shared CPU, RAM, scheduler time, loopback networking, and Go runtime overhead. In a separated multi-machine setup, the server may have more CPU headroom, although LAN bandwidth may become the next bottleneck.

---

## 3. Final Metrics Snapshot

Final observed snapshot:

```json
{
  "timestamp": "2026-05-31 23:24:27",
  "active_connections": 20942,
  "alloc_bytes": 1099450656,
  "cpu_percent": 558.2346844425417,
  "cpu_recent_percent": 442.6290287929804,
  "cpu_recent_percent_per_cpu": 27.664314299561276,
  "cpu_seconds": 495610.420792,
  "delivered_messages": 7771982802,
  "goroutines": 41894,
  "heap_objects": 4236972,
  "latency_count": 7771982802,
  "latency_mean_ms": 0.04715137000930802,
  "latency_p50_ms": 0.021,
  "latency_p95_ms": 0.051,
  "latency_p99_ms": 0.119,
  "latency_std_ms": 0.7387770971260125,
  "no_recipient_messages": 1511230747,
  "num_cpu": 16,
  "processed_messages": 9283213549,
  "sys_bytes": 2034588384,
  "total_alloc_bytes": 61733912224320,
  "uptime_seconds": 88781.73187804
}
```

---

## 4. Throughput Analysis

### Processed Throughput

```text
processed_messages / uptime_seconds
= 9,283,213,549 / 88,781.73
≈ 104,562 messages/sec
```

### Delivered Throughput

```text
delivered_messages / uptime_seconds
= 7,771,982,802 / 88,781.73
≈ 87,540 messages/sec
```

### No-Recipient Rate

```text
no_recipient_messages / uptime_seconds
= 1,511,230,747 / 88,781.73
≈ 17,022 messages/sec
```

### Interpretation

The server sustained approximately:

| Metric | Value |
|---|---:|
| Processed throughput | ~104.5K msg/s |
| Delivered throughput | ~87.5K msg/s |
| No-recipient rate | ~17.0K msg/s |
| Runtime | ~24.66 hours |
| Total processed messages | ~9.28B |
| Total delivered messages | ~7.77B |

This is a strong result. The key point is not only that the relay server reached more than 100K messages/sec, but that it sustained this rate for nearly 25 hours.

---

## 5. Delivery Accounting

The message accounting at the final snapshot is exact:

```text
delivered_messages + no_recipient_messages
= 7,771,982,802 + 1,511,230,747
= 9,283,213,549

processed_messages
= 9,283,213,549
```

Difference:

```text
0 messages
```

This is important because it shows that the metrics are internally consistent. Every processed message is accounted for as either:

1. Successfully delivered to an active target connection.
2. Counted as no-recipient because the target was offline or not registered at routing time.

There is no meaningful gap between processed messages and the sum of delivered/no-recipient messages.

---

## 6. No-Recipient Is Not a Relay Error

`no_recipient_messages` should not be interpreted as a server failure in this benchmark.

The churn test intentionally simulates clients going online and offline. Each sender selects a random target from the full client population, not only from currently online clients. Therefore, some targets are expected to be offline at the exact moment of routing.

The final ratios are:

```text
delivered ratio    = 7,771,982,802 / 9,283,213,549 ≈ 83.72%
no-recipient ratio = 1,511,230,747 / 9,283,213,549 ≈ 16.28%
```

This ratio is consistent with the churn workload. `no_recipient_messages` represents messages routed to offline or no-longer-registered clients. It does not indicate that the relay failed internally.

---

## 7. Latency Analysis

Final latency metrics:

| Metric | Value |
|---|---:|
| Mean | ~0.047 ms |
| P50 | ~0.021 ms |
| P95 | ~0.051 ms |
| P99 | ~0.119 ms |
| Std deviation | ~0.739 ms |

### Interpretation

The p99 latency of approximately **0.119 ms** means that 99% of successfully delivered messages were processed through the measured server-side delivery path within roughly **119 microseconds**.

This is very strong for a WebSocket relay maintaining around 20K active connections and processing more than 100K messages/sec.

### Important Latency Definition

The benchmark latency should be described as:

> Server-side post-read delivery latency.

It is measured from the point where the server has read a WebSocket message and starts processing it, until the relay successfully writes the message to the destination WebSocket connection.

It includes:

- Protocol parsing.
- Target extraction.
- Message cloning.
- Routing lookup.
- Enqueueing into the destination outbound queue.
- Waiting in the outbound queue.
- WebSocket write on the receiver connection.

It does not include:

- Sender client to server network latency.
- Time spent inside `conn.Read()` waiting for a full WebSocket frame.
- Receiver network latency after the server write.
- Receiver application processing time.
- Full client-to-client end-to-end acknowledgment.

### Latency Stability

The most valuable part of the result is not only that p99 latency is low, but that it remains stable over a long run. There is no visible long-term degradation in the p50/p95/p99 latency trend.

This suggests that:

- Outbound queues are not accumulating unbounded backlog.
- Goroutines are not piling up.
- GC and allocator behavior are not producing worsening latency over time.
- The relay path remains stable under sustained churn.

---

## 8. CPU Analysis

Final CPU metrics:

| Metric | Value |
|---|---:|
| Average CPU percent | ~558.23% |
| Recent CPU percent | ~442.63% |
| Logical CPUs | 16 |
| Recent CPU per logical CPU | ~27.66% |

In Go/Linux CPU metrics, `100%` usually represents one fully used CPU core.

Therefore:

```text
average CPU ≈ 558% ≈ 5.58 cores
recent CPU  ≈ 443% ≈ 4.43 cores
```

With 16 logical CPUs available, the relay server still had meaningful CPU headroom.

### Interpretation

At more than **104K processed messages/sec**, using approximately **4.4–5.6 logical CPU cores** is efficient. It suggests that the current bottleneck is not simply raw CPU exhaustion.

The remaining headroom is especially notable because the relay server and load generator were running on the same machine. If moved to separate machines, the relay server may have more available CPU, though LAN bandwidth may become the next limiting factor.

---

## 9. Goroutine Lifecycle Analysis

Final goroutine metrics:

```text
active_connections = 20,942
goroutines         = 41,894
```

Ratio:

```text
41,894 / 20,942 ≈ 2.00 goroutines per connection
```

### Interpretation

This is a very healthy result.

It suggests the server is maintaining approximately:

```text
1 read goroutine + 1 write goroutine per connection
```

There is no obvious goroutine leak and no sign of unbounded goroutine-per-message accumulation.

For a WebSocket relay, this is one of the strongest lifecycle indicators in the benchmark.

---

## 10. Memory Analysis

Final memory metrics:

| Metric | Value |
|---|---:|
| Live heap / `alloc_bytes` | ~1.10 GB |
| Go `sys_bytes` | ~2.03 GB |
| Heap objects | ~4.24M |
| Total allocation / `total_alloc_bytes` | ~61.73 TB |

### Total Allocation vs Live Memory

At the final snapshot:

```text
TotalAlloc ≈ 61.73 TB
Live heap  ≈ 1.10 GB
Go Sys     ≈ 2.03 GB
```

This is the most important memory conclusion.

The server moved through more than **61 TB of cumulative allocation**, but it did not hold that memory. Live heap remained around **~1 GB**, and Go runtime memory obtained from the OS stayed around **~2 GB**.

Average allocation rate:

```text
61,733,912,224,320 bytes / 88,781.73s
≈ 695 MB/s
```

This means the system handled nearly **700 MB/s allocation churn** for almost 25 hours without visible memory expansion.

### Interpretation

`TotalAlloc` is a cumulative counter. It only increases. It does not decrease when objects are garbage collected or memory spans are reused.

Therefore:

```text
High TotalAlloc does not mean high live memory.
High TotalAlloc does not mean the OS physically allocated 61 TB of RAM.
High TotalAlloc mainly reflects temporary allocation churn caused by high message throughput.
```

The relevant indicators for leak analysis are:

- `alloc_bytes` / live heap.
- `sys_bytes`.
- heap object count.
- GC pause behavior.
- RSS if available.
- latency tail.
- whether these metrics grow linearly over time.

In this benchmark, `sys_bytes` plateaued around **2.03 GB**, while live heap remained around **~1 GB**. This is strong evidence that the runtime and OS are reusing a stable working set effectively.

### Important Allocation Note

Memory allocation should not be understood as the OS physically allocating and writing every byte every time the application calls `make` or creates a new object.

In Go, most small and medium object allocations are handled by the Go runtime allocator. The runtime uses arenas, spans, size classes, per-P caches, central lists, and heap metadata. Once the heap or working set has grown to a stable size, many allocations are mostly allocator metadata operations: taking a block from a free list, updating span metadata, updating counters, and returning a pointer or slice header to the program.

On the fast path, these operations can be close to O(1) or amortized O(1). They are not necessarily linear in the total number of bytes ever allocated.

Linear cost appears when the program actually touches, zeros, copies, writes, or scans memory. For example, copying a 64KB payload is O(n) in the payload size. But an allocation counter increasing by 64KB does not automatically mean the OS physically allocated and wrote 64KB of new RAM at that moment.

This distinction is crucial when interpreting high-throughput Go benchmark results.

---

## 11. What This Benchmark Proves

This benchmark gives strong evidence for the following:

### 11.1 The Relay Fast Path Is Efficient

The server can route and deliver messages at more than **100K processed messages/sec** while maintaining p99 latency around **0.12 ms**.

### 11.2 Long-Run Stability Is Good

The system ran for nearly **25 hours** without visible degradation in throughput, latency, goroutine count, or memory usage.

### 11.3 Memory Behavior Is Healthy

Despite more than **61 TB** of cumulative allocation, live heap stayed around **~1 GB** and Go `Sys` stayed around **~2 GB**.

This indicates high allocation churn, but no observable memory leak within the benchmark window.

### 11.4 Goroutine Lifecycle Is Clean

The goroutine count stayed near **2 × active_connections**, matching the expected read/write loop model.

### 11.5 Metrics Accounting Is Clean

The final accounting is exact:

```text
processed_messages = delivered_messages + no_recipient_messages
```

This gives confidence that the benchmark metrics are internally consistent.

---

## 12. What This Benchmark Does Not Yet Prove

This result is strong, but it should not be overstated. It is still a local benchmark and does not fully prove production readiness.

It does not yet prove:

- Performance over a real LAN or WAN.
- Behavior under TLS/WSS.
- Behavior under slow receivers.
- Behavior under hotspot traffic, where many senders target one receiver.
- Fan-out behavior, where one message targets many receivers.
- p999/p9999 latency behavior.
- Max latency behavior.
- Kernel/network/NIC bottleneck behavior.
- Behavior under packet loss, unstable networks, or mobile clients.
- Production-grade authentication, authorization, abuse protection, or rate limiting.

These should be tested separately before making production-grade claims.

---

## 13. LAN Test Implications

For payloads around 512B–2KB, a rough average payload estimate is:

```text
payload avg ≈ (512B + 2048B) / 2 = 1280B
protocol overhead ≈ 69B
message avg ≈ 1349B
```

At approximately **104.5K processed messages/sec**:

```text
ingress ≈ 104,562 × 1349B
        ≈ 141 MB/s
        ≈ 1.13 Gbps
```

At approximately **87.5K delivered messages/sec**:

```text
egress ≈ 87,540 × 1349B
       ≈ 118 MB/s
       ≈ 0.94 Gbps
```

Total logical traffic is roughly:

```text
~2.07 Gbps before TCP/WebSocket overhead
```

### Practical Network Implication

| Network | Expected Result |
|---|---|
| 1GbE | Likely bottleneck |
| 2.5GbE | Possible, but may be close to saturation |
| 10GbE | Suitable for measuring higher server ceiling |
| Wi-Fi | Likely bottleneck before server core |

For LAN testing, **2.5GbE is the minimum reasonable target**, and **10GbE is preferred** if the goal is to measure the relay server ceiling rather than the network ceiling.

---

## 14. Overall Assessment

The relay server sustained approximately **104.5K processed messages/sec** for nearly **25 hours**, processing **9.28B messages** with **p99 server-side delivery latency around 0.119 ms**.

Live heap stayed around **~1 GB**, Go `Sys` memory plateaued around **~2 GB**, and goroutine count stayed near **2 × active_connections**. Despite **~61.73 TB cumulative allocation**, there was no observable memory leak during the benchmark window.

This is an impressive long-run endurance benchmark and demonstrates strong engineering quality in:

- Go concurrency.
- WebSocket relay design.
- High-throughput message routing.
- Runtime/memory behavior analysis.
- Load testing discipline.
- Long-running systems stability.
