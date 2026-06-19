# Relay Server Architecture

This document details the architecture of the **Relay Server**, how its components interact, the data flow, and the design decisions aimed at improving performance and scalability.

---

## 1. Overview

Relay Server is a message broker server following the **Pub/Sub** or **Targeted Multicast** model via WebSocket connections. Clients connect to the server, identified by a public key (`PubKey` — 32 bytes), and send binary messages to one or more destination `PubKey`s.

The system is divided into 4 distinct layers:

| Layer | Package | Responsibility |
|---|---|---|
| Memory | `internal/mem` | Platform-specific allocator abstraction |
| Core | `internal/core` | Routing logic, state management, metrics |
| Network | `internal/network` | WebSocket adapter; byte streams ↔ core types |
| Server | `internal/server` | HTTP server, endpoints, authentication |

---

## 2. Component Details

### 2.1. Memory Layer (`internal/mem`)

Provides a cross-platform `Buffer` abstraction for raw byte allocation without unnecessary zero-fill.

- **`Buffer` (`buffer.go`)**: Wraps a `[]byte` and an `unsafe.Pointer` to the allocator-owned memory. Exposes `Bytes()`, `Len()`, `Retain()`, and `Release()`. Ownership is tracked via an `atomic.Int32` reference counter: `Retain()` increments; `Release()` decrements and frees when the count reaches zero. Double-free panics.

- **`buffer_linux.go`** (`//go:build linux && cgo`):
  - `NewBuffer(n)` calls jemalloc's `C.malloc(n)` — skips the zero-fill that Go's `make` always performs.
  - `Release()` calls `C.free` directly on the hot path when the refcount reaches zero. No GC finalizer is used.

- **`buffer_other.go`** (`//go:build !linux || !cgo`):
  - `NewBuffer(n)` falls back to `make([]byte, n)` for portability.
  - `Release()` sets `data = nil` so the GC can reclaim the backing array.

**Lifetime contract**: `NewBuffer` returns a buffer with refcount = 1. Call `Retain()` before sharing across goroutines; call `Release()` when done. The last `Release()` frees the backing memory immediately.

---

### 2.2. Core Layer (`internal/core`)

- **`Relayer` (`relayer.go`)**:
  - **Registry**: Manages active connections using `sync.Map`, optimized for read-heavy workloads where routing lookups greatly outnumber register/unregister events.
  - **Metrics**: `atomic.Uint64` counters (`processed`, `delivered`, `noRecip`) avoid mutex contention on the hot path.
  - **Latency Monitoring**: A background worker collects latency samples. Uses the Welford algorithm for online mean/variance and **Reservoir Sampling** (fixed sample size) to approximate percentiles (p50, p95, p99) with bounded memory.

- **`Connector` Interface (`connector.go`)**: Defines `ID()`, `SafePush()`, and `Close()`. Any protocol adapter plugging into `Relayer` must implement this interface.

- **`Message` (`message.go`)**: A `[]byte` view over the raw buffer. Helper methods (`ToIDsLen()`, `ToIDAt()`, `ZeroToIDs()`) operate directly on fixed byte offsets, with no struct allocation or deserialization.

- **`OutMessage` (`message.go`)**: Carries a `Message` and its receive timestamp across channels. Passed by value to avoid a separate pointer allocation per message in the common case. Contains a `Buf *mem.Buffer` field — on Linux this holds the jemalloc-backed region; on other platforms it is nil. Each recipient's write pump calls `Buf.Release()` after writing (nil-safe), and the last release frees the backing memory.

---

### 2.3. Network Layer (`internal/network`)

**`WSConnector` (`ws_connector.go`)**: Implements `core.Connector` using `github.com/coder/websocket`.

- **Asynchronous I/O**: Each `WSConnector` owns a bounded `outChan` (buffered channel). `SafePush` acquires an `RLock` to coordinate safely with `Close()` — preventing sends into a closed channel — then attempts a non-blocking send. If `outChan` is full, the message is dropped for that destination (*drop-on-full*). The `outChan` capacity is set by the caller of `NewWSConnector`; if `<= 0` it defaults to 256. Both `outChan` capacity and `MaxMessageSize` should be tuned to the expected payload size and burst characteristics of the deployment.

- **ReadWriteLoop**: Each connection maintains two goroutines:
  - **Read Pump**: Uses `conn.Reader()` and `readMessage()` to incrementally parse the protocol prefix, validate `ToIDsLen` and `DataLen`, allocate one precisely-sized `mem.Buffer`, and read the payload directly into that buffer. Calls `ZeroToIDs()` in-place to strip recipient IDs for privacy, then passes the buffer to `Relay()`.
  - **Write Pump**: Takes `OutMessage` values from `outChan`, writes to the socket with a 5-second per-write timeout, then calls `msg.Buf.Release()` if `Buf` is non-nil. When the last recipient releases, `C.free` is called immediately. On write error the pump closes the connection and drains remaining queued messages, releasing each buffer before exiting.

- **Incremental Read + Exact-size Allocation**: `readMessage()` first reads the small protocol header to learn the exact payload size, then allocates one `mem.Buffer` of that size and reads the payload directly in. This avoids the older pattern of reading into a Go-heap slice and cloning into a `mem.Buffer`. One buffer is shared across all recipients via `Retain`/`Release` reference counting.

---

### 2.4. Server Layer (`internal/server`)

**`Server` (`server.go`)**: Wraps `http.Server` and `core.Relayer`.

- **Pluggable Auth**: Defines `Authenticator` and `Registrar` interfaces. Custom implementations (e.g., JWT, OAuth) can be injected; otherwise defaults are used.
- **Endpoints**:
  - `GET /` — HTTP upgrade to WebSocket
  - `GET /register` — issue a new identity (PubKey)
  - `GET /metrics` — JSON metrics (RAM, CPU, goroutines, TPS, latency)

---

## 3. Data Flow

### A. Connection Phase

1. Client sends `GET /?pub=<HEX_STRING>`.
2. `Authenticator` validates the `pub`.
3. HTTP is upgraded to WebSocket.
4. `WSConnector` is created and registered with `Relayer.Register(pubKey, connector)`.

### B. Message Routing Phase

1. Client A sends a binary frame.
2. Read pump parses the frame via `readMessage()`. It then filters the recipient list — removing self and building a deduplicated targets array. If no valid targets remain, the buffer is released and the frame is skipped without incrementing `processed`.
3. With at least one valid target, `processed` is incremented, `ZeroToIDs()` is called in-place, and `c.Relay()` is called with the pre-filtered targets array.
4. `Relay()` looks up each target via `Relayer.Get()` and calls `dest.SafePush(msg)` — non-blocking.

### C. Delivery Phase

1. Recipient's write pump dequeues from `outChan`.
2. Writes to the socket.
3. On success: increments `delivered`, records latency, calls `msg.Buf.Release()`.

---

## 4. Design Principles

| Principle | Mechanism | Effect |
|---|---|---|
| Platform-aware allocation | jemalloc on Linux; `make` elsewhere | Skips zero-fill on the hot path |
| Explicit buffer ownership | `Retain`/`Release` reference counting | Deterministic `C.free`; no GC finalizers |
| Low-contention routing | `sync.Map` + brief `RLock` in `SafePush` + `atomic` counters | No write-heavy mutex on the routing path |
| Async delivery | Buffered `outChan` per connection | Routing goroutine never blocks on writes |
| Slow-client isolation | Drop-on-full at `outChan` | One slow connection cannot stall others |
| Core/network separation | `core.Connector` interface | Core is testable without a real network |

**Tuning note**: `outChan` capacity (`DefaultOutBufSize`, `MaxOutBufSize`) and `MaxMessageSize` directly trade per-connection memory against burst tolerance. A larger buffer absorbs spikes but increases RSS; `MaxMessageSize` should match real payload sizes to limit memory amplification under load or adversarial input.

---

## 5. Benchmark Results and Architectural Rationale

This section connects observed benchmark numbers to the architectural decisions that produce them. Source data: `test-result/churn_test_benchmark.md` (38-hour churn test).

### 5.1. Test Conditions

| Parameter | Value |
|---|---|
| Test type | Churn test — continuous connect/disconnect |
| Peak connections | 25,000 (start) → stabilized ~21,000 |
| Test duration | ~38 hours (`uptime_seconds` ≈ 137,000) |
| Machine CPUs | 16 cores |
| Total messages processed | ~14.4 billion |
| Total messages delivered | ~12 billion |

### 5.2. Steady-State Metrics (~21,000 connections)

| Metric | Value |
|---|---|
| Throughput — processed | ~105,000 msg/s |
| Throughput — delivered | ~88,000 msg/s |
| Latency p50 | 0.021 ms |
| Latency p95 | 0.042 ms |
| Latency p99 | 0.074–0.077 ms |
| Latency mean | ~0.029 ms |
| Memory RSS per connection | ~97 KB/conn |
| Memory RSS total | ~1.97–1.99 GB |
| Memory trend | Flat — no growth over 38 hours |
| CPU usage | ~4.9 cores average (out of 16) |
| Goroutines | ~41,000–42,000 (~2 per connection) |

### 5.3. Sub-millisecond Latency (p99 < 0.1 ms)

Three decisions cooperate to keep p99 below 100 µs:

1. **Non-blocking routing path.** The read/routing goroutine calls `SafePush` and returns immediately. It never waits for any write pump to finish. The entire path from frame receipt to `SafePush` completes without blocking on a network syscall.

2. **Low-contention routing path.** `sync.Map` stores a read-only atomic snapshot of the connection registry. Under the read-dominant access pattern (many routing lookups, rare register/unregister), lookups proceed without a write-heavy mutex. Each `SafePush` call does acquire a brief `RLock` (shared with all concurrent pushes, contending only against `Close()`), but releases it immediately after the non-blocking channel operation. `atomic.Uint64` metrics counters remove mutex overhead from the hot-path counters entirely.

3. **No serialization on the receive path.** `readMessage()` reads fixed offsets. `ToIDsLen()`, `ToIDAt()`, and `ZeroToIDs()` operate directly on the raw byte slice. There is no JSON, protobuf, or struct construction until after routing is complete.

### 5.4. ~97 KB/connection Memory Footprint

The per-connection RSS floor comes from goroutine stacks (two goroutines per connection), `outChan` buffer slots, the `WSConnector` struct, registry entries, and kernel socket buffers. The `go_alloc_per_conn_kb` component (40–72 KB in the benchmark) oscillates as the GC cycles through short-lived objects — it does not represent a stable floor.

Two architectural decisions keep the footprint bounded:

1. **One buffer per message, shared across all recipients.** For a multicast to N recipients, `NewBuffer` is called once, `Retain` is called N times, and each write pump calls `Release` once. Payload memory does not multiply by recipient count.

2. **jemalloc reduces GC pressure for message buffers.** Message payload buffers are allocated outside the Go heap. The GC does not scan, trace, or finalize them. This keeps GC-visible `heap_objects` proportional to Go-managed structs and metadata — not to message throughput. The main benefit is lower GC pressure at high message rates, not that jemalloc holds memory that the GC would otherwise free.

### 5.5. Memory Stability Over 38 Hours

RSS stabilized at ~1.97–1.99 GB throughout a churn test where clients continuously disconnected and reconnected.

1. **No goroutine leak.** The `goroutines` counter tracks closely with `active_connections × 2 + fixed overhead`. Goroutine counts rise and fall with connection counts, confirming that read/write pumps are fully cleaned up on disconnect.

2. **No memory leak.** `C.free` is called synchronously when the last `Release()` fires — there is no GC finalizer introducing lag between "last write pump done" and "memory returned to allocator". A finalizer-based approach would cause a slow RSS drift under churn; the explicit reference count eliminates it.

### 5.6. CPU Efficiency (~4.9 cores for 88,000 msg/s)

~5 of 16 cores sustain 88,000 delivered messages/second across 21,000 connections; the remaining 11 are idle.

1. **Idle connections consume zero CPU.** Write pump goroutines block on channel receive when there is nothing to send. Go's M:N scheduler parks them without occupying an OS thread.

2. **`sync.Map` avoids read-path serialization.** Traditional `map + RWMutex` serializes all reader goroutines during the window a writer holds the write lock. `sync.Map` lets concurrent routing lookups proceed in parallel.

3. **`atomic` counters eliminate mutex round-trips on metrics.** At 105,000 operations/second across many goroutines, `atomic.Add` removes a meaningful slice of per-operation overhead compared to a mutex-guarded increment.

4. **Zero-fill elimination.** `readMessage()` allocates via jemalloc (no zero-fill) and reads the payload directly into the final buffer. The older pattern — `conn.Read` into a Go-heap slice, then `copy` into `mem.Buffer` — performed an extra zero-fill and an extra copy on every message.

### 5.7. GC Behavior Under Load

`alloc_bytes` oscillates between ~800 MB and ~1.5 GB while `sys_bytes` stays fixed at ~2.29 GB and RSS stays flat at ~1.97 GB.

- `alloc_bytes` spikes and drops each GC cycle as short-lived Go objects (channel metadata, routing temporaries, `OutMessage` values) are allocated and reclaimed.
- `sys_bytes` stays fixed because the Go runtime retains reserved virtual memory pages rather than returning them to the OS immediately.
- RSS stays flat because jemalloc-managed message buffers — the dominant memory consumer — are freed deterministically through `Release()`, independent of GC timing.

The oscillating `heap_objects` count (3–16 million) confirms the GC is active. Neither `alloc_bytes` nor RSS trends upward, validating that buffer ownership is leak-free.

---

## 6. Performance Model: Why the Relay Server Is Fast

The server's performance comes from a copy-minimized binary relay pipeline rather than from any single optimization. The hot path is intentionally small: read one binary WebSocket frame, parse fixed offsets, route by recipient key, enqueue to destination write pumps, and release memory through explicit reference counting.

### 6.1. Binary Protocol, No Deserialization

Clients send `websocket.MessageBinary` frames in a compact fixed-layout format:

```
FromID(32) | ToIDsLen(1) | ToIDs(N×32) | DataLen(4) | Data(DataLen)
```

Recipient IDs and payload length are read from fixed byte offsets. There is no JSON, protobuf, map, or nested struct on the routing path. CPU work per message is predictable and allocation-free at the parsing step.

### 6.2. Incremental Read with Exact-size Allocation

`readMessage()` reads the protocol prefix first, validates `ToIDsLen` and `DataLen`, then allocates one `mem.Buffer` of the exact required size and reads the payload directly into it.

This avoids the older pattern:

```
WebSocket read → Go heap []byte → mem.Buffer clone → relay
```

The current path is:

```
WebSocket reader → parse header → allocate exact mem.Buffer → read payload into final buffer
```

This removes one full-message copy and one Go heap allocation from every received frame.

### 6.3. Zero-copy Message View Inside the Relay

`core.Message` is a `[]byte` view over the raw buffer. `ToIDsLen()`, `ToIDAt()`, and `ZeroToIDs()` operate directly on the slice. The relay does not build a separate message object or recipient structure per frame.

This is not zero-copy at the kernel or WebSocket library level, but inside the relay there are no extra application-level copies or allocations during routing.

### 6.4. One Shared Buffer for Multicast

For targeted multicast, one buffer is allocated per inbound message — not one per recipient. Before the message is queued to each destination connector, the buffer reference count is incremented with `Retain()`. Each write pump calls `Release()` after writing.

```
1 inbound buffer
N retained references (one per recipient)
N write pumps release independently
last Release() → C.free()
```

Multicast does not multiply payload memory by recipient count.

### 6.5. In-place Privacy Cleanup

Before routing, `ZeroToIDs()` clears the recipient list inside the shared buffer in-place. The wire layout and payload offset are preserved; recipients are not visible in the forwarded frame. Because the operation is in-place, no new outgoing payload is constructed.

### 6.6. Non-blocking Routing Path

The read/routing goroutine does not write to destination sockets directly. It calls `SafePush()` into each destination's `outChan` and returns. Network writes happen in each destination's write pump. A slow client delays only its own write pump — it cannot block the sender's read loop or any other client's routing.

```
parse → lookup recipient → SafePush → next recipient
```

### 6.7. Slow-client Isolation and Bounded Queues

Each connector has a bounded `outChan`. If the queue is full, `SafePush()` returns false and the message is dropped for that destination. This prevents one slow or broken connection from accumulating unbounded memory or blocking unrelated clients.

Queue capacity is set via the `outBufSize` parameter to `NewWSConnector` (defaults to 256 if `<= 0`). There are no named constants in the codebase — the appropriate value depends on the deployment's burst profile and memory budget.

A larger buffer absorbs burst traffic but increases per-connection RSS. A smaller buffer reduces memory but drops earlier under load spikes.

### 6.8. Bounded Fanout

`MaxTargetsPerMessage` caps the number of recipients per frame, keeping routing cost bounded and preventing a malicious or buggy client from forcing unbounded per-frame work.

`MaxMessageSize` caps the data field. For use cases such as MPC or signing where payloads are small and predictable, a tight limit (e.g., 16–64 KB) reduces memory amplification risk under slow-client or adversarial-client scenarios.

### 6.9. Reduced GC Pressure via Explicit Buffer Ownership

On Linux with CGO, `mem.Buffer` uses `malloc`/`free` for message payload buffers. These buffers live outside the Go heap and are released deterministically through `Release()` when the last recipient write completes.

The main benefit is not that jemalloc provides a "stable memory floor" — message buffers are short-lived and are freed after each delivery. The benefit is that high-throughput payload memory does not become Go heap garbage, which reduces GC pressure and helps keep p95/p99 latency stable by reducing the frequency and duration of GC pauses.

The per-connection RSS floor comes from goroutine stacks, channel buffers, connector structs, runtime metadata, registry entries, and socket buffers — not from long-lived jemalloc allocations.

### 6.10. Low-contention Shared State

The connection registry uses `sync.Map`, which fits the read-heavy access pattern: routing performs many lookups per second while register/unregister happens rarely. Metrics counters use atomics instead of mutex-protected increments.

`SafePush()` holds an `RLock` for the duration of the non-blocking channel send. This lightweight lock coordinates with `Close()` to prevent sending into a closed channel — it is not on the message-data path and releases immediately after the channel operation.

### 6.11. Cheap Idle Connections

Each connection owns a read pump and a write pump. Idle write pumps block on channel receive and consume no CPU. Go's scheduler multiplexes these goroutines onto a much smaller number of OS threads, so tens of thousands of mostly idle connections remain cheap.

This explains why the benchmark holds ~20,000–25,000 concurrent connections while consuming only ~5 of 16 available CPU cores.
