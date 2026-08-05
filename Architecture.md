# Relay Server Architecture

This document details the architecture of the **Relay Server**, how its components interact, the data flow, and the design decisions aimed at improving performance and scalability.

---

## 1. Overview

Relay Server is a message broker server following the **Pub/Sub** or **Targeted Multicast** model via WebSocket connections. Clients connect to the server, identified by a public key (`PubKey` — 32 bytes), and send binary messages to one or more destination `PubKey`s.

The transport and connection-handling plumbing (`internal/network`, `internal/server`) does not depend on the relay's concrete routing logic — it depends only on the `core.App` interface (§2.2), including the routing decision itself (`App.HandleMessage`, §2.2). The currently shipped app, `domains.Relayer`, implements targeted multicast, but any type implementing `core.App` can be substituted at the entrypoint (see `cmd/relayer/main.go`) to reuse the same transport, buffer management, and HTTP layer for a different kind of application — e.g. a server-authoritative game server where clients send input to the server for processing rather than to each other, and `HandleMessage` runs game logic instead of a target lookup.

The system is divided into 5 distinct layers:

| Layer | Package | Responsibility |
|---|---|---|
| Memory | `internal/mem` | Platform-specific allocator abstraction |
| Core | `internal/core` | Connector/message primitives, `App` contract |
| Domains | `internal/domains` | Concrete `App` implementations (routing logic, state, metrics) |
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

- **`App` Interface (`app.go`)**: The pluggability seam of the system. `internal/network` and `internal/server` depend only on this interface, never on a concrete app type:
  - **Connection lifecycle**: `OnConnect(pubKey, connector)`, `OnDisconnect(pubKey)`, `Count()`.
  - **Routing (the seam)**: `HandleMessage(from, msg, buf, recvTime)` — called once per successfully framed inbound message. The App owns the entire routing decision: which recipients (if any) receive it, whether/how the recipient list is stripped before forwarding, and which counters apply. `internal/network` calls this and otherwise has no opinion on what a message means. `buf` ownership stays with the caller: `internal/network` holds the original reference from `readMessage()` and releases it right after `HandleMessage` returns; `HandleMessage` must not release that reference itself, only `Retain()` additional ones (via `DeliverTo`, see below) for each connector it pushes to.
  - **Delivery-outcome hooks**: `IncrementDeliverySuccess()`, `IncrementDeliveryFailure()`, `RecordLatency(d)`. These are called by the connector write pump after the actual socket write outcome is known.
  - **Metrics lifecycle/reporting**: `core.App` embeds `Metrics` with `StartRecording()`, `StopRecording()`, `FetchMetrics() any` so `internal/server` can start/stop collection and expose an opaque app-defined snapshot without knowing app internals.
  - **Shutdown**: `Close()`.
  - Any type satisfying `App` can be constructed at the entrypoint and injected into `server.NewServer(...)` in place of the relayer.

  ```go
  type App interface {
      OnConnect(pubKey [32]byte, c Connector)
      OnDisconnect(pubKey [32]byte)
      HandleMessage(from Connector, msg Message, buf *mem.Buffer, recvTime time.Time)
      Count() int
      IncrementDeliverySuccess()
      IncrementDeliveryFailure()
      RecordLatency(d time.Duration)
      Close()
      Metrics // embedded — see below
  }

  type Metrics interface {
      StartRecording()      // called once, before the first FetchMetrics call
      StopRecording()       // called once during shutdown
      FetchMetrics() any    // point-in-time snapshot; shape is App-defined
  }
  ```

- **`Connector` Interface (`connector.go`)**: Defines `ID()`, `SafePush()`, and `Close()`. Any protocol adapter plugging into an `App` must implement this interface. Implementations must be safe for concurrent use from multiple goroutines; `SafePush` must never block (drop-on-full instead), and `Close` must be idempotent and make every subsequent `SafePush` return `false`.

  ```go
  type Connector interface {
      ID() [32]byte
      SafePush(msg OutMessage) bool
      Close()
  }
  ```

- **`Message` (`message.go`)**: A `[]byte` view over the raw buffer. Helper methods (`ToIDsLen()`, `ToIDAt()`, `ZeroToIDs()`) operate directly on fixed byte offsets, with no struct allocation or deserialization.

- **`OutMessage` (`message.go`)**: Carries a `Message` and its receive timestamp across channels. Passed by value to avoid a separate pointer allocation per message in the common case. Contains a `Buf *mem.Buffer` field — on Linux this holds the jemalloc-backed region; on other platforms it is nil. Each recipient's write pump calls `Buf.Release()` after writing (nil-safe), and the last release frees the backing memory.

- **Protocol primitives (`protocol.go`)**: Mechanical, policy-free operations shared across `internal/domains` `App` implementations, so each new domain doesn't have to re-derive them:
  - `ExtractTargets(msg, self, &dst)` — reads `msg.ToIDs`, excludes `self`, writes into a caller-provided fixed array (no allocation).
  - `DeliverTo(dest, msg, buf, recvTime)` — the retain/push/release-on-drop dance for pushing `msg` to one `Connector`. It only ever manages the reference it creates via its own `Retain()`; it never releases the original reference (that belongs to `internal/network`, see above). This is the easiest place to introduce a refcount bug by hand, so it exists once here instead of being copy-pasted per `App`.
  - Both are opinion-free: they don't decide what counts as `processed`, whether to strip the recipient list, or which counter to bump on failure — that's still each `App`'s own policy (see §2.3).

---

### 2.3. Domains Layer (`internal/domains`)

Concrete `App` implementations live here, outside `internal/core`. Adding a new application type — including one with entirely different routing semantics — never requires touching `internal/core`, `internal/network`, or `internal/server`: only adding a new file to this package (implementing `core.App`, in particular `HandleMessage`) and wiring it up in a new `cmd/<name>/main.go`.

- **`Relayer` (`relayer.go`)**: The default implementation of `core.App`, providing targeted multicast — one of potentially several `App`s this package can hold:
  - **Registry**: Manages active connections using `sync.Map`, optimized for read-heavy workloads where routing lookups greatly outnumber register/unregister events. `OnConnect`/`OnDisconnect` are backed by this map; `GetConnectorByKey` is a private-to-the-package lookup helper used only by `HandleMessage` below (it is not part of `core.App`).
  - **`HandleMessage`**: The routing decision itself, composed from the shared primitives in §2.2 plus Relayer's own policy: `core.ExtractTargets` reads recipients from `msg.ToIDs` excluding the sender; if none remain, the frame is dropped without counting it as `processed`. Otherwise `processed` is incremented, the recipient list is zeroed in-place (`ZeroToIDs`, for privacy — Relayer-specific policy, not every `App` needs this), each target is looked up via the registry, and `core.DeliverTo` pushes to it. A different `App` (e.g. a game server) can reuse `ExtractTargets`/`DeliverTo` as-is, or ignore them entirely and route through server-side game state instead of a peer registry — the primitives don't force any particular routing semantics.
  - **Metrics**: `atomic.Uint64` counters (`processed`, `delivered`, `noRecip`) avoid mutex contention on the hot path.
  - **Latency Monitoring**: A background worker collects latency samples. Uses the Welford algorithm for online mean/variance and **Reservoir Sampling** (fixed sample size) to approximate percentiles (p50, p95, p99) with bounded memory.

---

### 2.4. Network Layer (`internal/network`)

**`WSConnector` (`ws_connector.go`)**: Implements `core.Connector` using `github.com/coder/websocket`. It holds a `core.App` reference (`app`), not a concrete relayer type, obtained via `NewWSConnector(conn, pubKey, app, outBufSize)`.

- **Asynchronous I/O**: Each `WSConnector` owns a bounded `outChan` (buffered channel). `SafePush` acquires an `RLock` to coordinate safely with `Close()` — preventing sends into a closed channel — then attempts a non-blocking send. If `outChan` is full, the message is dropped for that destination (*drop-on-full*). The `outChan` capacity is set by the caller of `NewWSConnector`; if `<= 0` it defaults to 256. Both `outChan` capacity and `MaxMessageSize` should be tuned to the expected payload size and burst characteristics of the deployment.

- **ReadWriteLoop**: Each connection maintains two goroutines:
  - **Read Pump**: Uses `conn.Reader()` and `readMessage()` to incrementally parse the fixed wire envelope (`FromID | ToIDsLen | ToIDs | DataLen | Data` — this framing is protocol-level, not app-specific, and stays in this package), allocate one precisely-sized `mem.Buffer`, and read the payload directly into that buffer. The resulting `core.Message` is then handed off whole to `app.HandleMessage(c, msg, buf, recvTime)` — this package does not interpret `ToIDs`/`Data` beyond the fixed envelope offsets; the routing decision belongs entirely to the `App` (§2.3). On disconnect, calls `app.OnDisconnect(pubKey)`.
  - **Write Pump**: Takes `OutMessage` values from `outChan`, writes to the socket with a 5-second per-write timeout, then calls `msg.Buf.Release()` if `Buf` is non-nil. When the last recipient releases, `C.free` is called immediately. On write error the pump closes the connection, calls `app.IncrementDeliveryFailure()`, and drains remaining queued messages, releasing each buffer before exiting. On success it calls `app.IncrementDeliverySuccess()` and `app.RecordLatency(...)` — these are generic delivery-outcome hooks, not part of the routing decision, so they stay here regardless of which `App` is plugged in.

- **Incremental Read + Exact-size Allocation**: `readMessage()` first reads the small protocol header to learn the exact payload size, then allocates one `mem.Buffer` of that size and reads the payload directly in. This avoids the older pattern of reading into a Go-heap slice and cloning into a `mem.Buffer`. One buffer is shared across all recipients via `Retain`/`Release` reference counting.

---

### 2.5. Server Layer (`internal/server`)

**`Server` (`server.go`)**: Wraps `http.Server` and a `core.App`, injected via `NewServer(addr, outBuf, auth, reg, app)`. The server itself constructs no app — the concrete `core.App` (e.g. `domains.NewRelayer()`) is built by the entrypoint (`cmd/relayer/main.go`) and passed in, keeping `internal/server` app-agnostic.

- **Pluggable Auth**: Defines `Authenticator` and `Registrar` interfaces. Custom implementations (e.g., JWT, OAuth) can be injected via `NewServer(addr, outBuf, auth, reg, app)`; passing `nil` for either falls back to `DefaultAuthenticator`/`DefaultRegistrar` (pubkey-from-query-param, no real identity check — suitable for trusted/dev environments only).

  ```go
  // Authenticator verifies an inbound WebSocket upgrade request (the "/"
  // endpoint) and resolves the caller's identity before the connection is
  // accepted. A non-nil error rejects the upgrade with 400 Bad Request.
  type Authenticator interface {
      Authenticate(r *http.Request) (*AuthResult, error)
  }

  // Registrar handles the "/register" endpoint: it provisions or resolves
  // an identity for a caller that does not yet have one. A non-nil error
  // rejects the request with 500 Internal Server Error.
  type Registrar interface {
      Register(r *http.Request) (*AuthResult, error)
  }
  ```
- **Endpoints**:
  - `GET /` — HTTP upgrade to WebSocket
  - `GET /register` — issue a new identity (PubKey)
  - `GET /metrics` — JSON runtime metrics (RAM, CPU, goroutines, uptime) at top level, plus app-defined metrics nested under `app_metrics` from `app.FetchMetrics()`

---

## 3. Data Flow

### A. Connection Phase

1. Client sends `GET /?pub=<HEX_STRING>`.
2. `Authenticator` validates the `pub`.
3. HTTP is upgraded to WebSocket.
4. `WSConnector` is created and registered via `app.OnConnect(pubKey, connector)`.

### B. Message Routing Phase

1. Client A sends a binary frame.
2. Read pump parses the frame via `readMessage()` into a `core.Message` + `mem.Buffer`, calls `app.HandleMessage(c, msg, buf, recvTime)` synchronously, then releases its own reference to `buf` — network stops interpreting the message here; everything else below is `Relayer`'s (the default `App`) behavior, not network's.
3. `Relayer.HandleMessage` (via `core.ExtractTargets`) filters the recipient list — removing self. If no valid targets remain, the frame is skipped without incrementing `processed` (and without touching `buf`'s refcount — see §2.2 on `buf` ownership).
4. With at least one valid target, `processed` is incremented, `ZeroToIDs()` is called in-place, and each target is looked up via the internal registry and pushed via `core.DeliverTo` — non-blocking. A different `App` would perform entirely different steps here.

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
| App pluggability | `core.App` interface, implementations in `internal/domains` | `network`/`server` are reusable across different application types without modification |

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

---

## 7. Distributed Deployment Extensibility

Everything above (§1–§6) describes a single process. This section is about what happens when one process is not enough — what the architecture does and does not give you toward running a cluster of relay nodes.

### 7.1. What the architecture provides: an extension seam, not a runtime

`internal/network`'s entire job is:

```text
WebSocket frame → parse common binary envelope → app.HandleMessage(from, msg, buf, recvTime)
```

It has no opinion on what happens next. `core.App.HandleMessage` owns the complete routing decision (§2.2), so a domain implementation is free to do more than look up a local `sync.Map`. Nothing in `core`, `network`, or `server` assumes the recipient is reachable through a local `Connector` — that assumption lives entirely inside `domains.Relayer.HandleMessage`, which is one possible policy, not the only one.

A cluster-aware `App` can be dropped in without touching any other layer:

```go
func (r *DistributedRelayer) HandleMessage(from core.Connector, msg core.Message, buf *mem.Buffer, recvTime time.Time) {
    var targets [core.MaxTargetsPerMessage][32]byte
    n := core.ExtractTargets(msg, from.ID(), &targets)
    msg.ZeroToIDs()

    for _, target := range targets[:n] {
        if dest, ok := r.localRegistry.GetConnectorByKey(target); ok {
            core.DeliverTo(dest, msg, buf, recvTime) // local hop, existing primitive
            continue
        }
        if node, ok := r.directory.LookupOwner(target); ok {
            r.interNode.Forward(node, target, msg, buf, recvTime) // this node's own logic
            continue
        }
        r.IncrementDeliveryFailure()
    }
}
```

`core.ExtractTargets` and `core.DeliverTo` (§2.2) are reused as-is for the local-hop case; the remote-hop case is entirely new code owned by the domain, wired up in a new `cmd/<name>/main.go`. `internal/core`, `internal/network`, and `internal/server` do not change, and the wire envelope (§6.1) does not change.

Two properties make this practically workable, not just theoretically possible:

- **`Connector` is already an interface** (§2.2). A "remote" connector that forwards bytes to another node over gRPC/NATS/a custom TCP protocol instead of writing to a local WebSocket satisfies `core.Connector` the same way `WSConnector` does — nothing downstream can tell the difference.
- **The `buf` retain/release contract is transport-agnostic** (§2.2, §6.4). `DeliverTo` retains before a local push and releases if it's dropped; an inter-node sender must follow the same discipline — retain until the payload bytes are actually copied onto the wire (or queued past the point where they're needed), then release exactly once. This is the same rule `WSConnector`'s write pump already follows for local delivery — a distributed connector does not get a different contract, only a different transport underneath it.

### 7.2. What the architecture does not provide

None of the following exists in this codebase, and nothing here is designed to provide it:

- Cluster membership or discovery
- A routing/ownership directory (the `directory.LookupOwner` call above is a stub — no such type exists)
- Inter-node transport
- Consensus, replication, or sharding
- Failure detection or automatic failover
- Distributed tracing

This is deliberate, not an oversight: different `App`s need fundamentally different distribution models (a relay routes by `PubKey → owning node`; a game server routes by `Room → authoritative node`; an MPC service routes by `SessionID → participant set`). Baking one specific model — consistent hashing, a Redis-backed registry, a particular replication scheme — into `core` would force every `App` through a shape that doesn't fit most of them. The design instead keeps `core` to the one thing every `App` needs regardless of topology (frame in, routing decision out) and leaves the distribution model itself as application policy: **transport owns delivery mechanics; the App owns routing and distribution semantics.**

### 7.3. What a distributed `App` still has to solve on its own

Having the extension seam does not make a correct distributed system automatic. Any concrete cluster-aware `App` built on top of §7.1 still owns the usual hard problems of a distributed system, none of which `core.App` abstracts away:

- A node dying mid-route, after ownership lookup but before delivery
- Stale ownership data (the directory says node B, but the client reconnected to node C)
- The same client momentarily registered on two nodes during a reconnect/migration race
- Duplicate delivery from retries, or out-of-order delivery across nodes
- Inter-node queue backpressure and what to do when it's full
- Split-brain between nodes that disagree on ownership
- Version skew during a rolling deployment
- A recipient's ownership migrating while a message for them is already in flight

These are not gaps in the layering — they are exactly the problems a distributed implementation is responsible for solving, and no amount of interface design at the `core` layer removes them.

### 7.4. A concrete single-node assumption worth knowing

One specific place the current shipped `App` (`domains.Relayer`) is single-node by construction, not by contract: `Count()` and `FetchMetrics()` (§2.2, §2.3) only ever see connections registered via `OnConnect` on the local process. In a multi-node deployment, `/metrics`' `active_connections` and `app_metrics` (§2.5) each report that node's local view only — there is no cluster-wide aggregation anywhere in this codebase. A distributed deployment that wants fleet-wide numbers has to aggregate `/metrics` across nodes itself (e.g. at the scrape/dashboard layer); nothing here does it for you.

### 7.5. Summary

| Aspect | Status |
|---|---|
| Extension seam for distributed routing (`core.App.HandleMessage`) | Present, and reusable via `core.ExtractTargets`/`core.DeliverTo` for the local-hop case |
| `Connector` as a transport-agnostic delivery target | Present — a remote connector is a normal `core.Connector` |
| Cluster membership, directory, inter-node transport, consensus, replication | Not present — left to the concrete `App` |
| Cluster-wide metrics | Not present — `/metrics` is per-node |
| Correctness under node failure, stale ownership, duplication, ordering | Not addressed by this layer — owned entirely by whatever distributed `App` is built |

In short: this is a **single-node relay with a routing seam that does not have to be reworked to grow into a distributed one** — not a distributed system today, and not a claim this document makes elsewhere.
