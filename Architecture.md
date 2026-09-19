# Relay Server Architecture

This document explains how Relay Server is put together: its components, how a message flows through them, and why specific design choices were made for performance.

**If you only read one paragraph:** clients connect over WebSocket, identified by a 32-byte public key. A client sends one message naming up to 10 recipient pubkeys; the server looks each one up and delivers the message to whichever ones are online. The transport code doesn't know or care about this "look up by pubkey and deliver" logic — that logic lives behind a small interface (`core.App`), so it can be swapped out entirely for a different kind of application (e.g. a game server) without touching how connections, buffers, or HTTP are handled.

---

## 1. Overview

Relay Server is a message broker: clients connect over WebSocket, each identified by a public key (`PubKey` — 32 bytes), and send binary messages to one or more destination `PubKey`s. This pattern is sometimes called **targeted multicast** — a message can go to several specific recipients, but not to "everyone."

The important architectural fact is this: the transport and connection-handling code (`internal/network`, `internal/server`) does not know anything about *routing* — it just knows it has to call one function, `App.HandleMessage`, and let that function decide what happens to a message. The `App` type shipped today, `domains.Relayer`, implements exactly the targeted-multicast behavior described above. But because it's just one implementation of the `core.App` interface (§2.2), you could substitute a completely different one at the entrypoint (see `cmd/relayer/main.go`) — for example, a game server where clients send input *to the server* for processing, rather than to each other — and reuse all the same transport, buffer management, and HTTP plumbing.

The system is split into 5 layers, each with one job:

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

**In plain terms:** every message payload needs a chunk of memory to live in while it's being routed. This layer decides *how* that memory is allocated and freed, so the rest of the codebase doesn't have to think about it — it just calls `Retain()`/`Release()`.

Provides a cross-platform `Buffer` abstraction for allocating raw bytes without doing unnecessary work (like zero-filling memory nobody asked to be zeroed).

- **`Buffer` (`buffer.go`)**: Wraps a `[]byte` and a pointer to the allocator-owned memory. Exposes `Bytes()`, `Len()`, `Retain()`, and `Release()`. Ownership is tracked with a reference count (an `atomic.Int32`): `Retain()` increments it, `Release()` decrements it and frees the memory once it hits zero. Calling `Release()` on an already-freed buffer panics (double-free), by design — better to crash loudly than silently corrupt memory.

- **`buffer_linux.go`** (used when building for `linux && cgo`):
  - `NewBuffer(n)` calls jemalloc's `C.malloc(n)` directly. Go's own `make([]byte, n)` always zero-fills the memory first; this skips that step, since the buffer is about to be fully overwritten anyway.
  - `Release()` calls `C.free` directly, on the same goroutine, the moment the reference count hits zero. There's no garbage-collector finalizer involved — freeing happens exactly when the code says it does, not "eventually."

- **`buffer_other.go`** (used everywhere else — non-Linux, or CGO disabled):
  - `NewBuffer(n)` falls back to plain `make([]byte, n)`.
  - `Release()` just drops the reference (`data = nil`) and lets Go's garbage collector reclaim it normally.

**Lifetime contract, in short**: a buffer starts life with a reference count of 1. If you're going to hand it to more than one place, call `Retain()` first for each extra place. Whoever calls `Release()` last is the one that actually frees the memory, immediately.

---

### 2.2. Core Layer (`internal/core`)

**In plain terms:** this package defines the *contract* — the small set of methods any "application" (routing logic) must implement, and the small set of methods any "connection" must implement — so the transport layer can work with either one without knowing the concrete type.

- **`App` Interface (`app.go`)**: this is the main plug point of the whole system. `internal/network` and `internal/server` only ever talk to this interface — never to a concrete type like `Relayer` directly.
  - **Connection lifecycle**: `OnConnect(pubKey, connector)`, `OnDisconnect(pubKey)`, `Count()`.
  - **Routing — the actual decision-making**: `HandleMessage(from, m)`. This is called exactly once per message that was successfully parsed off the wire. The `App` owns the *entire* decision: who (if anyone) receives it, whether the recipient list gets stripped before forwarding, and which counters go up. `internal/network` just calls this function — it has no opinion on what a message means.
    - A subtlety worth knowing: who owns `m.Buf` (the payload memory) at each point. `internal/network` holds the original reference (from reading the message off the socket) and releases it right after `HandleMessage` returns. `HandleMessage` must **not** release that same reference — it should only call `Retain()` to create additional references (one per recipient, via `DeliverTo`, below). Getting this backwards causes either a memory leak or a crash (double-free).
  - **Delivery-outcome hooks**: `IncrementDeliverySuccess()`, `IncrementDeliveryFailure()`, `RecordLatency(d)` — called by the connection's write pump once it actually knows whether the socket write succeeded.
  - **Metrics lifecycle**: `core.App` embeds a `Metrics` interface with `StartRecording()`, `StopRecording()`, `FetchMetrics() any`, so `internal/server` can start/stop collecting metrics and expose a snapshot without needing to know what's inside it.
  - **Shutdown**: `Close()`.
  - Any type that implements `App` can be built at the entrypoint and passed into `server.NewServer(...)` in place of the relay.

  ```go
  type App interface {
      OnConnect(pubKey [32]byte, c Connector)
      OnDisconnect(pubKey [32]byte)
      HandleMessage(from Connector, m OutMessage)
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

- **`Connector` Interface (`connector.go`)**: this represents "a thing you can push a message to and eventually close" — normally a WebSocket connection, but the interface doesn't say that. Any protocol adapter that wants to plug into an `App` implements this.

  ```go
  type Connector interface {
      ID() [32]byte
      SafePush(msg OutMessage) bool
      Close() error
  }
  ```

  Two rules implementations must follow: `SafePush` must be safe to call from multiple goroutines at once, and it must **never block** — if it can't accept the message right now, it should drop it and return `false` rather than wait. `Close` must be safe to call more than once (idempotent), and after it's been called, every subsequent `SafePush` should return `false`.

- **`Message` (`message.go`)**: a thin `[]byte` view over the raw buffer — it doesn't copy or parse into a struct. Helper methods (`ToIDsLen()`, `ToIDAt()`, `ZeroToIDs()`) read/write directly at fixed byte offsets.

- **`OutMessage` (`message.go`)**: carries the receive timestamp plus a `Buf *mem.Buffer` as it moves between goroutines/channels. `Msg()` derives the zero-copy `Message` view from `Buf` on demand, so `Buf` stays the single source of truth. It's passed by value (not as a pointer) to avoid an extra allocation per message in the common case. On Linux, `Buf` points into jemalloc-managed memory; on other platforms it's just a normal Go slice. Each recipient's write pump calls `Buf.Release()` after writing its copy out, and the very last release is what actually frees the memory.

- **Protocol primitives (`protocol.go`)**: small, reusable, policy-free helper functions shared by every `App` implementation in `internal/domains`, so nobody has to hand-roll the tricky parts:
  - `ExtractTargets(msg, self, &dst)` — reads the recipient list out of `msg.ToIDs`, skips `self`, writes into a fixed-size array the caller provides. No allocation.
  - `DeliverTo(dest, m)` — handles the retain → push → (release-if-dropped) sequence for pushing one `OutMessage` to one `Connector`. This function only ever touches the reference *it* creates via its own `Retain()` — it never releases the caller's original reference. This retain/push/release dance is the easiest place to introduce a refcount bug by hand, which is exactly why it's written once here instead of being copy-pasted into every `App`.
  - Neither function has an opinion on business logic — they don't decide what counts as "processed," whether to strip the recipient list, or which counter to bump on failure. That's still each `App`'s own call (see §2.3).

---

### 2.3. Domains Layer (`internal/domains`)

**In plain terms:** this is where actual routing behavior lives. Adding a brand-new kind of application — even one with completely different routing rules — never requires touching `internal/core`, `internal/network`, or `internal/server`. You just add a new file here (implementing `core.App`, mainly `HandleMessage`) and wire it up in a new `cmd/<name>/main.go`.

- **`Relayer` (`relayer.go`)**: the default `core.App` implementation — targeted multicast. It's one possible application this package can hold, not a special case baked into the framework.
  - **Registry**: tracks active connections using `sync.Map`, which is optimized for read-heavy access (routing does far more lookups than connect/disconnect events). `OnConnect`/`OnDisconnect` write to this map; `GetConnectorByKey` is a private lookup helper used only by `HandleMessage` (it's not part of the public `core.App` interface).
  - **`HandleMessage`** — the actual routing decision, built from the primitives in §2.2 plus Relayer's own rules: `core.ExtractTargets` pulls the recipient list out of `m.Msg()`, excluding the sender. If nothing is left after that, the frame is dropped and not even counted as "processed." Otherwise: `processed` goes up by one, the recipient list is zeroed in place (`ZeroToIDs`, for privacy — this is a Relayer-specific choice, not something every `App` needs to do), each target is looked up in the registry, and `core.DeliverTo` pushes the message to it. A different `App` (say, a game server) could reuse `ExtractTargets`/`DeliverTo` as-is, or skip them entirely and route through its own game state instead of a peer registry — nothing about the primitives forces one routing style.
  - **Metrics**: uses `atomic.Uint64` counters (`processed`, `delivered`, `noRecip`) specifically to avoid a mutex on the hot path.
  - **Latency monitoring**: a background worker collects latency samples using the Welford algorithm (an online way to compute mean/variance without storing every value) and reservoir sampling (a fixed-size random sample) to estimate percentiles (p50, p95, p99) using bounded memory, regardless of how many messages flow through.

---

### 2.4. Network Layer (`internal/network`)

**In plain terms:** this is the adapter between raw WebSocket bytes and the `core` types above. It knows the wire format; it does not know what a message *means*.

**`WSConnector` (`ws_connector.go`)**: implements `core.Connector` on top of `github.com/coder/websocket`. It holds a `core.App` reference (`app`) — not a concrete relayer type — passed in via `NewWSConnector(conn, pubKey, app, outBufSize)`.

- **Asynchronous I/O**: each `WSConnector` owns a bounded, buffered channel (`outChan`) for outgoing messages. `SafePush` briefly acquires a read-lock (to coordinate with `Close()` and avoid sending on a closed channel), then attempts a non-blocking send. If the channel is already full, the message is dropped for that one destination — this is the *drop-on-full* behavior. The channel's capacity comes from whoever calls `NewWSConnector` (defaults to 256 if `<= 0`). Both this capacity and `MaxMessageSize` should be tuned to match the expected payload size and burstiness of your actual traffic.

- **Read/write loop**: each connection runs two goroutines:
  - **Read pump** — uses `conn.Reader()` and `readMessageWithFixedFromID()` to parse the incoming frame (`ToIDsLen | ToIDs | DataLen | Data`), stamp the authenticated `pubKey` into the message, allocate exactly one right-sized `mem.Buffer`, and read the payload directly into it. The resulting `core.OutMessage` is handed to `app.HandleMessage(c, m)` — this package never looks past the fixed envelope offsets; the actual routing decision belongs entirely to the `App` (§2.3). When the connection drops, it calls `app.OnDisconnect(pubKey)`.
  - **Write pump** — pulls `OutMessage` values off `outChan`, writes each to the socket (with a 5-second timeout), then calls `msg.Buf.Release()` if `Buf` isn't nil. When the last recipient releases, the memory is freed (`C.free`) immediately. If a write fails, the pump closes the connection, calls `app.IncrementDeliveryFailure()`, and drains any remaining queued messages (releasing each buffer as it goes, so nothing leaks). On success, it calls `app.IncrementDeliverySuccess()` and `app.RecordLatency(...)` — these two calls are generic "how did delivery go" hooks, not part of the routing decision, so they live here regardless of which `App` is plugged in.

- **Incremental read + exact-size allocation**: `readMessageWithFixedFromID()` first reads just the small header to learn the exact payload size, *then* allocates one `mem.Buffer` and reads the payload straight into it. The older, slower approach was: read into a plain Go slice, then copy that into a `mem.Buffer` — an extra copy this version avoids entirely. One buffer, shared by every recipient via `Retain`/`Release`.

---

### 2.5. Server Layer (`internal/server`)

**In plain terms:** this is the HTTP-facing shell — accepting connections, authenticating them, and exposing `/metrics`. It has zero knowledge of routing.

**`Server` (`server.go`)**: wraps `http.Server` and a `core.App`, both passed in via `NewServer(addr, outBuf, auth, reg, app)`. The server doesn't build its own app internally — the concrete `core.App` (e.g. `domains.NewRelayer()`) is constructed by the entrypoint (`cmd/relayer/main.go`) and handed in, which keeps `internal/server` completely app-agnostic.

- **Pluggable auth**: `Authenticator` and `Registrar` are interfaces, so you can inject your own (JWT, OAuth, whatever) via `NewServer(...)`. Passing `nil` for either falls back to a default that just reads the pubkey from a query parameter, with no real identity check — fine for trusted or dev environments, not for production against untrusted clients.

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
  - `GET /` — upgrades the HTTP connection to a WebSocket
  - `GET /register` — issues a new identity (PubKey)
  - `GET /metrics` — JSON runtime metrics (RAM, CPU, goroutines, uptime) at the top level, plus whatever the app reports nested under `app_metrics` (from `app.FetchMetrics()`)

---

## 3. Data Flow

This section walks through what actually happens, step by step, for the two things that matter: a client connecting, and a message being routed.

### A. Connection Phase

1. Client sends `GET /?pub=<HEX_STRING>`.
2. `Authenticator` validates the `pub` value.
3. The HTTP connection is upgraded to a WebSocket.
4. A `WSConnector` is created and registered by calling `app.OnConnect(pubKey, connector)`.

### B. Message Routing Phase

1. Client A sends a binary frame.
2. The read pump parses it via `readMessageWithFixedFromID()` into a `core.OutMessage`, calls `app.HandleMessage(c, m)` synchronously, then releases its own reference to `m.Buf`. From here on, everything described below is `Relayer`'s behavior (the default `App`) — the network layer itself has already stepped out of the picture.
3. `Relayer.HandleMessage` (via `core.ExtractTargets`) filters the recipient list, removing the sender if they listed themselves. If no valid targets remain, the frame is skipped — `processed` is not incremented, and `m.Buf`'s reference count is left untouched (see §2.2 for why that matters).
4. If at least one valid target remains: `processed` goes up by one, `ZeroToIDs()` runs in place, and each target is looked up in the registry and pushed via `core.DeliverTo` — none of this blocks. A different `App` would do something entirely different at this step.

### C. Delivery Phase

1. The recipient's write pump dequeues the message from `outChan`.
2. It writes the message to the socket.
3. On success: `delivered` goes up, latency is recorded, and `msg.Buf.Release()` is called.

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

**Tuning note**: `outChan` capacity (`DefaultOutBufSize`, `MaxOutBufSize`) and `MaxMessageSize` directly trade per-connection memory against burst tolerance. A bigger buffer absorbs traffic spikes but costs more RAM per connection; `MaxMessageSize` should be set close to your real payload sizes so that a burst of traffic (or a hostile client) can't inflate memory use far beyond what you actually need.

---

## 5. Benchmark Results and Architectural Rationale

This section ties the numbers from an actual benchmark run back to the design decisions that produced them. Source data: `test-result/churn_test_benchmark.md` (a 38-hour churn test — clients continuously connecting and disconnecting, not one steady load).

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

Three decisions work together to keep p99 under 100 microseconds:

1. **The routing path never blocks.** The goroutine that reads and routes a message calls `SafePush` and returns immediately — it never waits for a write pump to finish, and never touches a network syscall itself.

2. **The routing path has almost no lock contention.** `sync.Map` stores a read-friendly snapshot of the connection registry, so lookups (which vastly outnumber connects/disconnects) don't need a write-heavy mutex. `SafePush` does briefly hold a read-lock (shared across all concurrent pushes, contending only with `Close()`) but releases it right after the non-blocking channel send. Metrics counters use `atomic.Uint64`, removing mutex overhead from the hot path entirely.

3. **Nothing is serialized on the receive path.** `readMessageWithFixedFromID()` reads fixed byte offsets directly. `ToIDsLen()`, `ToIDAt()`, and `ZeroToIDs()` all operate straight on the raw bytes. There's no JSON, no protobuf, no struct construction anywhere before routing finishes.

### 5.4. ~97 KB/connection Memory Footprint

Where that per-connection floor comes from: two goroutine stacks, `outChan` buffer slots, the `WSConnector` struct itself, a registry entry, and kernel socket buffers. One component, `go_alloc_per_conn_kb` (40–72 KB in the benchmark), oscillates because the GC is cycling through short-lived objects — it's not a stable number, just a snapshot at whatever point in the GC cycle the measurement landed.

Two decisions keep this bounded rather than growing with load:

1. **One buffer per message, no matter how many recipients.** For a multicast to N recipients, `NewBuffer` is called once, `Retain` N times, and each write pump calls `Release` once. Payload memory doesn't multiply by recipient count.

2. **jemalloc keeps message buffers off the GC's plate.** Since message payload buffers live outside the Go heap, the GC never has to scan, trace, or finalize them. This keeps GC-visible object counts tied to Go-side structs and bookkeeping — not to how many messages are flowing through. The real benefit here is *less GC pressure at high throughput*, not that jemalloc is somehow holding onto memory the GC would otherwise free.

### 5.5. Memory Stability Over 38 Hours

RSS held steady at ~1.97–1.99 GB the entire time, even while clients were continuously disconnecting and reconnecting.

1. **No goroutine leak.** The goroutine count tracked `active_connections × 2 + a fixed overhead` throughout the run — rising and falling with connection count, which confirms read/write pump goroutines are fully cleaned up on every disconnect.

2. **No memory leak.** `C.free` runs synchronously the instant the last `Release()` fires — there's no GC finalizer adding lag between "the last write pump finished" and "the memory is actually returned." A finalizer-based design would cause a slow RSS drift under this kind of churn; the explicit reference count avoids that entirely.

### 5.6. CPU Efficiency (~4.9 cores for 88,000 msg/s)

About 5 of 16 cores were enough to sustain 88,000 delivered messages/second across 21,000 connections — the other 11 sat idle.

1. **Idle connections cost nothing.** A write pump with nothing to send just blocks on a channel receive. Go's scheduler parks it without occupying an OS thread.

2. **`sync.Map` avoids serializing readers.** A traditional `map + RWMutex` would force every reader to wait during the window a writer holds the lock. `sync.Map` lets concurrent routing lookups proceed in parallel instead.

3. **Atomic counters remove mutex round-trips.** At 105,000 operations/second across many goroutines, `atomic.Add` is meaningfully cheaper than a mutex-guarded increment.

4. **No wasted zero-fill.** `readMessageWithFixedFromID()` allocates via jemalloc (no zero-fill) and reads the payload straight into the final buffer. The older approach — read into a Go slice, then copy into `mem.Buffer` — did an extra zero-fill *and* an extra copy on every single message.

### 5.7. GC Behavior Under Load

`alloc_bytes` swings between ~800 MB and ~1.5 GB while `sys_bytes` stays fixed at ~2.29 GB and RSS stays flat at ~1.97 GB. Here's why those three numbers behave differently:

- `alloc_bytes` rises and falls with each GC cycle, as short-lived Go objects (channel metadata, routing temporaries, `OutMessage` values) get allocated and then reclaimed.
- `sys_bytes` stays fixed because the Go runtime holds onto reserved virtual memory pages rather than handing them back to the OS right away.
- RSS stays flat because the dominant memory consumer — jemalloc-managed message buffers — is freed deterministically through `Release()`, on its own schedule, independent of whatever the GC happens to be doing.

`heap_objects` oscillating between 3–16 million confirms the GC is actively working. Since neither `alloc_bytes` nor RSS trends upward over 38 hours, buffer ownership is confirmed leak-free.

---

## 6. Performance Model: Why the Relay Server Is Fast

There's no single trick behind these numbers — it's a deliberately small, copy-minimized hot path: read one binary WebSocket frame, parse fixed offsets, route by recipient key, enqueue to destination write pumps, release memory via reference counting. Each subsection below is one piece of that path.

### 6.1. Binary Protocol, No Deserialization

Clients send `websocket.MessageBinary` frames in a compact, fixed layout:

```
ToIDsLen(1) | ToIDs(N×32) | DataLen(4) | Data(DataLen)
```

The server never accepts a client-supplied `FromID` — it stamps the authenticated identity into the message itself before calling the App. Delivered (server → client) frames use the full layout, with `FromID(32)` and a privacy-zeroed recipient list.

Recipient IDs and payload length are read straight from fixed byte offsets. There's no JSON, protobuf, map, or nested struct anywhere on the routing path — so CPU cost per message is predictable, and there's no allocation at the parsing step.

### 6.2. Incremental Read with Exact-size Allocation

`readMessageWithFixedFromID()` reads the header first, validates `ToIDsLen` and `DataLen`, then allocates one `mem.Buffer` sized exactly for the payload and reads directly into it.

The older pattern did this:

```
WebSocket read → Go heap []byte → mem.Buffer clone → relay
```

The current path does this instead:

```
WebSocket reader → parse header → stamp authenticated FromID → allocate exact mem.Buffer → read payload into final buffer
```

That removes one full-message copy and one Go heap allocation from every single received frame.

### 6.3. Zero-copy Message View Inside the Relay

`core.Message` is just a `[]byte` view over the raw buffer — `ToIDsLen()`, `ToIDAt()`, and `ZeroToIDs()` all read and write that same slice directly. The relay never builds a separate message object or recipient structure per frame.

This isn't zero-copy all the way down to the kernel or the WebSocket library — but inside the relay itself, there are no extra application-level copies or allocations during routing.

### 6.4. One Shared Buffer for Multicast

For a targeted multicast, exactly one buffer is allocated per inbound message — never one per recipient. Before the message is queued for each destination, the reference count goes up by one (`Retain()`). Each write pump calls `Release()` after it finishes writing.

```
1 inbound buffer
N retained references (one per recipient)
N write pumps release independently
last Release() → C.free()
```

So sending to more recipients does not multiply payload memory.

### 6.5. In-place Privacy Cleanup

Before routing, `ZeroToIDs()` clears the recipient list inside the shared buffer, in place — the layout and payload offset don't change, so recipients simply can't see who else the message went to. Because it's in-place, there's no new outgoing payload to build.

### 6.6. Non-blocking Routing Path

The goroutine that reads and routes a message never writes to a destination socket directly. It calls `SafePush()` into that destination's `outChan` and moves on. The actual network write happens in the destination's own write pump. So a slow client only ever delays its own write pump — it can't block the sender's read loop, or any other client's routing.

```
parse → lookup recipient → SafePush → next recipient
```

### 6.7. Slow-client Isolation and Bounded Queues

Every connector has a bounded `outChan`. If it's full, `SafePush()` just returns `false` and the message is dropped for that one destination. This is what stops one slow or broken connection from accumulating unbounded memory, or blocking clients that have nothing to do with it.

Queue capacity comes from the `outBufSize` parameter passed to `NewWSConnector` (256 if `<= 0`). There's no single "right" value baked into the code — it depends on your deployment's burst profile and memory budget.

A bigger buffer absorbs bursts better but raises per-connection RAM use. A smaller buffer uses less memory but starts dropping messages sooner under a spike.

### 6.8. Bounded Fanout

`MaxTargetsPerMessage` caps how many recipients one frame can name, which keeps routing cost per frame bounded and stops a malicious or buggy client from forcing unbounded work with a single message.

`MaxMessageSize` caps the `Data` field. For workloads with small, predictable payloads — MPC or signing protocols, for example — setting this tighter (e.g. 16–64 KB) reduces how much memory a slow or adversarial client can force the server to hold.

### 6.9. Reduced GC Pressure via Explicit Buffer Ownership

On Linux with CGO, `mem.Buffer` uses `malloc`/`free` for message payload buffers, so they live outside the Go heap and get freed deterministically via `Release()` the moment the last recipient's write completes.

The real benefit isn't that jemalloc holds some "stable pool" of memory — these buffers are short-lived and freed right after each delivery. The benefit is that high-throughput payload memory never becomes Go heap garbage, which means less GC work and more stable p95/p99 latency (fewer, shorter GC pauses).

The actual per-connection RSS floor comes from goroutine stacks, channel buffers, connector structs, runtime bookkeeping, registry entries, and socket buffers — not from any long-lived jemalloc allocation.

### 6.10. Low-contention Shared State

The connection registry uses `sync.Map`, which matches the access pattern here: routing does many lookups per second, while register/unregister events are comparatively rare. Metrics counters use atomics rather than a mutex-guarded increment.

`SafePush()` holds a read-lock only for the duration of the non-blocking channel send — just enough to coordinate with `Close()` and avoid sending into a closed channel. It's not on the actual message-data path, and the lock is released immediately after the channel operation.

### 6.11. Cheap Idle Connections

Every connection has a read pump and a write pump. An idle write pump just blocks on a channel receive and uses zero CPU while waiting. Go's scheduler multiplexes many such goroutines onto a much smaller number of OS threads, so tens of thousands of mostly-idle connections stay cheap.

That's why the benchmark can hold ~20,000–25,000 concurrent connections while using only ~5 of 16 available CPU cores.

---

## 7. Distributed Deployment Extensibility

Everything above (§1–§6) describes a single process. This section is about what happens once one process isn't enough — what the current architecture does, and doesn't, give you toward running a cluster of relay nodes.

### 7.1. What the architecture provides: an extension seam, not a runtime

`internal/network`'s entire job, spelled out plainly:

```text
WebSocket frame → parse client envelope → stamp authenticated identity → app.HandleMessage(from, m)
```

That's it — it has no opinion on what happens next. `core.App.HandleMessage` owns the *complete* routing decision (§2.2), so a domain implementation is free to do a lot more than look something up in a local `sync.Map`. Nothing in `core`, `network`, or `server` assumes the recipient has to be reachable through a local `Connector` — that assumption is entirely inside `domains.Relayer.HandleMessage`, and it's a policy choice, not a hard constraint.

A cluster-aware `App` could be dropped in without touching any other layer:

```go
func (r *DistributedRelayer) HandleMessage(from core.Connector, m core.OutMessage) {
    var targets [core.MaxTargetsPerMessage][32]byte
  msg := m.Msg()
    n := core.ExtractTargets(msg, from.ID(), &targets)
    msg.ZeroToIDs()

    for _, target := range targets[:n] {
        if dest, ok := r.localRegistry.GetConnectorByKey(target); ok {
            core.DeliverTo(dest, m) // local hop, existing primitive
            continue
        }
        if node, ok := r.directory.LookupOwner(target); ok {
            r.interNode.Forward(node, target, m) // this node's own logic
            continue
        }
        r.IncrementDeliveryFailure()
    }
}
```

`core.ExtractTargets` and `core.DeliverTo` (§2.2) get reused as-is for the local-hop case; the remote-hop case is entirely new code that belongs to the domain, wired up in a new `cmd/<name>/main.go`. `internal/core`, `internal/network`, and `internal/server` don't change, and neither does the wire format (§6.1).

Two things make this practically workable, not just theoretically possible:

- **`Connector` is already an interface** (§2.2). A "remote" connector that forwards bytes to another node over gRPC/NATS/a custom TCP protocol — instead of writing to a local WebSocket — satisfies `core.Connector` exactly the same way `WSConnector` does. Nothing downstream can tell the difference.
- **The retain/release contract on `buf` doesn't assume a single machine** (§2.2, §6.4). `DeliverTo` retains before a local push and releases if the push gets dropped; an inter-node sender has to follow the same discipline — retain until the payload bytes are actually copied onto the wire (or safely past the point where they're still needed), then release exactly once. It's the same rule `WSConnector`'s write pump already follows for local delivery. A distributed connector doesn't get a special contract, just a different transport underneath the same one.

### 7.2. What the architecture does not provide

None of the following exists in this codebase today, and nothing here is designed to provide it:

- Cluster membership or discovery
- A routing/ownership directory (`directory.LookupOwner` in the example above is a stub — no such type exists)
- Inter-node transport
- Consensus, replication, or sharding
- Failure detection or automatic failover
- Distributed tracing

This is a deliberate omission, not an oversight: different `App`s need fundamentally different distribution models. A relay routes by `PubKey → owning node`; a game server routes by `Room → authoritative node`; an MPC service routes by `SessionID → participant set`. Baking one specific model — consistent hashing, a Redis-backed registry, a particular replication scheme — into `core` would force every `App` through a shape that fits few of them well. Instead, `core` handles the one thing every `App` needs regardless of topology (frame in, routing decision out), and leaves the distribution model as application-level policy: **the transport owns delivery mechanics; the App owns routing and distribution semantics.**

### 7.3. What a distributed `App` still has to solve on its own

Having this extension seam does not make a correct distributed system automatic. Any concrete cluster-aware `App` built on top of §7.1 still has to solve the usual hard problems of distributed systems — none of which `core.App` abstracts away:

- A node dying mid-route, after ownership lookup but before delivery
- Stale ownership data (the directory says node B, but the client reconnected to node C)
- The same client momentarily registered on two nodes during a reconnect/migration race
- Duplicate delivery from retries, or out-of-order delivery across nodes
- Inter-node queue backpressure and what to do when it's full
- Split-brain between nodes that disagree on ownership
- Version skew during a rolling deployment
- A recipient's ownership migrating while a message for them is already in flight

These aren't gaps in the layering — they're exactly the problems a distributed implementation is responsible for solving, and no amount of interface design at the `core` layer makes them go away.

### 7.4. A concrete single-node assumption worth knowing

One specific spot where the shipped `App` (`domains.Relayer`) is single-node by construction, not by contract: `Count()` and `FetchMetrics()` (§2.2, §2.3) only ever see connections registered via `OnConnect` on the local process. In a multi-node deployment, `/metrics`' `active_connections` and `app_metrics` (§2.5) each report *that node's* view only — nothing in this codebase aggregates across nodes. If you want fleet-wide numbers, you have to aggregate `/metrics` across nodes yourself (e.g. at the scrape/dashboard layer) — nothing here does it for you.

### 7.5. Summary

| Aspect | Status |
|---|---|
| Extension seam for distributed routing (`core.App.HandleMessage`) | Present, and reusable via `core.ExtractTargets`/`core.DeliverTo` for the local-hop case |
| `Connector` as a transport-agnostic delivery target | Present — a remote connector is a normal `core.Connector` |
| Cluster membership, directory, inter-node transport, consensus, replication | Not present — left to the concrete `App` |
| Cluster-wide metrics | Not present — `/metrics` is per-node |
| Correctness under node failure, stale ownership, duplication, ordering | Not addressed by this layer — owned entirely by whatever distributed `App` is built |

In short: this is a **single-node relay with a routing seam that doesn't need to be reworked in order to grow into a distributed one** — it is not a distributed system today, and this document isn't claiming otherwise anywhere else.
