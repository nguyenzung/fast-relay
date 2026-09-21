# Fast Relay

Fast Relay is a WebSocket relay server written in Go. It routes binary messages between clients, where each client is identified by a 32-byte public key instead of a username or account.

In plain terms: a client connects, tells the server "I am pubkey X", and can then send a message to any other pubkey(s). The server does not know or care what's inside the message — it just delivers it to the right recipient(s), fast.

It's built to handle tens of thousands of connections at once, deliver messages in well under a millisecond, and keep memory use per connection small and predictable.

## Key Features

- **PubKey-based routing** — no accounts, no login flow. A client is simply "whoever holds this public key." Messages are routed straight to the target pubkey(s).
- **Targeted multicast** — one message can list up to 10 recipients, and the server delivers to all of them in a single pass.
- **Copy-minimized hot path** — the function that reads an incoming message (`readMessageWithFixedFromID()`) reads just enough of the header to know the exact message size, stamps the sender's verified identity onto it, and reads the payload straight into one right-sized buffer. No extra copy in between. That one buffer is then shared by every recipient (via reference counting — see below) instead of being duplicated per recipient.
- **jemalloc on Linux** — message buffers are allocated outside Go's normal memory heap. This skips a zero-fill step Go would otherwise do on every allocation, and frees the memory immediately (not on the GC's schedule) once the last recipient has read it.
- **Bounded per-connection memory** — about 97 KB of RAM per connection, measured at 21,000 concurrent connections.
- **Drop-on-full isolation** — each connection has its own outgoing message queue with a fixed capacity. If a client is too slow to keep up and its queue fills up, new messages to it are dropped instead of piling up in memory or stalling everyone else.
- **Metrics endpoint** — `/metrics` reports active connections, throughput, latency percentiles, CPU, and memory as JSON.

### What "reference counting" means here

When a message goes to multiple recipients, the server doesn't copy the message N times. It allocates the message once and keeps a counter of how many recipients still need to read it (`Retain()` adds to the counter, `Release()` subtracts). When the counter hits zero — meaning every recipient's write has finished — the memory is freed immediately.

## Binary Protocol

The frame a client **sends** and the frame it **receives** are shaped differently. That's on purpose: the server always knows who a client is (from `?pub=` at connection time), so it never trusts a client's own claim about who a message is "from" — it stamps that in itself.

```text
# Client -> server
ToIDsLen  (1 byte)    — number of recipients N (1–10; 0 = frame is discarded)
ToIDs    (N × 32 B)   — recipient public keys
DataLen   (4 bytes)   — payload length in bytes (big-endian uint32)
Data     (DataLen B)  — message payload

# Server -> client (delivered)
FromID   (32 bytes)   — sender public key, stamped by the server from the authenticated
                         connection, never from the client's own frame
ToIDsLen  (1 byte)    — still N, the sender's original recipient count (NOT zeroed —
                         only the ToIDs bytes below are, see below)
ToIDs    (N × 32 B)   — zero bytes
DataLen   (4 bytes)   — payload length in bytes (big-endian uint32), at offset 33+N*32
Data     (DataLen B)  — message payload
```

Before forwarding a message, the server does two things to it in place:
1. Zeroes out the `ToIDs` bytes, so a recipient can't see who else the message was sent to.
2. Stamps `FromID` with the sender's real, authenticated pubkey, so a recipient can't be shown a spoofed sender.

One detail worth remembering: the delivered frame keeps the original `ToIDsLen` count (it's *not* zeroed, only the `ToIDs` key bytes are) — so a client reading a delivered frame must still read that count to know how many zero bytes to skip before `DataLen`. `DataLen`/`Data` always start at `33 + ToIDsLen*32`, never a fixed offset.

`MaxTargetsPerMessage = 10`. A frame listing more than 10 recipients is treated as a protocol violation and the connection is closed. A frame listing 0 recipients is simply dropped (the connection stays open — this is not an error).

## Installation

**Requirements**: Go 1.23+, Linux (for jemalloc; other platforms fall back to Go's built-in `make`).

```bash
git clone https://github.com/nguyenzung/relay-server.git
cd relay-server
make build
```

## Running

```bash
./bin/relayer -addr :8080 -outbuf 256
```

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8080` | Listen address |
| `-outbuf` | `256` | Per-connection outbound queue depth |

## Performance (Linux, 38-hour churn test)

Tested on a single machine (Acer Nitro V15, 16 logical CPUs) with the server and load generator running side by side ("churn" means clients are continuously connecting and disconnecting throughout the test, not just holding one steady connection).

| Metric | Value |
|---|---|
| Concurrent connections | ~21,000 |
| Processed throughput | ~104,600 msg/s |
| Delivered throughput | ~87,600 msg/s |
| Latency p50 | 0.021 ms |
| Latency p95 | 0.042 ms |
| Latency p99 | 0.074 ms |
| RSS per connection | ~97 KB |
| Total RSS | ~2.07 GB |
| CPU used | ~5 of 16 cores |
| Test duration | 38.24 hours |
| Total messages processed | 14.4 billion |
| Memory leak observed | None |

Full analysis: [`test-result/analyst.md`](test-result/analyst.md)

## Load Testing

```bash
# Stress test: 20,000 concurrent clients
make stress ARGS="-n 20000 -m 5 -addr localhost:8080"

# Churn test: continuous connect/disconnect cycles
make churn ARGS="-n 1000 -m 10 -addr localhost:8080"
```

## Metrics Endpoint

`GET http://localhost:8080/metrics` returns JSON with:

```json
{
  "active_connections": 20886,
  "cpu_percent": 493.88,
  "alloc_bytes": 1008676352,
  "sys_bytes": 2246647808,
  "uptime_seconds": 137542.1,
  "app_metrics": {
    "processed_messages": 14398018519,
    "delivered_messages": 12054818090,
    "no_recipient_messages": 12345,
    "latency_p50_ms": 0.021,
    "latency_p95_ms": 0.042,
    "latency_p99_ms": 0.074
  }
}
```

`active_connections` and `app_metrics` describe only this one process. If you run several nodes, each exposes its own `/metrics`, and nothing aggregates them across nodes for you — see [Distributed Deployment](#distributed-deployment) below.

## Architecture

See [`Architecture.md`](Architecture.md) for how the pieces fit together, how a message flows through the system, and why the server performs the way it does.

## Distributed Deployment

This server runs as a single process — there's no built-in way to run several nodes as one cluster, no automatic routing between nodes, and no data replication. What it *does* give you is a clean seam where a distributed setup could be built on top, without touching the networking code.

Here's why that seam exists: `internal/network` only parses the incoming bytes and hands the parsed message to `app.HandleMessage(...)`. It has no opinion on where the recipient actually lives. The relay logic that ships with this server (`domains.Relayer`) happens to look recipients up in a local, in-memory map — but that's a choice made inside `HandleMessage`, not something baked into the lower layers. If you wanted a cluster-aware version, you could write an `App` that checks the local registry first and forwards to another node otherwise, reusing the same `core.ExtractTargets`/`core.DeliverTo` building blocks for the local case, without changing anything else in the server.

That seam does *not* include the hard parts of running a cluster: knowing which nodes exist, an ownership directory (which node owns which pubkey), the network transport between nodes, consensus/replication, or metrics that span the whole cluster. Those are all left for whoever builds the distributed `App`, along with the usual distributed-systems headaches (stale ownership info, duplicate or out-of-order delivery, a node dying mid-route, backpressure between nodes). See [Architecture.md §7](Architecture.md#7-distributed-deployment-extensibility) for the full picture.

## License

MIT
