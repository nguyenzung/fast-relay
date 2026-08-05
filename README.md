# Fast Relay

Fast Relay is a high-performance WebSocket relay server written in Go, optimized for message routing between clients identified by 32-byte public keys.

The server is designed to handle tens of thousands of concurrent connections with sub-millisecond delivery latency and predictable memory usage through a copy-minimized binary relay pipeline.

## Key Features

- **PubKey-based routing**: Route messages directly by 32-byte public key — no account system required.
- **Targeted multicast**: One frame can name up to 10 recipients; the server routes to each in one pass.
- **Copy-minimized hot path**: `readMessage()` reads the frame header first, allocates one exactly-sized buffer, and reads the payload directly in — no intermediate Go-heap copy. One buffer is shared across all recipients via reference counting (`Retain`/`Release`).
- **jemalloc on Linux**: Message buffers are allocated outside the Go heap (no zero-fill, no GC finalization). Released deterministically when the last write pump finishes.
- **Bounded per-connection memory**: ~97 KB RSS per connection at 21,000 concurrent connections.
- **Drop-on-full isolation**: Each connection has its own bounded outbound queue. A slow client is dropped rather than blocking others.
- **Metrics endpoint**: `/metrics` exposes active connections, throughput, latency percentiles, CPU, and memory in JSON.

## Binary Protocol

```text
FromID   (32 bytes)   — sender public key
ToIDsLen  (1 byte)    — number of recipients N (1–10; 0 = frame is discarded)
ToIDs    (N × 32 B)   — recipient public keys
DataLen   (4 bytes)   — payload length in bytes (big-endian uint32)
Data     (DataLen B)  — message payload
```

Before forwarding, the server zeroes the `ToIDs` field in-place so recipients cannot see each other's public keys.

`MaxTargetsPerMessage = 10`. Frames with `ToIDsLen > 10` are treated as protocol violations and close the connection. Frames with `ToIDsLen = 0` are silently discarded (no recipients, connection stays open).

## Installation

**Requirements**: Go 1.23+, Linux (for jemalloc; other platforms fall back to `make`).

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

Tested on a single machine (Acer Nitro V15, 16 logical CPUs) with server and load generator co-located.

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

`active_connections` and `app_metrics` reflect this process only — in a multi-node deployment, each node exposes its own `/metrics`; there is no built-in cluster-wide aggregation (see [Distributed Deployment](#distributed-deployment) below).

## Architecture

See [`Architecture.md`](Architecture.md) for component details, data flow, and the performance model explaining why the server achieves these numbers.

## Distributed Deployment

This server is single-node: there is no built-in cluster membership, cross-node routing, or replication. What it does provide is a routing seam that a distributed implementation can be built on without changing the transport.

`internal/network` only parses the wire frame and hands it to `app.HandleMessage(...)` — it has no opinion on where a recipient lives. The shipped `domains.Relayer` resolves recipients through a local in-memory registry, but that's a policy choice made inside `HandleMessage`, not something `internal/core`, `internal/network`, or `internal/server` assume. A cluster-aware `App` can look up a local connector first and forward to another node for everything else, reusing the existing `core.ExtractTargets`/`core.DeliverTo` primitives for the local case, without touching any other layer.

That seam does not include cluster membership, an ownership directory, inter-node transport, consensus/replication, or cluster-wide metrics — those are left to whatever distributed `App` you build, along with the usual distributed-systems problems (stale ownership, duplicate/out-of-order delivery, node failure mid-route, backpressure across nodes). See [Architecture.md §7](Architecture.md#7-distributed-deployment-extensibility) for the full breakdown.

## License

MIT
