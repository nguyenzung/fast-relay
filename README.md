# Fast Relay

Fast Relay is a high-performance WebSocket Relay Server written in Go, optimized for message routing between clients based on Public Keys (Ed25519 or equivalent).

The project focuses on handling tens of thousands of concurrent connections with extremely low latency and minimal resource consumption through a Zero-copy architecture and asynchronous processing.

## 🚀 Key Features

- **PubKey-based Routing**: Route messages directly using 32-byte Public Keys, no complex account registration required.
- **High Performance**: 
    - Zero-copy architecture for data payloads.
    - Efficient Multicast/Broadcast processing.
    - Intelligent memory management to reduce GC pressure.
- **Monitoring & Metrics**: Built-in `/metrics` endpoint providing real-time information on:
    - Number of active connections.
    - Message processing rates (Processed/Delivered).
    - Latency statistics (P50, P95, P99).
    - System resources (CPU, RAM).
- **Graceful Shutdown**: Ensures clean resource cleanup when stopping the server.

## 🛠 Binary Communication Protocol

The server uses an optimized binary protocol:

```text
[0:32]   FromID      (32 bytes)   - Sender's Public Key
[32]     ToIDsLen    (1 byte)     - Number of recipients (N)
[33:..]  ToIDs       (N*32 bytes) - List of recipient Public Keys
[?]      DataLen     (4 bytes)    - Payload length (Big-endian uint32)
[?]      Data        (variable)   - Message content (Payload)
```

*Note: If `ToIDsLen = 0`, the message is broadcast to all clients except the sender.*

## 📦 Installation & Usage

### Requirements
- Go 1.23+

### Installation
```bash
git clone https://github.com/nguyenzung/relay-server.git
cd relay-server
make build
```

### Running the Server
```bash
./bin/relayer -addr :8080 -outbuf 64
```

Parameters:
- `-addr`: Listen address (default `:8080`).
- `-outbuf`: Outbound queue buffer size per client (default `64`).

## 📊 Performance Testing

The project includes powerful stress testing tools in `cmd/loadtest` and `cmd/churntest`.

### Run Load Test (20k+ clients)
```bash
make stress ARGS="-n 20000 -m 5 -addr localhost:8080"
```

### Run Churn Test (Test stability with continuous connect/disconnect cycles)
```bash
make churn ARGS="-n 1000 -m 10 -addr localhost:8080"
```

## 🔍 Metrics Endpoint

Monitor server status at: `http://localhost:8080/metrics`

The returned JSON data includes:
- `active_connections`: Number of online clients.
- `processed_messages`: Total messages received by the server.
- `delivered_messages`: Number of messages successfully delivered to targets.
- `latency_p99_ms`: 99th percentile latency (ms).

## 📄 License

This project is released under the MIT License.
