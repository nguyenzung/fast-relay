# Fast Relay Communication Protocol

This document explains how to construct and send messages to other parties using the Fast Relay server's binary protocol.

## 1. Message Frame Layout

All messages relayed via WebSocket must use the following binary format:

```
[0:32]   FromID      (32 bytes)   - Sender's public key (identity, 32 bytes)
[32]     ToIDsLen    (1 byte)     - Number of recipients (N)
[33:..]  ToIDs       (N*32 bytes) - List of recipient public keys (each 32 bytes)
[?]      DataLen     (4 bytes)    - Payload length (big-endian uint32)
[?]      Data        (variable)   - Payload (DataLen bytes)
```

- **FromID**: 32-byte sender public key (hex-encoded in registration, raw bytes in frame)
- **ToIDsLen**: Number of recipients (0 = broadcast to all except sender)
- **ToIDs**: List of 32-byte recipient public keys (if ToIDsLen > 0)
- **DataLen**: 4-byte big-endian unsigned integer (uint32, supports large payloads)
- **Data**: Binary payload (protocol message, encrypted or plaintext)

## 2. Constructing a Message

### a. Targeted Message (to one or more parties)

- Set FromID to your 32-byte public key
- Set ToIDsLen to the number of recipients (N > 0)
- For each recipient, append their 32-byte public key to ToIDs
- Set DataLen to the length of your payload (uint32, supports > 65535)
- Append Data (payload)

**Example (pseudo-code):**

```js
const from = ... // Uint8Array(32) sender pubkey
const to = [peer1Pub, peer2Pub]; // Array of Uint8Array(32)
const payload = ... // Uint8Array

const buf = new Uint8Array(32 + 1 + to.length*32 + 4 + payload.length);
buf.set(from, 0);
buf[32] = to.length;
for (let i = 0; i < to.length; ++i) {
  buf.set(to[i], 33 + i*32);
}
const dataLenOffset = 33 + to.length*32;
buf[dataLenOffset]   = (payload.length >> 24) & 0xff;
buf[dataLenOffset+1] = (payload.length >> 16) & 0xff;
buf[dataLenOffset+2] = (payload.length >> 8) & 0xff;
buf[dataLenOffset+3] = payload.length & 0xff;
buf.set(payload, dataLenOffset+4);
// send buf over WebSocket
```

### b. Broadcast Message (to all except sender)

- Set ToIDsLen = 0
- Omit ToIDs
- DataLen and Data as above

**Example:**

```js
const buf = new Uint8Array(32 + 1 + 4 + payload.length);
buf.set(from, 0);
buf[32] = 0; // broadcast
buf[33] = (payload.length >> 24) & 0xff;
buf[34] = (payload.length >> 16) & 0xff;
buf[35] = (payload.length >> 8) & 0xff;
buf[36] = payload.length & 0xff;
buf.set(payload, 37);
```

## 3. Receiving a Message

- Parse FromID (sender pubkey)
- Read ToIDsLen (N)
- If N > 0, read N*32 bytes for ToIDs
- Read DataLen (4 bytes, big-endian uint32)
- Read Data (payload)

## 4. Notes

- All public keys are 32 bytes (raw, not hex in frame)
- DataLen is big-endian (network order) and encoded as a 4-byte uint32
- The relay server does not inspect or modify Data; encryption is end-to-end
- If ToIDsLen = 0, the message is broadcast to all online parties except the sender
- If ToIDsLen > 0, the message is delivered only to the listed recipients (if online)
- If a recipient is offline, the message is dropped (no queue)

## 5. Example Usage (TypeScript)

```ts
function encodeFrame(from: Uint8Array, to: Uint8Array[], payload: Uint8Array): Uint8Array {
  const buf = new Uint8Array(32 + 1 + to.length*32 + 4 + payload.length);
  buf.set(from, 0);
  buf[32] = to.length;
  for (let i = 0; i < to.length; ++i) {
    buf.set(to[i], 33 + i*32);
  }
  const dataLenOffset = 33 + to.length*32;
  buf[dataLenOffset]   = (payload.length >> 24) & 0xff;
  buf[dataLenOffset+1] = (payload.length >> 16) & 0xff;
  buf[dataLenOffset+2] = (payload.length >> 8) & 0xff;
  buf[dataLenOffset+3] = payload.length & 0xff;
  buf.set(payload, dataLenOffset+4);
  return buf;
}

// To send:
// ws.send(encodeFrame(myPub, [peerPub], payload));
```

## 6. Security Considerations

- Always encrypt sensitive payloads (e.g., MPC shares, private keys) before sending
- Validate all public keys and payload sizes before constructing frames
- Do not trust relay server for confidentiality or message integrity

---

For further details, see the Fast Relay README or contact the maintainer.
