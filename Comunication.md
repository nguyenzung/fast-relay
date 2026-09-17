# Fast Relay Communication Protocol

This document explains how to construct and send messages to other parties using the Fast Relay server's binary protocol.

## 1. Message Frame Layout

The frame you **send** to the server and the frame you **receive** from it are not the same shape. The server already knows who you are from authentication (the `pub` query param on connect), so it never trusts a client-supplied sender id — it strips FromID from what you send and stamps your authenticated identity onto what it delivers to recipients instead.

**Frame you send (client -> server):**

```
[0]      ToIDsLen    (1 byte)     - Number of recipients (N)
[1:..]   ToIDs       (N*32 bytes) - List of recipient public keys (each 32 bytes)
[?]      DataLen     (4 bytes)    - Payload length (big-endian uint32)
[?]      Data        (variable)   - Payload (DataLen bytes)
```

**Frame you receive (server -> client):**

```
[0:32]   FromID      (32 bytes)   - Sender's public key, stamped by the server from its
                                     authenticated identity - not something the sender chose
[32]     ToIDsLen    (1 byte)     - N, the recipient count the sender targeted - UNCHANGED
                                     by relaying, only the bytes after it are zeroed
[33:..]  ToIDs       (N*32 zero bytes) - zeroed for privacy (see §4), but still N*32 bytes
                                     wide; DataLen starts after them, not at a fixed offset
[?]      DataLen     (4 bytes)    - Payload length (big-endian uint32)
[?]      Data        (variable)   - Payload (DataLen bytes)
```

- **FromID**: 32-byte sender public key. Only present on delivered frames; never sent by the client and never trusted from the client even if it were — the server derives it from the authenticated connection.
- **ToIDsLen**: Number of recipients N (1-10; **0 = frame is silently discarded**, see §4). On a sent frame this is your target count. On a delivered frame it is **the same N the sender sent, not 0** — the server only zeroes the bytes of `ToIDs` itself (see `ZeroToIDs` in the source), never the length byte. You must still read it to know how many (zeroed) `ToIDs` bytes to skip before `DataLen`.
- **ToIDs**: On a sent frame, the list of 32-byte recipient public keys (if ToIDsLen > 0); every ID must be distinct — a repeated recipient is rejected (see §4). On a delivered frame, `N*32` zero bytes (the server strips the actual recipient list before relaying so recipients can't see each other's keys) — skip them by `ToIDsLen`, same as parsing a sent frame.
- **DataLen**: 4-byte big-endian unsigned integer (uint32, supports large payloads)
- **Data**: Binary payload (protocol message, encrypted or plaintext)

## 2. Constructing a Message

### a. Targeted Message (to one or more parties)

- Do **not** include a FromID — the server already knows who you are from authentication
- Set ToIDsLen to the number of recipients (1 <= N <= 10; each must be distinct)
- For each recipient, append their 32-byte public key to ToIDs
- Set DataLen to the length of your payload (uint32, supports > 65535)
- Append Data (payload)

**Example (pseudo-code):**

```js
const to = [peer1Pub, peer2Pub]; // Array of Uint8Array(32)
const payload = ... // Uint8Array

const buf = new Uint8Array(1 + to.length*32 + 4 + payload.length);
buf[0] = to.length;
for (let i = 0; i < to.length; ++i) {
  buf.set(to[i], 1 + i*32);
}
const dataLenOffset = 1 + to.length*32;
buf[dataLenOffset]   = (payload.length >> 24) & 0xff;
buf[dataLenOffset+1] = (payload.length >> 16) & 0xff;
buf[dataLenOffset+2] = (payload.length >> 8) & 0xff;
buf[dataLenOffset+3] = payload.length & 0xff;
buf.set(payload, dataLenOffset+4);
// send buf over WebSocket
```

### b. There is no broadcast

There is no server-side broadcast: the relay never fans a message out to "everyone except the sender". `ToIDsLen = 0` does not mean broadcast — it means the frame has no recipients and is **silently discarded** (see §4). To reach multiple parties, list each of them explicitly in `ToIDs` (up to `MaxTargetsPerMessage = 10`, see §2a) — the client, not the server, decides who "everyone" is.

## 3. Receiving a Message

Delivered frames have the server -> client shape from §1 — they **do** carry FromID:

- Parse FromID (sender pubkey, stamped by the server — trust it, it is not the sender's own claim)
- Read ToIDsLen (N, the sender's original recipient count — **not 0**; only the bytes after it are zeroed)
- Skip N*32 bytes of zeroed ToIDs
- Read DataLen (4 bytes, big-endian uint32)
- Read Data (payload)

## 4. Notes

- All public keys are 32 bytes (raw, not hex in frame)
- DataLen is big-endian (network order) and encoded as a 4-byte uint32
- The relay server does not inspect or modify Data; encryption is end-to-end
- The frame you send has no FromID; the frame you receive always does (server-stamped) — see §1
- The frame you receive keeps the sender's original ToIDsLen (N) even though `ToIDs` itself is zeroed — DataLen is at offset `33 + N*32`, never a fixed offset — see §1/§3
- If ToIDsLen = 0, the frame is **silently discarded** (no recipients); the connection stays open, nothing is relayed, and there is no broadcast fallback
- If ToIDsLen > 10 (`MaxTargetsPerMessage`), the frame is a **protocol violation** and the connection is closed
- ToIDs must not contain a repeated recipient — a duplicate is a **protocol violation** and the connection is closed (prevents amplifying delivery of a single frame to the same recipient more than once)
- If ToIDsLen is between 1 and 10 with no duplicates, the message is delivered only to the listed recipients (if online)
- If a recipient is offline, the message is dropped (no queue)

## 5. Example Usage (TypeScript)

```ts
// Encodes the frame you SEND. The server adds FromID itself on delivery —
// do not include your own pubkey here.
function encodeFrame(to: Uint8Array[], payload: Uint8Array): Uint8Array {
  const buf = new Uint8Array(1 + to.length*32 + 4 + payload.length);
  buf[0] = to.length;
  for (let i = 0; i < to.length; ++i) {
    buf.set(to[i], 1 + i*32);
  }
  const dataLenOffset = 1 + to.length*32;
  buf[dataLenOffset]   = (payload.length >> 24) & 0xff;
  buf[dataLenOffset+1] = (payload.length >> 16) & 0xff;
  buf[dataLenOffset+2] = (payload.length >> 8) & 0xff;
  buf[dataLenOffset+3] = payload.length & 0xff;
  buf.set(payload, dataLenOffset+4);
  return buf;
}

// To send:
// ws.send(encodeFrame([peerPub], payload));

// Decodes a frame you RECEIVE (has FromID; ToIDsLen is the sender's original
// recipient count, NOT 0 - only the ToIDs bytes themselves are zeroed, so you
// must still read ToIDsLen and skip that many *32 bytes before DataLen).
function decodeFrame(data: Uint8Array): { from: Uint8Array; payload: Uint8Array } {
  const from = data.slice(0, 32);
  const toIDsLen = data[32];
  const dataLenOffset = 33 + toIDsLen * 32;
  const payloadLen =
    (data[dataLenOffset] << 24) |
    (data[dataLenOffset + 1] << 16) |
    (data[dataLenOffset + 2] << 8) |
    data[dataLenOffset + 3];
  const payload = data.slice(dataLenOffset + 4, dataLenOffset + 4 + payloadLen);
  return { from, payload };
}
```

## 6. Security Considerations

- Always encrypt sensitive payloads (e.g., MPC shares, private keys) before sending
- Validate all public keys and payload sizes before constructing frames
- Do not trust relay server for confidentiality or message integrity

---

For further details, see the Fast Relay README or contact the maintainer.
