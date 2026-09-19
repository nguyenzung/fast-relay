# Fast Relay Communication Protocol

This document explains how to build and send messages using Fast Relay's binary protocol — the exact byte layout the server expects and returns.

## Quick summary

- You connect once with your public key. From then on, the server always knows who you are — you never include your own key in a message you send.
- To send a message, you say who it's for (1–10 recipient pubkeys) and attach a payload. The server delivers it as-is, without reading or understanding the contents.
- When you receive a message, it always tells you who it's *from* (the server stamped that itself, so you can trust it) but the list of *other recipients* is wiped out before it reaches you, for privacy.
- There is no broadcast. If you want 5 people to get a message, you list all 5 pubkeys yourself.

## 1. Message Frame Layout

The frame you **send** to the server and the frame you **receive** from it are not the same shape — that's intentional. The server already knows who you are from authentication (the `pub` query param you connected with), so it never trusts a sender identity claimed inside a message. It strips any such claim from what you send, and stamps your real, authenticated identity onto what it delivers to your recipients instead.

**Frame you send (client → server):**

```
[0]      ToIDsLen    (1 byte)     - Number of recipients (N)
[1:..]   ToIDs       (N*32 bytes) - List of recipient public keys (each 32 bytes)
[?]      DataLen     (4 bytes)    - Payload length (big-endian uint32)
[?]      Data        (variable)   - Payload (DataLen bytes)
```

**Frame you receive (server → client):**

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

Field-by-field:

- **FromID** — 32-byte sender public key. Only appears on a *delivered* frame; you never send this field yourself, and the server would ignore it even if you tried — it always derives this from your authenticated connection.
- **ToIDsLen** — the recipient count N (1–10; **0 means the frame is silently discarded**, see §4). On a frame you send, this is how many recipients you're targeting. On a frame you receive, it's still the sender's original N — **not reset to 0** — the server only wipes the `ToIDs` bytes themselves (see `ZeroToIDs` in the source), never this length byte. You still need to read it, purely to know how many zeroed `ToIDs` bytes to skip before `DataLen`.
- **ToIDs** — on a sent frame: the recipients' 32-byte public keys (when `ToIDsLen > 0`); every key must be distinct, a repeated recipient gets the message rejected (see §4). On a delivered frame: `N*32` zero bytes — the server strips the real recipient list before relaying, so recipients can't see who else got the message. Skip past them using `ToIDsLen`, same as when building a frame to send.
- **DataLen** — a 4-byte big-endian unsigned integer (uint32), so it supports large payloads.
- **Data** — the binary payload itself (encrypted or plaintext — the server doesn't care).

## 2. Constructing a Message

### a. Targeted message (to one or more recipients)

1. Don't include a `FromID` — the server already knows who you are.
2. Set `ToIDsLen` to your recipient count (1–10, each one distinct).
3. Append each recipient's 32-byte public key to `ToIDs`.
4. Set `DataLen` to your payload's length (uint32 — sizes over 65535 are fine).
5. Append `Data` (your payload).

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

The relay never fans a message out to "everyone except the sender" — there's no such mode. Setting `ToIDsLen = 0` does **not** mean broadcast; it means the frame has no recipients at all, and the server **silently discards it** (see §4). If you want to reach several people, list every one of them explicitly in `ToIDs` (up to `MaxTargetsPerMessage = 10`, see §2a). Deciding who "everyone" is is entirely up to your client — the server has no concept of it.

## 3. Receiving a Message

A delivered frame always has the server → client shape from §1, which means it always carries a `FromID`. To read it:

1. Parse `FromID` (the sender's pubkey — stamped by the server, so trust it; it is not something the sender asserted).
2. Read `ToIDsLen` (N — the sender's original recipient count, **not 0**; only the bytes after it were zeroed).
3. Skip `N*32` bytes of zeroed `ToIDs`.
4. Read `DataLen` (4 bytes, big-endian uint32).
5. Read `Data` (the payload).

## 4. Notes

- All public keys are 32 raw bytes in the frame (not hex-encoded).
- `DataLen` is big-endian (network byte order), a 4-byte uint32.
- The server never inspects or modifies `Data` — encryption, if any, is end-to-end between clients.
- A frame you send has no `FromID`; a frame you receive always does (server-stamped) — see §1.
- A delivered frame keeps the sender's original `ToIDsLen` (N) even though the `ToIDs` bytes themselves are zeroed — so `DataLen` sits at offset `33 + N*32`, never a fixed offset — see §1/§3.
- If `ToIDsLen = 0`, the frame is **silently discarded**: no recipients, nothing relayed, connection stays open, no broadcast fallback.
- If `ToIDsLen > 10` (`MaxTargetsPerMessage`), that's a **protocol violation** and the connection is closed.
- `ToIDs` must not repeat a recipient — a duplicate is also a **protocol violation** (connection closed). This exists to stop one frame from being amplified into multiple deliveries to the same recipient.
- A frame with `ToIDsLen` between 1 and 10 and no duplicates is delivered to exactly those recipients, if they're online.
- If a recipient is offline, their copy of the message is simply dropped — there's no queue or retry.

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

- Always encrypt sensitive payloads (e.g., MPC shares, private keys) before sending — the server does not encrypt anything for you.
- Validate all public keys and payload sizes on your end before constructing a frame.
- Don't rely on the relay server for confidentiality or message integrity — it only moves bytes; it does not protect them.

## 7. Suggested: End-to-End Encryption via ECDH (optional, client-side)

The relay never sees this — it's a suggested pattern for how two clients can encrypt `Data` themselves, using the pubkeys they already have. The server takes no part in it, doesn't verify any of it, and the wire format doesn't change: the encrypted bytes are simply what you put in `Data`.

**Important caveat first:** the 32-byte "pubkey" you connect with (`?pub=`) is, as far as the server is concerned, just a routing label — the `DefaultAuthenticator` accepts any 32 bytes, it does not check that they form a valid point on any curve. If you want to use it for ECDH too, that's a convention you and your peers agree on among yourselves, not something the protocol enforces.

### 7.1. The idea

This works with any elliptic curve that supports Diffie-Hellman (Curve25519, NIST P-256, etc.) — the mechanism below is the same regardless of which one you pick, so it's described here in general terms rather than tied to one curve.

1. Each client generates an EC keypair on whichever curve its crypto library supports, and uses the **public key** (a point on that curve) as its routing pubkey — the same 32 (or however many) bytes you pass as `?pub=` and put in `ToIDs`.
2. To send an encrypted message to a peer, derive a shared secret from your private key and their public point (ECDH), then use that shared secret to encrypt `Data` before building the frame.
3. The recipient derives the exact same shared secret from their own private key and `FromID` (the sender's pubkey/public point, which the server already stamped for you — see §1/§3), and uses it to decrypt.

ECDH is symmetric in the sense that both sides land on the same secret: `ECDH(myPriv, theirPub) == ECDH(theirPriv, myPub)` — scaling the other side's public point by your own private scalar gives the same point either way. Neither side needs to send the secret itself, only their already-public point, which the relay is already carrying for routing.

### 7.2. Simplest option: an ECDH-based `crypto_box` primitive

If your crypto library has a NaCl/libsodium-style "box" primitive (e.g. `tweetnacl`'s `nacl.box`, libsodium's `crypto_box` — these use Curve25519 under the hood, but the same idea applies with any curve your library offers), it does steps 2–3 above in one call — ECDH plus authenticated encryption together — so you don't have to wire up ECDH, a KDF, and an AEAD cipher separately.

```ts
import nacl from "tweetnacl";

// Once per client: generate and keep this keypair. keyPair.publicKey is
// what you connect with (?pub=) and what goes in ToIDs.
const keyPair = nacl.box.keyPair();

// Sending to peerPublicKey (their routing pubkey):
function encryptPayload(plaintext: Uint8Array, peerPublicKey: Uint8Array, myPrivateKey: Uint8Array): Uint8Array {
  const nonce = nacl.randomBytes(24); // must be unique per message
  const box = nacl.box(plaintext, nonce, peerPublicKey, myPrivateKey);
  // Prepend the nonce — the recipient needs it to decrypt. It isn't secret.
  const out = new Uint8Array(nonce.length + box.length);
  out.set(nonce);
  out.set(box, nonce.length);
  return out; // this is what you put in Data
}

// Receiving from senderPublicKey (= FromID on the delivered frame):
function decryptPayload(data: Uint8Array, senderPublicKey: Uint8Array, myPrivateKey: Uint8Array): Uint8Array | null {
  const nonce = data.slice(0, 24);
  const box = data.slice(24);
  return nacl.box.open(box, nonce, senderPublicKey, myPrivateKey); // null if tampered/wrong key
}
```

If you're not using NaCl-style libraries, the equivalent manual sequence is: `ECDH(myPrivateKey, peerPublicPoint)` → HKDF to derive a symmetric key → encrypt with an AEAD cipher (e.g. XChaCha20-Poly1305 or AES-256-GCM) using a fresh random nonce per message, and send `nonce || ciphertext` as `Data`.

### 7.3. What this does and doesn't give you

- **Confidentiality and integrity** against the relay itself and any network observer: the server (and anyone who compromises it) sees only ciphertext.
- **No forward secrecy**, as described here. Both sides reuse the *same* long-lived EC keypair for every message. If either private key leaks later, every message ever captured between that pair can be decrypted retroactively. For most relay use cases (MPC coordination, signing) this is an acceptable tradeoff; for anything where past-message secrecy matters, you'd need per-session ephemeral keys or a ratchet (Signal-style Double Ratchet) layered on top — a meaningfully bigger design than what's sketched here, and out of scope for this doc.
- **No protection against a malicious relay silently dropping or reordering messages** — that's a separate concern from encryption (see §6), and this scheme doesn't address it either.

---

For further details, see the Fast Relay README or contact the maintainer.
