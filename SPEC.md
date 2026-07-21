# portunus wire protocol, version 1

This is the normative description of what goes on the wire. It is written for
someone implementing the other end, and it deliberately contains no rationale:
[DESIGN.md](DESIGN.md) explains why the protocol is shaped this way, and where
the two disagree about behaviour, this document is the one that describes what
the implementation does.

"Must" and "must not" describe requirements. A receiver that detects a
violation terminates the session with GOAWAY and the stated error code.

## 1. Transport

The protocol runs over any reliable, ordered, bidirectional byte stream: TCP,
TLS, a Unix socket, an SSH channel. It assumes only that bytes arrive in the
order they were sent and that a broken stream is reported rather than silently
truncated. It provides no encryption, authentication, or integrity of its own.

Both endpoints must implement this specification; there is no negotiation with
other multiplexers and no compatibility with them.

## 2. Frames

Every frame is an 8-byte header followed by exactly `length` payload bytes.
All integers are big-endian.

```
byte 0            bytes 1-4          bytes 5-7
+--------------+------------------+-------------+
| type | flags |  stream ID (u32) | length (u24)|
+--------------+------------------+-------------+
  bits 0-3        bits 4-7
```

- **type** occupies the low four bits of byte 0.
- **flags** occupy the high four bits of byte 0.
- **length** is the payload size in bytes, so a frame carries at most
  16,777,215 bytes. A receiver's negotiated maximum frame size is normally far
  lower; see §5.

### 2.1 Types

| Value | Name | Payload |
|---|---|---|
| 0 | DATA | application bytes |
| 1 | WINDOW_UPDATE | u64 absolute credit limit |
| 2 | RST | u64 error code, u64 final size |
| 3 | STOP_SENDING | u64 error code |
| 4 | PING | u64 opaque |
| 5 | GOAWAY | u32 last stream ID, u64 error code, optional reason |
| 6 | SETTINGS | list of (u16 id, u64 value) |
| 7 | PADDING | reserved for a future version |

A receiver must treat type 7 and types 8 through 15 as a protocol error
(`PROTOCOL`). Reserving 7 rather than ignoring it keeps the option of adding
padding later without a version that silently discards it.

### 2.2 Flags

| Bit | Name | Meaning |
|---|---|---|
| 0x1 | SYN | this frame opens the stream |
| 0x2 | FIN | the sender has finished sending on this stream |
| 0x4 | ACK | this frame answers a PING or SETTINGS |
| 0x8 | — | reserved; senders must not set it |

SYN is valid only on DATA. A receiver must treat SYN on any other type as a
protocol error.

### 2.3 Control payload sizes

WINDOW_UPDATE, STOP_SENDING, and PING payloads are exactly 8 bytes. RST is
exactly 16. GOAWAY is at least 12 and at most 1036 (12 plus up to 1024 reason
bytes). A SETTINGS payload is a multiple of 10 bytes and carries at most 64
entries. A payload of any other size for these types is a frame size error
(`FRAME_SIZE`).

## 3. Streams

### 3.1 Identifiers

Stream ID 0 refers to the session and never to a stream. Streams initiated by
the client use odd identifiers, those initiated by the server use even ones,
so the two sides can never collide. Identifiers are 32-bit; an endpoint
approaching exhaustion must stop opening streams and start a new session
rather than reuse them.

### 3.2 Opening

A stream is announced by the first frame that carries it, which sets SYN. There
is no separate open frame and no round trip: an endpoint may allocate an
identifier and send data in the same frame.

**Announcement order is not identifier order.** Because identifiers are
allocated locally and a stream announces itself only when it first sends, a
peer may legitimately observe SYN for identifier 63 before SYN for identifier
1. A receiver must accept SYN for any identifier that is not currently live,
and must not treat "below the highest identifier seen" as reuse.

An endpoint that allocates an identifier and never sends on it has not opened
a stream as far as its peer is concerned, and must not later send any frame
referring to that identifier except one carrying SYN.

### 3.3 Ordering within a stream

A stream's frames must reach the peer in the order the sender issued them. In
particular a reset or a stop-sending for a stream must never be written before
the SYN that announces it: a receiver seeing a frame for a stream it has never
heard of is required to treat it as a protocol error, so reordering here
terminates the session.

### 3.4 Closing

FIN on a DATA frame closes the sender's direction: the peer will read the
preceding bytes and then end of file. The other direction stays usable.

RST abandons the sender's direction with an application error code and a final
size, the cumulative number of payload bytes the sender had sent. STOP_SENDING
asks the peer to stop sending, with an application error code; the peer's
writes then fail.

A stream is finished when both directions have ended, by FIN, RST, or
STOP_SENDING.

### 3.5 Concurrency limits

An endpoint advertises how many concurrently live peer-initiated streams it
will accept (§5). A stream occupies a slot until the accepting application
finishes with it, and specifically not until the peer resets it: freeing the
slot on a peer reset lets an attacker exceed the limit by opening and
resetting continuously.

An endpoint that cannot accept a stream must create no state for it, discard
its payload, and answer with RST carrying `REFUSED`. `REFUSED` is retriable;
an opener receiving it may try again, normally on a different session.

## 4. Flow control

Flow control is per stream and enforced by the sender. There is no
session-level window.

A receiver grants credit as an **absolute cumulative limit**: the total number
of payload bytes it is willing to receive on that stream since the stream
began. A sender must not send payload bytes beyond the current limit. Because
limits are absolute, a lost or duplicated update is harmless.

- The initial limit for every stream is the peer's advertised initial window
  (§5).
- WINDOW_UPDATE carries a new absolute limit. A limit lower than the current
  one is a flow control error. A limit equal to the current one grants nothing
  and is also a flow control error.
- A receiver that discards data — because its application abandoned the read
  side — must still advance the limit for those bytes. Otherwise a peer that
  is still sending exhausts its window and blocks with nothing to tell it why.
- Exceeding the limit is a flow control error (`FLOW_CONTROL`).

A receiver may raise a stream's limit whenever it chooses. Implementations are
expected to size windows from the observed bandwidth-delay product rather than
a constant, because a fixed window caps a stream at window ÷ round-trip time
regardless of available bandwidth.

## 5. Session establishment

Each endpoint sends SETTINGS as its first frame, on stream 0, before any other
frame. Neither side waits for the other's SETTINGS before sending; until a
peer's SETTINGS arrives, an endpoint must assume the protocol floors:

- initial window: 65,536 bytes
- maximum frame size: 16,384 bytes

A SETTINGS payload is a sequence of 10-byte entries, each a u16 identifier
followed by a u64 value.

| ID | Name | Meaning |
|---|---|---|
| 1 | VERSION | protocol version; must be 1 |
| 2 | INITIAL_WINDOW | initial per-stream credit, at least 65,536 |
| 3 | MAX_FRAME_SIZE | largest DATA payload accepted, at least 16,384 |
| 4 | MAX_CONCURRENT_STREAMS | peer-initiated streams accepted at once |
| 5 | FEATURE_BITS | reserved for future capabilities |
| 6 | PING_MIN_INTERVAL | minimum gap between PINGs, in milliseconds |

Unknown identifiers must be ignored, which is what makes the set extensible. A
version other than 1 is a `VERSION` error. A value outside the stated bounds
for identifiers 2 or 3 is a flow control or frame size error respectively.

SETTINGS on any stream other than 0 is a protocol error.

## 6. PING

PING carries eight opaque bytes. A receiver must answer a PING that does not
have ACK set by sending back a PING with ACK set and the same opaque value.
PING on any stream other than 0 is a protocol error.

PING serves both as a liveness check and as the round-trip measurement used to
size windows. An endpoint should judge liveness on the absence of *received*
frames rather than on its ability to send, since both peers can be blocked
sending at the same time.

An endpoint may police the rate of inbound PINGs, since each obliges a
response; see §8.

## 7. Session shutdown

GOAWAY carries the highest peer-initiated stream identifier that was
processed, an error code, and up to 1024 bytes of human-readable reason.

After sending GOAWAY an endpoint must not accept new streams. After receiving
GOAWAY, an endpoint's locally opened streams with identifiers above the stated
last identifier were never seen by the peer and must fail retriably; streams
at or below it continue normally.

A graceful shutdown must not close the underlying connection until frames
already queued have been written. Streams finish before the frames that
announce and end them necessarily reach the wire, so closing on stream count
alone discards them and the peer never learns those streams existed.

Error code 0 means no error: a normal drain.

## 8. Receiver rules

A conforming receiver applies these in order for every frame.

1. A frame whose type is unknown or reserved is a protocol error.
2. DATA, WINDOW_UPDATE, RST, or STOP_SENDING on stream 0 is a protocol error.
   SETTINGS or PING on any stream other than 0 is a protocol error.
3. A control payload of the wrong size is a frame size error. A DATA payload
   larger than the advertised maximum frame size is a frame size error.
4. SYN on a type other than DATA is a protocol error. SYN for a stream that is
   currently live is a protocol error.
5. SYN for an identifier that is not currently live opens a stream, subject to
   the concurrency limit of §3.5, whatever that identifier's relation to
   previously seen ones (§3.2).
6. A frame without SYN refers to a stream that has been torn down when its
   identifier is one already used: at or below the highest peer-initiated
   identifier seen, or at or below the highest locally-initiated identifier
   issued. Its payload is discarded and is not counted against any budget. If
   it was a DATA frame carrying payload, the receiver answers with
   STOP_SENDING, so that a peer blocked on credit for a stream that no longer
   exists learns to stop rather than waiting for credit that cannot come.
7. A frame without SYN for an identifier that has never been used is a
   protocol error.
8. DATA after FIN on the same stream is a protocol error.
9. Payload beyond the current credit limit is a flow control error.
10. WINDOW_UPDATE that lowers the limit, or leaves it unchanged, is a flow
    control error.

A receiver may additionally bound the rate of frames that cost it work while
consuming no credit — PINGs, empty DATA frames that neither open nor close a
stream, and stream resets — and terminate a session that exceeds those bounds
with the `CALM` code. Exceeding a rate limit is a judgment about behaviour
rather than a protocol violation, which is why it has its own code.

## 9. Error codes

Codes below 0x100 are reserved by this specification. Codes at or above 0x100
are free for application use and are what RST and STOP_SENDING normally carry.

| Code | Name | Meaning |
|---|---|---|
| 0 | NONE | no error; a normal close or drain |
| 1 | PROTOCOL | a rule in §8 was broken |
| 2 | INTERNAL | the sender failed for its own reasons |
| 3 | FLOW_CONTROL | credit was exceeded or an update was invalid |
| 4 | FRAME_SIZE | a frame or payload had an impossible size |
| 5 | REFUSED | the stream was not accepted; retriable |
| 6 | CANCELED | the stream was abandoned locally |
| 7 | CALM | a rate limit was exceeded; back off |
| 8 | VERSION | the offered protocol version is not supported |
| ≥0x100 | — | application-defined |

## 10. Reserved for future versions

The following are allocated but unused in version 1. An implementation must
not send them, and must treat their appearance as specified above.

- Frame type 7, PADDING, for traffic-shape obfuscation.
- Flag 0x8, for carrying a credit limit alongside DATA.
- SETTINGS identifier 5, FEATURE_BITS, for capability negotiation, including
  unidirectional streams and carrying one session over several connections.
- The final-size field of RST, which is present in version 1 but not yet used;
  it exists so that a session-level window can be added without a wire change.
