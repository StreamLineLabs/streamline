# ADR-0023: Edge Transport Protocol Selection

## Status

Proposed (M3 P0 deliverable)

## Context

M3 needs a wire transport that works from browsers and edge devices to
Streamline brokers. Kafka's native TCP protocol does not work from
browsers. Candidates:

| Transport     | Browsers           | Edge OK | Backpressure | Notes                |
|---------------|--------------------|---------|--------------|----------------------|
| WebSocket     | All modern         | Yes     | Manual       | Battle-tested        |
| WebTransport  | Chrome/Edge/FF*    | Yes     | Native       | QUIC-based; new      |
| HTTP/3 SSE    | Most               | Yes     | Server→cli   | One-way only         |
| Raw QUIC      | None (no JS API)   | Yes     | Native       | Native edge only     |

(* Firefox stable supports WebTransport behind nightly toggle; Safari is
not yet GA — see <https://caniuse.com/webtransport>.)

## Decision

- **Default browser transport:** WebSocket over TLS, framing the existing
  Kafka-compatible record batches in a binary subprotocol
  (`streamline.v1.binary`).
- **Optional / opportunistic:** WebTransport when the browser advertises
  support; use for high-throughput consume paths (1+ MB/s sustained) where
  HoL blocking matters. Negotiate during the WS handshake via an
  `X-Streamline-Upgrade: webtransport` hint.
- **Native edge devices** (no browser): raw QUIC via `quinn` with the same
  binary subprotocol. Falls back to WebSocket if a corporate proxy blocks
  UDP.
- **Auth:** short-lived JWT issued by `streamline join --token=...`,
  carried in the WebSocket subprotocol header. JWT signed by broker key
  managed via KMS plugin (ADR-0018).

## Consequences

### Positive
- Single binary subprotocol works across WebSocket / WebTransport / QUIC.
- WebSocket default means we ship to 100% of supported browsers on day one.
- WebTransport upgrade path delivers the wow for power users without
  blocking GA.

### Negative
- Two transports to test in CI (WS + QUIC); WebTransport tracked as Phase-3
  validation effort.
- JWT lifetime / rotation is a new auth flow distinct from server-side
  SASL; documented under M3 P3.

### Neutral
- HTTP/3 SSE was rejected because we need bidirectional (produce + consume).

## References

- WebSocket RFC 6455
- WebTransport: <https://www.w3.org/TR/webtransport/>
- `quinn`: <https://github.com/quinn-rs/quinn>
- caniuse webtransport: <https://caniuse.com/webtransport>
- Internal: `MOONSHOT_PLAN.md` Feature M3
