# Pulse Specification

## Goal

Pulse is a coordinator-free distributed rate limiter. A request decision uses
only local memory; nodes exchange state asynchronously and converge through a
bucketed grow-only counter (G-Counter) CRDT.

## Functional requirements

- Run as a single dependency-free Java process.
- Support any number of statically configured peers.
- Make `/check` decisions without a network call.
- Merge duplicated, delayed, and reordered gossip safely.
- Keep serving when any peer is unavailable.
- Expire old fixed-time buckets without decrementing CRDT counters.
- Expose enough response metadata to observe convergence.

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| `NODE_ID` | `node-a` | Stable unique identifier for this node |
| `PORT` | `9001` | HTTP listen port |
| `PEERS` | empty | Comma-separated `host:port` peers |
| `LIMIT` | `50` | Maximum observed requests in the active window |
| `WINDOW_BUCKETS` | `10` | Number of buckets in the active window |
| `BUCKET_MS` | `1000` | Bucket duration in milliseconds |
| `GOSSIP_INTERVAL_MS` | `200` | Full-state gossip interval |
| `GOSSIP_TIMEOUT_MS` | `1000` | Per-peer HTTP timeout |
| `MAX_KEYS` | `100000` | Local memory safety ceiling |
| `MAX_GOSSIP_BYTES` | `4194304` | Maximum accepted gossip body |

Invalid configuration is a startup error.

## API contract

### `GET /check?key=<key>`

Atomically increments this node's slot for the key's current bucket, then sums
all known slots in the active window.

Status:

- `200` for a valid request, whether allowed or denied.
- `400` when `key` is missing, empty, or longer than 512 UTF-8 bytes.
- `503` when a new key cannot be admitted because `MAX_KEYS` is reached.

Example response:

```json
{
  "allowed": true,
  "key": "ip:1.2.3.4",
  "estimatedCount": 18,
  "limit": 50,
  "remaining": 32,
  "nodeId": "node-a",
  "bucket": 1710000000
}
```

`allowed` is true when the post-increment estimate is less than or equal to
`LIMIT`. Denied attempts are counted so sustained traffic remains denied until
the active window advances.

### `POST /gossip`

Accepts Pulse's versioned line wire format and CRDT-merges it into local state.
It is intended for configured peers, not public clients.

Status:

- `204` after a successful merge.
- `400` for malformed state.
- `405` for methods other than `POST`.
- `413` when the request exceeds `MAX_GOSSIP_BYTES`.

### `GET /estimate?key=<key>`

Returns the current local estimate without incrementing it. This endpoint is
intended for convergence experiments and operational inspection.

Status and key validation match `/check`.

### `GET /health`

Returns process and configuration status as JSON. Status is `200` whenever the
node can serve local decisions; peer failures do not make the node unhealthy.

### `GET /metrics`

Returns dependency-free Prometheus text metrics for request decisions, gossip,
payload bytes, merge activity, key count, and active CRDT cells.

## Distributed semantics

For each `(key, bucket, nodeId)` tuple, the stored value only increases.
Merging takes the maximum value for each tuple. Therefore merge is:

- commutative,
- associative,
- idempotent.

Nodes may temporarily disagree. During the delay before gossip arrives, more
than `LIMIT` requests can be admitted across the cluster. Pulse favors local
availability and low latency over strict global admission.

## Non-goals

- Strict linearizable global limits.
- Byzantine or unauthenticated Internet-facing gossip.
- Dynamic membership or consensus.
- Durable counters across process restarts.
- Exact sliding-window logs.
