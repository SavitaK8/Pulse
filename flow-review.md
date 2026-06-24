# End-to-End Flow and Self-Review

## Request flow

1. `HttpApi` parses `GET /check?key=X` and rejects invalid keys.
2. `PulseService` asks `BucketedGCounter` to increment the current bucket.
3. Under one write lock, the counter:
   - expires buckets outside the active window,
   - increments only this node's `(key, bucket, nodeId)` slot,
   - sums all known active slots.
4. `PulseService` allows when the post-increment estimate is `<= LIMIT`.
5. `HttpApi` returns the decision immediately. No peer call occurs in this
   path.

## Gossip flow

1. `GossipManager` periodically snapshots all active cells.
2. `GossipCodec` serializes a versioned UTF-8 line payload with Base64 URL-safe
   identifiers.
3. The same immutable payload is posted asynchronously to each configured peer.
4. A peer bounds the body size, strictly parses the payload, and passes cells to
   `BucketedGCounter.merge`.
5. Merge takes the maximum for each cell. Expired, implausibly future, and
   over-capacity new-key cells are ignored.
6. Duplicate or reordered snapshots become no-ops; later rounds repair dropped
   messages.

## Expiry flow

Counters never decrement. When time advances, buckets older than
`currentBucket - WINDOW_BUCKETS + 1` are removed. Delayed gossip for those
buckets is ignored, so old traffic cannot re-enter the active estimate.

## Failure flow

If a peer is down, its request times out or fails and a metric increments. No
serving-health state changes, no leader election begins, and `/check` keeps
using local state. When connectivity returns, a full snapshot repairs missing
cells.

## Self-review findings

### What is coherent

- API and gossip both depend on the same counter abstraction, so there is one
  merge/admission state model.
- The local decision path is isolated from peer health.
- Wire parsing happens completely before merge, preventing partial mutation
  from malformed payloads.
- Body, key, and in-flight request bounds prevent obvious unbounded queues.
- Tests cover admission, expiry, concurrency, codec validation, HTTP behavior,
  convergence, and peer loss.

### Deliberate limits

- One global state lock is simple and correct but can cap multicore throughput.
  Benchmark before introducing lock striping by key.
- Full-state full-mesh gossip is `O(peers × active cells)` in bandwidth.
- Fixed buckets trade boundary precision for bounded, mergeable state.
- Static peers do not solve discovery, membership changes, or split-brain
  topology management.

### Production blockers

- Gossip has no authentication, authorization, or transport encryption.
- State is not durable.
- Reusing a node ID after restart can temporarily conflict with the monotonic
  G-Counter assumption. Use a fresh incarnation ID or restore durable slots.
- Clock skew changes bucket assignment and convergence behavior.
- `MAX_KEYS` is a safety ceiling, not tenant-aware eviction or admission
  fairness.
- Metrics have no key labels by design, avoiding unbounded monitoring
  cardinality; per-key diagnosis needs a separate bounded tool.

### Rejected false promise

Pulse cannot guarantee a strict cluster-wide limit while also making every
decision locally during partitions. The honest product claim is low-latency,
available, eventually convergent rate limiting with measurable
over-admission—not Redis-equivalent global strictness without Redis.

