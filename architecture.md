# Pulse Architecture

## Shape

Pulse is a small, standard-library-only Java application.

```text
HTTP /check ──> PulseService ──> BucketedGCounter
                      │                  │
                      │                  └── active key/bucket/node cells
                      │
HTTP /gossip ─> GossipCodec ─────────── merge(max)
                      ▲
                      │
              GossipManager ── HTTP POST ──> peers

HTTP /health, /metrics ──> local state and Metrics
```

## Modules

| File | Responsibility |
|---|---|
| `PulseNode.java` | Composition root, lifecycle, HTTP server wiring |
| `PulseConfig.java` | Environment parsing and validation |
| `PulseService.java` | Rate-limit use case and key validation |
| `BucketedGCounter.java` | Thread-safe bucketed CRDT state |
| `GossipCodec.java` | Versioned line serialization and strict parsing |
| `GossipManager.java` | Periodic best-effort peer fan-out |
| `HttpApi.java` | Routes, request bounds, JSON/text responses |
| `Metrics.java` | Lock-free operational counters |
| `Json.java` | Minimal safe JSON escaping |
| `PulseTests.java` | Deterministic unit and local integration tests |

No production source file should grow beyond roughly 500 lines. The modules
communicate through small records rather than sharing HTTP or parser concerns.

The read-only `/estimate` route exists for convergence measurement and does not
participate in admission.

## State model

```text
key
└── epoch bucket
    └── node ID -> monotonically increasing count
```

`BucketedGCounter.incrementAndEstimate` performs the local increment and active
window sum under one write lock. Snapshots and merges use the same lock so no
cell is observed half-updated.

Old buckets are removed when they fall before:

```text
currentBucket - WINDOW_BUCKETS + 1
```

Removing an expired bucket is safe for this time-windowed application because
it can no longer influence a decision. Gossip parser/merge logic ignores
already-expired buckets so stale messages cannot resurrect them.

## Concurrency model

- `HttpServer` uses a bounded worker pool.
- Counter mutations are protected by one read/write lock. This favors
  correctness and simple review; sharding the lock is a measured future
  optimization.
- Gossip scheduling and HTTP callbacks use separate executors, so a slow peer
  does not block `/check`.
- Metrics use `LongAdder`.

## Failure model

- Peer timeout/refusal: recorded as a failed gossip; local serving continues.
- Duplicate/reordered gossip: harmless due to element-wise max.
- Malformed/oversized gossip: rejected without mutating state.
- Clock skew: fixed buckets require reasonably synchronized clocks. State more
  than one bucket in the future is ignored, limiting accidental memory growth.
- Restart: local in-memory contribution is lost. A restarted node should use a
  new incarnation ID in production (for example `node-a-<boot-id>`) to avoid
  reusing an old G-Counter slot with a lower value.

## Security boundary

The sample cluster assumes a trusted private network. Production deployment
must authenticate gossip (mTLS or signed messages), restrict `/gossip` at the
network layer, and apply ingress limits before parsing bodies.
