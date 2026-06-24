# Design Decisions

## ADR-001: Bucketed G-Counter

**Decision:** Store a G-Counter per key and fixed epoch bucket.

**Why:** Each node changes only its own slot, and merge-by-maximum cannot be
corrupted by duplicate or reordered delivery. Expiry avoids distributed
decrement.

**Trade-off:** Accuracy is bounded by bucket granularity and gossip delay.

## ADR-002: Local post-increment decisions

**Decision:** `/check` increments first and allows when the resulting estimate
is `<= LIMIT`.

**Why:** The admitted request must be represented in the decision. Counting
denied attempts also prevents a hot client from repeatedly probing an unchanged
count within a bucket.

**Trade-off:** The reported count can grow beyond the limit.

## ADR-003: Full-state gossip

**Decision:** Send every active CRDT cell to every configured peer each round.

**Why:** It is easy to reason about and demonstrates convergence without a
membership or anti-entropy subsystem.

**Trade-off:** Cost is `O(peers × active cells)`. The benchmark records payload
size so key sharding or delta gossip can be justified with data.

## ADR-004: Versioned line protocol

**Decision:** Use a strict UTF-8 line format with Base64 URL-safe identifiers.

**Why:** The project compiles with `javac *.java`, has no dependency manager,
and remains inspectable. Base64 avoids delimiter ambiguity.

**Trade-off:** Protobuf would be smaller and provide generated schemas.

## ADR-005: Availability over strictness

**Decision:** Never require quorum, leader election, or peer contact on the
request path.

**Why:** Low latency and partition tolerance are the point of Pulse.

**Trade-off:** A partition or gossip delay can cause distributed over-admission.

