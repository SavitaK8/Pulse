# Benchmarking Pulse

Use a fresh key for every run. The benchmark script does this automatically
unless `--key` is supplied.

## 1. Over-admission versus gossip interval

For each interval, restart a clean cluster and run:

```bash
GOSSIP_INTERVAL_MS=20 docker compose up -d --build
python scripts/benchmark.py convergence --requests 300 --concurrency 100 --limit 50
docker compose down
```

Repeat for `20, 50, 100, 200, 500, 1000` milliseconds with at least 10 runs per
point. Record:

- `over_admission = allowed - limit`,
- `convergence_ms`,
- p50/p99 request latency,
- failures.

Plot gossip interval on the x-axis and median/p95 over-admission on the y-axis.
The expected shape is increasing over-admission as the interval grows, though
scheduler timing and load distribution add variance.

## 2. Gossip bandwidth versus key cardinality

Start a fresh cluster for each cardinality:

```bash
python scripts/benchmark.py bandwidth --keys 1000 --settle-seconds 2
```

Repeat for `10, 100, 1_000, 10_000` keys. Plot `average_payload_bytes` against
key count. Full-state gossip should grow approximately linearly with active
CRDT cells.

The metric is calculated from deltas of:

- `pulse_gossip_bytes_sent_total`,
- `pulse_gossip_sent_total`.

Avoid unrelated traffic during this experiment.

## 3. Partition tolerance

1. Start all three nodes.
2. Generate a baseline load with the `load` command.
3. Stop `pulse-node-b`.
4. Continue load against nodes A and C.
5. Confirm both keep returning HTTP 200 and local decisions.
6. Restart B and poll `/estimate` until all active estimates match.

Measure availability and convergence time after healing. Precision is expected
to degrade during the partition; availability is expected to remain.

## 4. Throughput and latency

```bash
python scripts/benchmark.py load --requests 100000 --concurrency 200
```

The Python client is useful for regression checks but can become the
bottleneck. For credible peak-throughput numbers, use `wrk`, `hey`, or `k6`
from a separate machine and report:

- client and server hardware,
- JDK and OS,
- node count,
- key distribution,
- concurrency and duration,
- p50/p95/p99 latency,
- achieved requests per second,
- error rate,
- GC and CPU utilization.

Do not claim microsecond network latency from the Python harness. The
architecture removes peer/network waits from `/check`, but the HTTP stack,
client, host, and lock contention still determine measured latency.

## Result hygiene

- Warm the JVM before recording.
- Use unique keys or restart between experiments.
- Keep `LIMIT`, window settings, and node count explicit.
- Run enough repetitions to report distributions, not one lucky result.
- Preserve raw JSON output alongside charts.
- Treat over-admission as a correctness characteristic, not merely a latency
  metric.

