import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class Metrics {
    private final LongAdder checks = new LongAdder();
    private final LongAdder allowed = new LongAdder();
    private final LongAdder denied = new LongAdder();
    private final LongAdder capacityRejections = new LongAdder();
    private final LongAdder gossipReceived = new LongAdder();
    private final LongAdder gossipSent = new LongAdder();
    private final LongAdder gossipFailures = new LongAdder();
    private final LongAdder gossipSkipped = new LongAdder();
    private final LongAdder gossipBytesSent = new LongAdder();
    private final LongAdder gossipBytesReceived = new LongAdder();
    private final AtomicLong gossipKeysSent = new AtomicLong();
    private final AtomicLong evictionsLru = new AtomicLong();
    private final AtomicLong evictionsTtl = new AtomicLong();
    private final LongAdder mergedCells = new LongAdder();
    private final LongAdder ignoredCells = new LongAdder();

    public void recordCheck(boolean wasAllowed) {
        checks.increment();
        (wasAllowed ? allowed : denied).increment();
    }

    public void recordCapacityRejection() {
        capacityRejections.increment();
    }

    public void recordGossipReceived(long bytes, BucketedGCounter.MergeResult result) {
        gossipReceived.increment();
        gossipBytesReceived.add(bytes);
        mergedCells.add(result.changedCells());
        ignoredCells.add(result.ignoredExpired()
                + result.ignoredFuture()
                + result.ignoredCapacity());
    }

    public void recordGossipSent(long bytes) {
        gossipSent.increment();
        gossipBytesSent.add(bytes);
    }

    public void recordGossipKeysSent(long keys) {
        gossipKeysSent.addAndGet(keys);
    }

    public void recordEvictionLru(int count) {
        evictionsLru.addAndGet(count);
    }

    public void recordEvictionTtl(int count) {
        evictionsTtl.addAndGet(count);
    }

    public void recordGossipFailure() {
        gossipFailures.increment();
    }

    public void recordGossipSkipped() {
        gossipSkipped.increment();
    }

    public String prometheus(BucketedGCounter.StateSize size) {
        StringBuilder output = new StringBuilder();
        counter(output, "pulse_checks_total", "All rate-limit checks.", checks.sum());
        counter(output, "pulse_checks_allowed_total", "Allowed checks.", allowed.sum());
        counter(output, "pulse_checks_denied_total", "Denied checks.", denied.sum());
        counter(output, "pulse_capacity_rejections_total",
                "Checks rejected by the key safety ceiling.", capacityRejections.sum());
        counter(output, "pulse_gossip_received_total", "Accepted gossip messages.",
                gossipReceived.sum());
        counter(output, "pulse_gossip_sent_total", "Successful gossip messages.",
                gossipSent.sum());
        counter(output, "pulse_gossip_failures_total", "Failed gossip messages.",
                gossipFailures.sum());
        counter(output, "pulse_gossip_skipped_total", "Rounds skipped for an in-flight peer.",
                gossipSkipped.sum());
        counter(output, "pulse_gossip_bytes_sent_total", "Successful gossip payload bytes.",
                gossipBytesSent.sum());
        counter(output, "pulse_gossip_keys_sent_total", "Successful gossip payload keys.",
                gossipKeysSent.get());
        counter(output, "pulse_gossip_bytes_received_total", "Accepted gossip payload bytes.",
                gossipBytesReceived.sum());
        counter(output, "pulse_crdt_cells_merged_total", "CRDT cells changed by merge.",
                mergedCells.sum());
        counter(output, "pulse_crdt_cells_ignored_total", "Expired, future, or over-capacity cells.",
                ignoredCells.sum());
        counter(output, "pulse_evictions_lru_total", "Total keys evicted due to LRU memory pressure.",
                evictionsLru.get());
        counter(output, "pulse_evictions_ttl_total", "Total keys evicted due to TTL expiration.",
                evictionsTtl.get());
        gauge(output, "pulse_keys", "Distinct keys in local memory.", size.keys());
        gauge(output, "pulse_buckets", "Active key buckets in local memory.", size.buckets());
        gauge(output, "pulse_crdt_cells", "Active CRDT cells in local memory.", size.cells());
        return output.toString();
    }

    private static void counter(StringBuilder output, String name, String help, long value) {
        metric(output, name, help, "counter", value);
    }

    private static void gauge(StringBuilder output, String name, String help, long value) {
        metric(output, name, help, "gauge", value);
    }

    private static void metric(
            StringBuilder output, String name, String help, String type, long value) {
        output.append("# HELP ").append(name).append(' ').append(help).append('\n');
        output.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        output.append(name).append(' ').append(value).append('\n');
    }

    public long evictionsLru() {
        return evictionsLru.get();
    }

    public long evictionsTtl() {
        return evictionsTtl.get();
    }
}
