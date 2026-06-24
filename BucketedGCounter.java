import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public final class BucketedGCounter {
    private final String nodeId;
    private final int windowBuckets;
    private final long bucketMs;
    private final int maxKeys;
    private final Clock clock;
    private final ConcurrentHashMap<String, KeyState> state = new ConcurrentHashMap<>();
    private final ReentrantLock[] stripes;
    private final int evictionSampleSize;
    private final Metrics metrics;
    
    private volatile Iterator<Map.Entry<String, KeyState>> clockIterator = null;
    private final ReentrantLock clockLock = new ReentrantLock();
    private volatile Iterator<Map.Entry<String, KeyState>> sweepIterator = null;

    private static final class KeyState {
        final NavigableMap<Long, Map<String, Long>> buckets = new TreeMap<>();
        long lastModified;
        long lastAccessed;
    }

    public BucketedGCounter(
            String nodeId, int windowBuckets, long bucketMs, int maxKeys, Clock clock, int lockStripes, int evictionSampleSize, Metrics metrics) {
        this.nodeId = nodeId;
        this.windowBuckets = windowBuckets;
        this.bucketMs = bucketMs;
        this.maxKeys = maxKeys;
        this.clock = clock;
        this.evictionSampleSize = evictionSampleSize;
        this.metrics = metrics;
        this.stripes = new ReentrantLock[lockStripes];
        for (int i = 0; i < lockStripes; i++) {
            this.stripes[i] = new ReentrantLock(false);
        }
    }

    private ReentrantLock lockFor(String key) {
        return stripes[(key.hashCode() & 0x7FFFFFFF) % stripes.length];
    }

    private boolean evictBatch(long currentSize, long targetSize) {
        long toEvict = currentSize - targetSize;
        if (toEvict <= 0) return true;
        
        int evicted = 0;
        int budget = (int) (toEvict * 5);
        
        while (evicted < toEvict && budget-- > 0) {
            Map.Entry<String, KeyState> victim = null;
            clockLock.lock();
            try {
                for (int i = 0; i < evictionSampleSize; i++) {
                    if (clockIterator == null || !clockIterator.hasNext()) {
                        clockIterator = state.entrySet().iterator();
                        if (!clockIterator.hasNext()) break;
                    }
                    Map.Entry<String, KeyState> entry = clockIterator.next();
                    if (victim == null || entry.getValue().lastAccessed < victim.getValue().lastAccessed) {
                        victim = entry;
                    }
                }
            } finally {
                clockLock.unlock();
            }
            
            if (victim != null) {
                ReentrantLock lock = lockFor(victim.getKey());
                if (lock.tryLock()) {
                    try {
                        KeyState current = state.get(victim.getKey());
                        if (current == victim.getValue()) {
                            state.remove(victim.getKey());
                            evicted++;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } else {
                break;
            }
        }
        
        if (evicted > 0) {
            metrics.recordEvictionLru(evicted);
        }
        return evicted >= toEvict;
    }

    public void sweep() {
        if (state.mappingCount() < maxKeys * 0.7) {
            return;
        }
        int limit = 1000;
        int ttlEvicted = 0;
        long now = clock.millis();
        long currentBucket = Math.floorDiv(now, bucketMs);
        
        while (limit-- > 0) {
            if (sweepIterator == null || !sweepIterator.hasNext()) {
                sweepIterator = state.entrySet().iterator();
                if (!sweepIterator.hasNext()) break;
            }
            Map.Entry<String, KeyState> entry = sweepIterator.next();
            String key = entry.getKey();
            KeyState keyState = entry.getValue();
            
            if ((now - keyState.lastModified) >= (windowBuckets * bucketMs)) {
                ReentrantLock lock = lockFor(key);
                if (lock.tryLock()) {
                    try {
                        if (state.get(key) == keyState) {
                            long oldest = oldestActiveBucket(currentBucket);
                            keyState.buckets.headMap(oldest, false).clear();
                            if (keyState.buckets.isEmpty()) {
                                state.remove(key);
                                ttlEvicted++;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }
        if (ttlEvicted > 0) {
            metrics.recordEvictionTtl(ttlEvicted);
        }
    }

    public IncrementResult incrementAndEstimate(String key) {
        long currentBucket = currentBucket();
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            KeyState keyState = state.get(key);
            if (keyState == null) {
                long currentSize = state.mappingCount();
                if (currentSize >= maxKeys) {
                    if (!evictBatch(currentSize, (long) (maxKeys * 0.95))) {
                        throw new CapacityExceededException();
                    }
                }
                if (state.mappingCount() >= maxKeys) {
                    throw new CapacityExceededException();
                }
                keyState = new KeyState();
                state.put(key, keyState);
            }
            long oldest = oldestActiveBucket(currentBucket);
            keyState.buckets.headMap(oldest, false).clear();
            
            Map<String, Long> slots = keyState.buckets.computeIfAbsent(currentBucket, ignored -> new HashMap<>());
            long next = Math.addExact(slots.getOrDefault(nodeId, 0L), 1L);
            slots.put(nodeId, next);
            
            long now = clock.millis();
            keyState.lastModified = now;
            keyState.lastAccessed = now;
            
            return new IncrementResult(currentBucket, sumWindowLocked(keyState.buckets, currentBucket));
        } finally {
            lock.unlock();
        }
    }

    public long estimate(String key) {
        long currentBucket = currentBucket();
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            KeyState keyState = state.get(key);
            if (keyState == null) {
                return 0;
            }
            long oldest = oldestActiveBucket(currentBucket);
            keyState.buckets.headMap(oldest, false).clear();
            if (keyState.buckets.isEmpty()) {
                state.remove(key, keyState);
                return 0;
            }
            keyState.lastAccessed = clock.millis();
            return sumWindowLocked(keyState.buckets, currentBucket);
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot() {
        return snapshot(0L);
    }

    public Snapshot snapshot(long sinceTimestamp) {
        long now = clock.millis();
        long currentBucket = Math.floorDiv(now, bucketMs);
        long oldest = oldestActiveBucket(currentBucket);
        List<Cell> cells = new ArrayList<>();
        
        for (String key : state.keySet()) {
            ReentrantLock lock = lockFor(key);
            lock.lock();
            try {
                KeyState keyState = state.get(key);
                if (keyState == null || keyState.lastModified < sinceTimestamp) {
                    continue;
                }
                keyState.buckets.headMap(oldest, false).clear();
                if (keyState.buckets.isEmpty()) {
                    state.remove(key, keyState);
                    continue;
                }
                for (Map.Entry<Long, Map<String, Long>> bucketEntry : keyState.buckets.entrySet()) {
                    long bucket = bucketEntry.getKey();
                    for (Map.Entry<String, Long> slotEntry : bucketEntry.getValue().entrySet()) {
                        cells.add(new Cell(key, bucket, slotEntry.getKey(), slotEntry.getValue()));
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return new Snapshot(nodeId, now, List.copyOf(cells));
    }

    public MergeResult merge(List<Cell> cells) {
        long currentBucket = currentBucket();
        long oldest = oldestActiveBucket(currentBucket);
        long changed = 0;
        long ignoredExpired = 0;
        long ignoredFuture = 0;
        long ignoredCapacity = 0;
        
        Map<Integer, Map<String, List<Cell>>> grouped = cells.stream()
                .collect(Collectors.groupingBy(
                        c -> (c.key().hashCode() & 0x7FFFFFFF) % stripes.length,
                        Collectors.groupingBy(Cell::key)));

        for (Map.Entry<Integer, Map<String, List<Cell>>> stripeEntry : grouped.entrySet()) {
            ReentrantLock lock = stripes[stripeEntry.getKey()];
            lock.lock();
            try {
                for (Map.Entry<String, List<Cell>> keyEntry : stripeEntry.getValue().entrySet()) {
                    String key = keyEntry.getKey();
                    KeyState keyState = state.get(key);
                    for (Cell cell : keyEntry.getValue()) {
                        if (cell.bucket() < oldest) {
                            ignoredExpired++;
                            continue;
                        }
                        if (cell.bucket() > currentBucket + 1) {
                            ignoredFuture++;
                            continue;
                        }
                        if (keyState == null) {
                            long currentSize = state.mappingCount();
                            if (currentSize >= maxKeys) {
                                if (!evictBatch(currentSize, (long) (maxKeys * 0.95))) {
                                    ignoredCapacity++;
                                    continue;
                                }
                            }
                            if (state.mappingCount() >= maxKeys) {
                                ignoredCapacity++;
                                continue;
                            }
                            keyState = new KeyState();
                            state.put(key, keyState);
                        }
                        Map<String, Long> slots = keyState.buckets.computeIfAbsent(cell.bucket(), ignored -> new HashMap<>());
                        long previous = slots.getOrDefault(cell.nodeId(), 0L);
                        if (cell.count() > previous) {
                            slots.put(cell.nodeId(), cell.count());
                            changed++;
                            long now = clock.millis();
                            keyState.lastModified = now;
                            keyState.lastAccessed = now;
                        }
                    }
                    if (keyState != null) {
                        keyState.buckets.headMap(oldest, false).clear();
                        if (keyState.buckets.isEmpty()) {
                            state.remove(key, keyState);
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return new MergeResult(changed, ignoredExpired, ignoredFuture, ignoredCapacity);
    }

    public StateSize size() {
        long buckets = 0;
        long cells = 0;
        for (KeyState keyState : state.values()) {
            buckets += keyState.buckets.size();
            for (Map<String, Long> slots : keyState.buckets.values()) {
                cells += slots.size();
            }
        }
        return new StateSize(state.mappingCount(), buckets, cells);
    }

    private long sumWindowLocked(
            NavigableMap<Long, Map<String, Long>> buckets, long currentBucket) {
        long sum = 0;
        for (Map<String, Long> slots : buckets
                .subMap(oldestActiveBucket(currentBucket), true, currentBucket, true)
                .values()) {
            for (long count : slots.values()) {
                sum = saturatedAdd(sum, count);
            }
        }
        return sum;
    }

    private long currentBucket() {
        return Math.floorDiv(clock.millis(), bucketMs);
    }

    private long oldestActiveBucket(long currentBucket) {
        return currentBucket - windowBuckets + 1L;
    }

    private static long saturatedAdd(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public record Cell(String key, long bucket, String nodeId, long count) {
        public Cell {
            if (key == null || key.isEmpty() || nodeId == null || nodeId.isEmpty() || count <= 0) {
                throw new IllegalArgumentException("Invalid CRDT cell");
            }
        }
    }

    public record Snapshot(String sourceNodeId, long sentAtMillis, List<Cell> cells) {
        public Snapshot {
            cells = List.copyOf(cells);
        }
    }

    public record IncrementResult(long bucket, long estimatedCount) {}

    public record MergeResult(
            long changedCells, long ignoredExpired, long ignoredFuture, long ignoredCapacity) {}

    public record StateSize(long keys, long buckets, long cells) {}

    public static final class CapacityExceededException extends RuntimeException {
        public CapacityExceededException() {
            super("maximum distinct key count reached");
        }
    }
}
