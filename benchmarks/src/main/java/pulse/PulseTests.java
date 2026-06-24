package pulse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PulseTests {
    private static int passed;

    public static void main(String[] args) throws Exception {
        run("configuration parsing", PulseTests::configurationParsing);
        run("CRDT convergence and idempotence", PulseTests::crdtConvergence);
        run("concurrent local increments", PulseTests::concurrentIncrements);
        run("bucket expiry and stale merge", PulseTests::bucketExpiry);
        run("local admission semantics", PulseTests::localAdmission);
        run("gossip codec round trip", PulseTests::codecRoundTrip);
        run("gossip codec rejects malformed input", PulseTests::codecRejectsMalformed);
        run("HTTP API integration", PulseTests::httpIntegration);
        run("delta gossip efficiency", PulseTests::deltaGossipEfficiency);
        run("HTTP security validation", PulseTests::httpSecurity);
        run("approximate lru eviction", PulseTests::lruEviction);
        System.out.println("All " + passed + " tests passed.");
    }

    private static void configurationParsing() {
        PulseConfig config = PulseConfig.from(Map.of(
                "NODE_ID", "node-z",
                "PORT", "9100",
                "PEERS", "localhost:9001,https://example.test:9443",
                "LIMIT", "12",
                "CLUSTER_SECRET", "0123456789abcdef0123456789abcdef"));
        equal("node-z", config.nodeId());
        equal(9100, config.port());
        equal(2, config.peers().size());
        equal("http://localhost:9001/gossip", config.peers().get(0).toString());
        equal(12L, config.limit());
        expectThrows(IllegalArgumentException.class,
                () -> PulseConfig.from(Map.of("LIMIT", "0")));
    }

    private static void crdtConvergence() {
        MutableClock clock = new MutableClock(100_000);
        BucketedGCounter nodeA = counter("node-a", 10, 1_000, clock);
        BucketedGCounter nodeB = counter("node-b", 10, 1_000, clock);
        for (int index = 0; index < 3; index++) {
            nodeA.incrementAndEstimate("client");
        }
        for (int index = 0; index < 2; index++) {
            nodeB.incrementAndEstimate("client");
        }

        nodeA.merge(nodeB.snapshot().cells());
        nodeB.merge(nodeA.snapshot().cells());
        equal(5L, nodeA.estimate("client"));
        equal(5L, nodeB.estimate("client"));

        BucketedGCounter.MergeResult duplicate = nodeA.merge(nodeB.snapshot().cells());
        equal(0L, duplicate.changedCells());
        equal(5L, nodeA.estimate("client"));

        List<BucketedGCounter.Cell> lower = List.of(
                new BucketedGCounter.Cell("client", 100, "node-b", 1));
        nodeA.merge(lower);
        equal(5L, nodeA.estimate("client"));
    }

    private static void bucketExpiry() {
        MutableClock clock = new MutableClock(0);
        BucketedGCounter counter = counter("node-a", 2, 1_000, clock);
        counter.incrementAndEstimate("client");
        equal(1L, counter.estimate("client"));

        clock.setMillis(2_000);
        equal(0L, counter.estimate("client"));
        BucketedGCounter.MergeResult result = counter.merge(List.of(
                new BucketedGCounter.Cell("client", 0, "node-b", 10)));
        equal(1L, result.ignoredExpired());
        equal(0L, counter.estimate("client"));
    }

    private static void concurrentIncrements() throws Exception {
        MutableClock clock = new MutableClock(100_000);
        BucketedGCounter counter = counter("node-a", 10, 1_000, clock);
        int workers = 12;
        int incrementsPerWorker = 1_000;
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);
        try {
            for (int worker = 0; worker < workers; worker++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int index = 0; index < incrementsPerWorker; index++) {
                            counter.incrementAndEstimate("client");
                        }
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            start.countDown();
            done.await();
            equal((long) workers * incrementsPerWorker, counter.estimate("client"));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void localAdmission() {
        MutableClock clock = new MutableClock(5_000);
        PulseConfig config = testConfig("node-a", 0, 2);
        BucketedGCounter counter = counter("node-a", 10, 1_000, clock);
        PulseService service = new PulseService(config, counter, new Metrics());
        truth(service.check("client").allowed());
        truth(service.check("client").allowed());
        truth(!service.check("client").allowed());
        equal(3L, service.estimate("client").estimatedCount());
        expectThrows(PulseService.InvalidKeyException.class, () -> service.check(" "));
    }

    private static void deltaGossipEfficiency() {
        MutableClock clock = new MutableClock(100_000);
        BucketedGCounter counter = counter("node-a", 10, 1_000, clock);
        
        counter.incrementAndEstimate("key1");
        counter.incrementAndEstimate("key2");
        counter.incrementAndEstimate("key3");
        
        long syncTime = clock.millis() + 1;
        
        BucketedGCounter.Snapshot fullSnapshot = counter.snapshot(0L);
        equal(3, fullSnapshot.cells().size());
        
        clock.setMillis(101_000);
        
        BucketedGCounter.Snapshot emptyDelta = counter.snapshot(syncTime);
        equal(0, emptyDelta.cells().size());
        
        counter.incrementAndEstimate("key2");
        
        BucketedGCounter.Snapshot partialDelta = counter.snapshot(syncTime);
        long distinctKeys = partialDelta.cells().stream().map(BucketedGCounter.Cell::key).distinct().count();
        equal(1L, distinctKeys);
        equal("key2", partialDelta.cells().get(0).key());
    }

    private static void codecRoundTrip() {
        GossipCodec codec = new GossipCodec();
        BucketedGCounter.Snapshot snapshot = new BucketedGCounter.Snapshot(
                "nødé-a",
                1234,
                List.of(new BucketedGCounter.Cell("客户\tone", 9, "nødé-a", 7)));
        GossipCodec.Envelope decoded = codec.decode(codec.encode(snapshot));
        equal(snapshot.sourceNodeId(), decoded.sourceNodeId());
        equal(snapshot.cells(), decoded.cells());
    }

    private static void codecRejectsMalformed() {
        GossipCodec codec = new GossipCodec();
        expectThrows(GossipCodec.InvalidGossipException.class,
                () -> codec.decode("not-pulse".getBytes(StandardCharsets.UTF_8)));
    }

    private static void httpIntegration() throws Exception {
        PulseConfig config = testConfig("node-http", 0, 3);
        Metrics metrics = new Metrics();
        BucketedGCounter counter = new BucketedGCounter(
                config.nodeId(), 10, 60_000, 1_000, Clock.systemUTC(), 256, 10, metrics);
        GossipCodec codec = new GossipCodec();
        PulseService service = new PulseService(config, counter, metrics);
        HttpApi api = new HttpApi(config, service, counter, codec, metrics);
        api.start();
        try {
            HttpClient client = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + api.port();
            HttpResponse<String> check = get(client, base + "/check?key=client");
            equal(200, check.statusCode());
            contains(check.body(), "\"allowed\":true");
            contains(check.body(), "\"estimatedCount\":1");

            HttpResponse<String> missing = get(client, base + "/check");
            equal(400, missing.statusCode());

            long bucket = Math.floorDiv(System.currentTimeMillis(), 60_000);
            byte[] gossip = codec.encode(new BucketedGCounter.Snapshot(
                    "node-peer",
                    System.currentTimeMillis(),
                    List.of(new BucketedGCounter.Cell("client", bucket, "node-peer", 4))));
            HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/gossip"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(gossip))
                    .build();
            HttpResponse<String> merged = client.send(
                    request, HttpResponse.BodyHandlers.ofString());
            equal(204, merged.statusCode());

            HttpResponse<String> estimate = get(client, base + "/estimate?key=client");
            equal(200, estimate.statusCode());
            contains(estimate.body(), "\"estimatedCount\":5");
            contains(get(client, base + "/health").body(), "\"status\":\"ok\"");
            contains(get(client, base + "/metrics").body(), "pulse_gossip_received_total 1");
        } finally {
            api.close();
        }
    }

    private static HttpResponse<String> get(HttpClient client, String uri) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create(uri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static void httpSecurity() throws Exception {
        PulseConfig config = new PulseConfig(
                "node-sec", 0, List.of(), 50, 10, 1_000, 200, 1_000, 1_000, 1_048_576,
                "0123456789abcdef0123456789abcdef", 256, 10);
        Metrics metrics = new Metrics();
        BucketedGCounter counter = new BucketedGCounter(config.nodeId(), 10, 60_000, 1_000, Clock.systemUTC(), 256, 10, metrics);
        GossipCodec codec = new GossipCodec();
        PulseService service = new PulseService(config, counter, metrics);
        HttpApi api = new HttpApi(config, service, counter, codec, metrics);
        api.start();
        try {
            HttpClient client = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + api.port() + "/gossip";
            byte[] payload = codec.encode(new BucketedGCounter.Snapshot("node-peer", 0, List.of(new BucketedGCounter.Cell("client", 1, "node-peer", 1))));
            
            java.util.function.BiFunction<String, String, String> sign = (timestamp, msgId) -> {
                try {
                    javax.crypto.Mac m = javax.crypto.Mac.getInstance("HmacSHA256");
                    m.init(new javax.crypto.spec.SecretKeySpec(config.clusterSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                    m.update("POST\n/gossip\n".getBytes(StandardCharsets.UTF_8));
                    m.update(timestamp.getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    m.update(msgId.getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(m.doFinal(payload));
                } catch (Exception e) { throw new RuntimeException(e); }
            };

            // 1. Missing Signature
            HttpRequest req1 = HttpRequest.newBuilder(URI.create(base)).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(401, client.send(req1, HttpResponse.BodyHandlers.ofString()).statusCode());

            long now = System.currentTimeMillis();
            String ts = String.valueOf(now);
            String id = java.util.UUID.randomUUID().toString();
            String validSig = sign.apply(ts, id);

            // 2. Invalid Signature
            HttpRequest req2 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", ts).header("X-Pulse-Message-Id", id).header("X-Pulse-Signature", "invalid").POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(401, client.send(req2, HttpResponse.BodyHandlers.ofString()).statusCode());

            // 3. Modified Payload
            HttpRequest req3 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", ts).header("X-Pulse-Message-Id", id).header("X-Pulse-Signature", validSig).POST(HttpRequest.BodyPublishers.ofByteArray(new byte[]{1,2,3})).build();
            equal(401, client.send(req3, HttpResponse.BodyHandlers.ofString()).statusCode());

            // 4. Expired Timestamp
            String expiredTs = String.valueOf(now - 40_000);
            HttpRequest req4 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", expiredTs).header("X-Pulse-Message-Id", java.util.UUID.randomUUID().toString()).header("X-Pulse-Signature", sign.apply(expiredTs, id)).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(401, client.send(req4, HttpResponse.BodyHandlers.ofString()).statusCode());

            // 5. Accepts Valid
            HttpRequest req5 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", ts).header("X-Pulse-Message-Id", id).header("X-Pulse-Signature", validSig).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(204, client.send(req5, HttpResponse.BodyHandlers.ofString()).statusCode());

            // 6. Replay
            HttpRequest req6 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", ts).header("X-Pulse-Message-Id", id).header("X-Pulse-Signature", validSig).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(409, client.send(req6, HttpResponse.BodyHandlers.ofString()).statusCode());

            // 7. Wrong Secret
            java.util.function.BiFunction<String, String, String> signWrong = (timestamp, msgId) -> {
                try {
                    javax.crypto.Mac m = javax.crypto.Mac.getInstance("HmacSHA256");
                    m.init(new javax.crypto.spec.SecretKeySpec("wrongsecretwrongsecretwrongsecre".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                    m.update("POST\n/gossip\n".getBytes(StandardCharsets.UTF_8));
                    m.update(timestamp.getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    m.update(msgId.getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(m.doFinal(payload));
                } catch (Exception e) { throw new RuntimeException(e); }
            };
            String newId = java.util.UUID.randomUUID().toString();
            HttpRequest req7 = HttpRequest.newBuilder(URI.create(base)).header("X-Pulse-Timestamp", ts).header("X-Pulse-Message-Id", newId).header("X-Pulse-Signature", signWrong.apply(ts, newId)).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            equal(401, client.send(req7, HttpResponse.BodyHandlers.ofString()).statusCode());
            
        } finally {
            api.close();
        }
    }

    private static void lruEviction() throws Exception {
        PulseConfig config = new PulseConfig(
                "node-evict", 0, List.of(), 50, 10, 1_000, 200, 1_000, 100, 1_048_576, "", 256, 10);
        Metrics metrics = new Metrics();
        BucketedGCounter counter = new BucketedGCounter(
                config.nodeId(), 10, 60_000, 100, Clock.systemUTC(), 256, 10, metrics);

        // Fill up to maxKeys
        for (int i = 0; i < 100; i++) {
            counter.incrementAndEstimate("key-" + i);
        }
        
        long keysBefore = counter.size().keys();
        if (keysBefore != 100) {
            throw new IllegalStateException("Expected 100 keys, got " + keysBefore);
        }

        // Add 10 more keys. This should trigger eviction down to 95% (95 keys) and then add the new key.
        for (int i = 100; i < 110; i++) {
            counter.incrementAndEstimate("key-" + i);
        }

        long keysAfter = counter.size().keys();
        // The exact number of keys might vary slightly because eviction is a batch down to 95%
        // But it should definitely not exceed 100, and it should not throw CapacityExceededException
        if (keysAfter > 100) {
            throw new IllegalStateException("Eviction failed to keep size <= 100, got " + keysAfter);
        }
        if (metrics.evictionsLru() == 0) {
            throw new IllegalStateException("Expected LRU evictions to be recorded");
        }
    }

    private static PulseConfig testConfig(String nodeId, int port, long limit) {
        return new PulseConfig(
                nodeId, port, List.of(), limit, 10, 1_000, 200, 1_000, 1_000, 1_048_576, "", 256, 10);
    }

    private static BucketedGCounter counter(
            String nodeId, int windows, long bucketMs, Clock clock) {
        return new BucketedGCounter(nodeId, windows, bucketMs, 1_000, clock, 256, 10, new Metrics());
    }

    private static void run(String name, CheckedRunnable test) throws Exception {
        try {
            test.run();
            passed++;
            System.out.println("PASS " + name);
        } catch (Throwable throwable) {
            System.err.println("FAIL " + name + ": " + throwable);
            throw throwable;
        }
    }

    private static void equal(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("expected " + expected + " but got " + actual);
        }
    }

    private static void truth(boolean condition) {
        if (!condition) {
            throw new AssertionError("condition was false");
        }
    }

    private static void contains(String value, String expectedPart) {
        if (!value.contains(expectedPart)) {
            throw new AssertionError("expected '" + value + "' to contain '" + expectedPart + "'");
        }
    }

    private static <T extends Throwable> void expectThrows(
            Class<T> type, CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable throwable) {
            if (type.isInstance(throwable)) {
                return;
            }
            throw new AssertionError("expected " + type.getSimpleName()
                    + " but got " + throwable, throwable);
        }
        throw new AssertionError("expected " + type.getSimpleName());
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long millis) {
            this.millis = millis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
