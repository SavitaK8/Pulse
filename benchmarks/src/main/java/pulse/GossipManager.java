package pulse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.Base64;

public final class GossipManager implements AutoCloseable {
    private final PulseConfig config;
    private final BucketedGCounter counter;
    private final GossipCodec codec;
    private final Metrics metrics;
    private final HttpClient client;
    private final ScheduledExecutorService scheduler;
    private final Map<URI, AtomicBoolean> inFlight = new ConcurrentHashMap<>();
    private final Map<URI, Long> lastSuccessfulSync = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final ThreadLocal<Mac> mac;

    public GossipManager(
            PulseConfig config,
            BucketedGCounter counter,
            GossipCodec codec,
            Metrics metrics) {
        this.config = config;
        this.counter = counter;
        this.codec = codec;
        this.metrics = metrics;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.gossipTimeoutMs()))
                .build();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pulse-gossip-scheduler");
            thread.setDaemon(true);
            return thread;
        });
        for (URI peer : config.peers()) {
            inFlight.put(peer, new AtomicBoolean());
        }
        if (config.clusterSecret() != null && !config.clusterSecret().isBlank()) {
            this.mac = ThreadLocal.withInitial(() -> {
                try {
                    Mac m = Mac.getInstance("HmacSHA256");
                    m.init(new SecretKeySpec(config.clusterSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                    return m;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize HMAC", e);
                }
            });
        } else {
            this.mac = null;
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true) || config.peers().isEmpty()) {
            return;
        }
        scheduler.scheduleWithFixedDelay(
                this::safeGossipRound,
                config.gossipIntervalMs(),
                config.gossipIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    void gossipRound() {
        for (URI peer : config.peers()) {
            AtomicBoolean peerInFlight = inFlight.get(peer);
            if (!peerInFlight.compareAndSet(false, true)) {
                metrics.recordGossipSkipped();
                continue;
            }
            long lastSync = lastSuccessfulSync.getOrDefault(peer, 0L);
            long roundStart = System.currentTimeMillis();
            BucketedGCounter.Snapshot snapshot = counter.snapshot(lastSync);
            metrics.recordGossipKeysSent(snapshot.cells().size());
            byte[] payload = codec.encode(snapshot);
            String messageId = UUID.randomUUID().toString();
            
            URI gossipUri = peer.resolve("/gossip");
            
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(gossipUri)
                    .timeout(Duration.ofMillis(config.gossipTimeoutMs()))
                    .header("Content-Type", "application/x-pulse-gossip; version=1")
                    .header("Content-Encoding", "gzip")
                    .header("X-Pulse-Timestamp", String.valueOf(roundStart))
                    .header("X-Pulse-Message-Id", messageId);
                    
            if (mac != null) {
                try {
                    Mac m = mac.get();
                    m.update("POST".getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    m.update(gossipUri.getPath().getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    m.update(String.valueOf(roundStart).getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    m.update(messageId.getBytes(StandardCharsets.UTF_8));
                    m.update((byte) '\n');
                    byte[] sigBytes = m.doFinal(payload);
                    String signature = Base64.getUrlEncoder().withoutPadding().encodeToString(sigBytes);
                    requestBuilder.header("X-Pulse-Signature", signature);
                } catch (Exception e) {
                    metrics.recordGossipFailure();
                    peerInFlight.set(false);
                    continue;
                }
            }
            
            HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
            client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .whenComplete((response, error) -> {
                        try {
                            if (error == null
                                    && response.statusCode() >= 200
                                    && response.statusCode() < 300) {
                                metrics.recordGossipSent(payload.length);
                                lastSuccessfulSync.put(peer, roundStart);
                            } else {
                                metrics.recordGossipFailure();
                            }
                        } finally {
                            peerInFlight.set(false);
                        }
                    });
        }
    }

    private void safeGossipRound() {
        try {
            gossipRound();
        } catch (RuntimeException exception) {
            metrics.recordGossipFailure();
            System.err.println("Gossip round failed: " + exception.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
