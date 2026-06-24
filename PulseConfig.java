import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record PulseConfig(
        String nodeId,
        int port,
        List<URI> peers,
        long limit,
        int windowBuckets,
        long bucketMs,
        long gossipIntervalMs,
        long gossipTimeoutMs,
        int maxKeys,
        int maxGossipBytes,
        String clusterSecret,
        int lockStripes,
        int evictionSampleSize) {

    public PulseConfig {
        peers = List.copyOf(peers);
        if (!peers.isEmpty()) {
            if (clusterSecret == null || clusterSecret.isBlank()) {
                throw new IllegalArgumentException("CLUSTER_SECRET must be set when peers are configured");
            }
            if (clusterSecret.getBytes(StandardCharsets.UTF_8).length < 32) {
                throw new IllegalArgumentException("CLUSTER_SECRET must be at least 32 bytes long");
            }
        }
        requireText(nodeId, "NODE_ID", 128);
        requireRange(port, 0, 65535, "PORT");
        requirePositive(limit, "LIMIT");
        requireRange(windowBuckets, 1, 86_400, "WINDOW_BUCKETS");
        requirePositive(bucketMs, "BUCKET_MS");
        if (bucketMs > Long.MAX_VALUE / windowBuckets) {
            throw new IllegalArgumentException("configured window duration is too large");
        }
        requirePositive(gossipIntervalMs, "GOSSIP_INTERVAL_MS");
        requirePositive(gossipTimeoutMs, "GOSSIP_TIMEOUT_MS");
        requireRange(maxKeys, 1, 10_000_000, "MAX_KEYS");
        requireRange(maxGossipBytes, 1_024, 256 * 1024 * 1024, "MAX_GOSSIP_BYTES");
        requirePositive(lockStripes, "PULSE_LOCK_STRIPES");
        requireRange(evictionSampleSize, 1, 1000, "PULSE_EVICTION_SAMPLE_SIZE");
    }

    public static PulseConfig fromEnvironment() {
        return from(System.getenv());
    }

    static PulseConfig from(Map<String, String> env) {
        String nodeId = value(env, "NODE_ID", "node-a");
        int port = intValue(env, "PORT", 9001);
        List<URI> peers = parsePeers(value(env, "PEERS", ""));
        long limit = longValue(env, "LIMIT", 50);
        int windowBuckets = intValue(env, "WINDOW_BUCKETS", 10);
        long bucketMs = longValue(env, "BUCKET_MS", 1_000);
        long intervalMs = longValue(env, "GOSSIP_INTERVAL_MS", 200);
        long timeoutMs = longValue(env, "GOSSIP_TIMEOUT_MS", 1_000);
        int maxKeys = intValue(env, "MAX_KEYS", 100_000);
        int maxGossipBytes = intValue(env, "MAX_GOSSIP_BYTES", 4 * 1024 * 1024);
        String clusterSecret = value(env, "CLUSTER_SECRET", "");
        int defaultStripes = Math.max(64, Runtime.getRuntime().availableProcessors() * 8);
        int lockStripes = intValue(env, "PULSE_LOCK_STRIPES", defaultStripes);
        int evictionSampleSize = intValue(env, "PULSE_EVICTION_SAMPLE_SIZE", 10);
        return new PulseConfig(nodeId, port, peers, limit, windowBuckets, bucketMs,
                intervalMs, timeoutMs, maxKeys, maxGossipBytes, clusterSecret, lockStripes, evictionSampleSize);
    }

    private static List<URI> parsePeers(String raw) {
        if (raw.isBlank()) {
            return List.of();
        }
        List<URI> peers = new ArrayList<>();
        for (String item : raw.split(",")) {
            String peer = item.trim();
            if (peer.isEmpty()) {
                continue;
            }
            URI base = URI.create(peer.contains("://") ? peer : "http://" + peer);
            if (base.getHost() == null || base.getPort() < 1 || base.getUserInfo() != null
                    || base.getQuery() != null || base.getFragment() != null) {
                throw new IllegalArgumentException("Invalid PEERS entry: " + peer);
            }
            if (!base.getScheme().equals("http") && !base.getScheme().equals("https")) {
                throw new IllegalArgumentException("PEERS entries must use http or https: " + peer);
            }
            String path = base.getPath();
            if (path != null && !path.isEmpty() && !path.equals("/")) {
                throw new IllegalArgumentException("PEERS entries must not contain a path: " + peer);
            }
            peers.add(URI.create(base.getScheme() + "://" + base.getAuthority() + "/gossip"));
        }
        return peers;
    }

    private static String value(Map<String, String> env, String key, String fallback) {
        return env.getOrDefault(key, fallback);
    }

    private static int intValue(Map<String, String> env, String key, int fallback) {
        String value = env.get(key);
        try {
            return value == null ? fallback : Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(key + " must be an integer", exception);
        }
    }

    private static long longValue(Map<String, String> env, String key, long fallback) {
        String value = env.get(key);
        try {
            return value == null ? fallback : Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(key + " must be an integer", exception);
        }
    }

    private static void requireText(String value, String name, int maxBytes) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(name + " is too long");
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireRange(long value, long min, long max, String name) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
    }
}
