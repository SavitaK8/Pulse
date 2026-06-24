package pulse;
import java.nio.charset.StandardCharsets;

public final class PulseService {
    private static final int MAX_KEY_BYTES = 512;

    private final PulseConfig config;
    private final BucketedGCounter counter;
    private final Metrics metrics;

    public PulseService(PulseConfig config, BucketedGCounter counter, Metrics metrics) {
        this.config = config;
        this.counter = counter;
        this.metrics = metrics;
    }

    public CheckResult check(String key) {
        validateKey(key);
        try {
            BucketedGCounter.IncrementResult increment = counter.incrementAndEstimate(key);
            boolean allowed = increment.estimatedCount() <= config.limit();
            metrics.recordCheck(allowed);
            long remaining = Math.max(0, config.limit() - increment.estimatedCount());
            return new CheckResult(
                    allowed,
                    key,
                    increment.estimatedCount(),
                    config.limit(),
                    remaining,
                    config.nodeId(),
                    increment.bucket());
        } catch (BucketedGCounter.CapacityExceededException exception) {
            metrics.recordCapacityRejection();
            throw exception;
        }
    }

    public EstimateResult estimate(String key) {
        validateKey(key);
        long estimatedCount = counter.estimate(key);
        return new EstimateResult(key, estimatedCount, config.limit(), config.nodeId());
    }

    static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new InvalidKeyException("query parameter 'key' is required");
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new InvalidKeyException("key must be at most " + MAX_KEY_BYTES + " UTF-8 bytes");
        }
    }

    public record CheckResult(
            boolean allowed,
            String key,
            long estimatedCount,
            long limit,
            long remaining,
            String nodeId,
            long bucket) {}

    public record EstimateResult(
            String key, long estimatedCount, long limit, String nodeId) {}

    public static final class InvalidKeyException extends RuntimeException {
        public InvalidKeyException(String message) {
            super(message);
        }
    }
}
