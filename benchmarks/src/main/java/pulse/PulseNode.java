package pulse;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PulseNode implements AutoCloseable {
    private final PulseConfig config;
    private final HttpApi api;
    private final GossipManager gossip;
    private final ScheduledExecutorService sweeper;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PulseNode(PulseConfig config) throws Exception {
        this.config = config;
        Metrics metrics = new Metrics();
        BucketedGCounter counter = new BucketedGCounter(
                config.nodeId(),
                config.windowBuckets(),
                config.bucketMs(),
                config.maxKeys(),
                Clock.systemUTC(),
                config.lockStripes(),
                config.evictionSampleSize(),
                metrics);
        GossipCodec codec = new GossipCodec();
        PulseService service = new PulseService(config, counter, metrics);
        this.api = new HttpApi(config, service, counter, codec, metrics);
        this.gossip = new GossipManager(config, counter, codec, metrics);
        this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "pulse-sweeper"));
        this.sweeper.scheduleWithFixedDelay(counter::sweep, 2, 2, TimeUnit.SECONDS);
    }

    public void start() {
        api.start();
        gossip.start();
        System.out.printf(
                "Pulse %s listening on :%d with %d peer(s), limit=%d, window=%dms%n",
                config.nodeId(),
                api.port(),
                config.peers().size(),
                config.limit(),
                config.windowBuckets() * config.bucketMs());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            sweeper.shutdownNow();
            gossip.close();
            api.close();
        }
    }

    public static void main(String[] args) throws Exception {
        PulseConfig config;
        try {
            config = PulseConfig.fromEnvironment();
        } catch (IllegalArgumentException exception) {
            System.err.println("Configuration error: " + exception.getMessage());
            System.exit(2);
            return;
        }

        PulseNode node = new PulseNode(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.close();
            stopped.countDown();
        }, "pulse-shutdown"));
        node.start();
        stopped.await();
    }
}
