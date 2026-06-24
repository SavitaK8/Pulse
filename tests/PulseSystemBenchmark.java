import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;

public class PulseSystemBenchmark {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting System Benchmarks...\n");
        benchmarkC_DeltaGossipCompression();
        benchmarkD_MemoryPressure();
        benchmarkE_ClusterConvergence();
    }

    private static void benchmarkC_DeltaGossipCompression() throws Exception {
        System.out.println("Benchmark C: Delta Gossip Compression");
        BucketedGCounter counter = new BucketedGCounter("node-a", 60, 1000, 100000, Clock.systemUTC(), 256, 10, new Metrics());
        
        // Generate 1000 keys
        for (int i = 0; i < 1000; i++) {
            counter.incrementAndEstimate("key-" + i);
        }
        
        GossipCodec codec = new GossipCodec();
        BucketedGCounter.Snapshot fullSnapshot = counter.snapshot(0);
        byte[] fullPayload = codec.encode(fullSnapshot);
        System.out.println("Full State Size:  " + fullPayload.length + " bytes");
        
        // Modify 1 key
        long syncTime = System.currentTimeMillis();
        Thread.sleep(10);
        counter.incrementAndEstimate("key-500");
        
        BucketedGCounter.Snapshot deltaSnapshot = counter.snapshot(syncTime);
        byte[] deltaPayload = codec.encode(deltaSnapshot);
        System.out.println("Delta State Size: " + deltaPayload.length + " bytes");
        
        double saved = 100.0 * (fullPayload.length - deltaPayload.length) / fullPayload.length;
        System.out.printf("Bandwidth Saved:  %.2f%%\n", saved);
        System.out.println("--------------------------------------------------");
    }

    private static void benchmarkD_MemoryPressure() throws Exception {
        System.out.println("Benchmark D: Memory Pressure & Recovery");
        BucketedGCounter counter = new BucketedGCounter("node-a", 10, 1000, 100000, Clock.systemUTC(), 256, 10, new Metrics());
        
        long start = System.currentTimeMillis();
        // Attack with 500k unique keys
        for (int i = 0; i < 500000; i++) {
            counter.incrementAndEstimate("attack-" + i);
        }
        long end = System.currentTimeMillis();
        
        System.out.println("Attack Time: " + (end - start) + " ms");
        System.out.println("Peak Keys:   " + counter.keyCount() + " (Limit: 100000)");
        System.out.println("LRU Evicted: " + counter.evictedCount());
        
        System.out.println("Waiting for Sweeper to recover (TTL = 10s)...");
        long recoveryStart = System.currentTimeMillis();
        while (counter.keyCount() > 0) {
            counter.sweep();
            if (counter.keyCount() == 0) break;
            Thread.sleep(100);
            if (System.currentTimeMillis() - recoveryStart > 15000) break;
        }
        System.out.println("Recovery Time: " + (System.currentTimeMillis() - recoveryStart) + " ms");
        System.out.println("Final Keys:    " + counter.keyCount());
        System.out.println("--------------------------------------------------");
    }

    private static void benchmarkE_ClusterConvergence() throws Exception {
        System.out.println("Benchmark E: Cluster Convergence");
        
        PulseConfig confA = new PulseConfig("node-a", 8081, List.of(URI.create("http://localhost:8082"), URI.create("http://localhost:8083")), 50, 10, 1000, 50, 500, 100000, 256, 10, "0123456789abcdef0123456789abcdef");
        PulseConfig confB = new PulseConfig("node-b", 8082, List.of(URI.create("http://localhost:8081"), URI.create("http://localhost:8083")), 50, 10, 1000, 50, 500, 100000, 256, 10, "0123456789abcdef0123456789abcdef");
        PulseConfig confC = new PulseConfig("node-c", 8083, List.of(URI.create("http://localhost:8081"), URI.create("http://localhost:8082")), 50, 10, 1000, 50, 500, 100000, 256, 10, "0123456789abcdef0123456789abcdef");
        
        PulseNode nodeA = new PulseNode(confA);
        PulseNode nodeB = new PulseNode(confB);
        PulseNode nodeC = new PulseNode(confC);
        
        nodeA.start();
        nodeB.start();
        nodeC.start();
        
        HttpClient client = HttpClient.newHttpClient();
        long[] times = new long[100];
        
        for (int i = 0; i < 100; i++) {
            String key = "converge-" + i;
            long start = System.currentTimeMillis();
            
            HttpRequest reqA = HttpRequest.newBuilder().uri(URI.create("http://localhost:8081/increment?key=" + key)).GET().build();
            client.send(reqA, HttpResponse.BodyHandlers.discarding());
            
            boolean bConverged = false;
            boolean cConverged = false;
            
            while (!bConverged || !cConverged) {
                if (!bConverged) {
                    HttpRequest reqB = HttpRequest.newBuilder().uri(URI.create("http://localhost:8082/estimate?key=" + key)).GET().build();
                    HttpResponse<String> resB = client.send(reqB, HttpResponse.BodyHandlers.ofString());
                    if (resB.body().contains("\"estimatedCount\":") && !resB.body().contains("\"estimatedCount\":0")) {
                        bConverged = true;
                    }
                }
                if (!cConverged) {
                    HttpRequest reqC = HttpRequest.newBuilder().uri(URI.create("http://localhost:8083/estimate?key=" + key)).GET().build();
                    HttpResponse<String> resC = client.send(reqC, HttpResponse.BodyHandlers.ofString());
                    if (resC.body().contains("\"estimatedCount\":") && !resC.body().contains("\"estimatedCount\":0")) {
                        cConverged = true;
                    }
                }
                if (!bConverged || !cConverged) {
                    Thread.sleep(5);
                }
            }
            times[i] = System.currentTimeMillis() - start;
        }
        
        Arrays.sort(times);
        System.out.println("P50 Convergence: " + times[50] + " ms");
        System.out.println("P95 Convergence: " + times[95] + " ms");
        System.out.println("P99 Convergence: " + times[99] + " ms");
        
        System.exit(0);
    }
}
