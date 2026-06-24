import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

public final class HttpApi implements AutoCloseable {
    private final PulseConfig config;
    private final PulseService service;
    private final BucketedGCounter counter;
    private final GossipCodec codec;
    private final Metrics metrics;
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final ThreadLocal<Mac> mac;
    private final ConcurrentHashMap<String, Long> seenMessages = new ConcurrentHashMap<>();

    public HttpApi(
            PulseConfig config,
            PulseService service,
            BucketedGCounter counter,
            GossipCodec codec,
            Metrics metrics) throws IOException {
        this.config = config;
        this.service = service;
        this.counter = counter;
        this.codec = codec;
        this.metrics = metrics;
        this.server = HttpServer.create(new InetSocketAddress(config.port()), 128);
        int workers = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.executor = new ThreadPoolExecutor(
                workers,
                workers,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1_024),
                namedThreads(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        server.setExecutor(executor);
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
        server.createContext("/check", this::handleCheck);
        server.createContext("/estimate", this::handleEstimate);
        server.createContext("/gossip", this::handleGossip);
        server.createContext("/health", this::handleHealth);
        server.createContext("/metrics", this::handleMetrics);
    }

    private void handleEstimate(HttpExchange exchange) {
        try {
            if (!requirePath(exchange, "/estimate")) {
                return;
            }
            if (!requireMethod(exchange, "GET")) {
                return;
            }
            String key = queryParameters(exchange).get("key");
            PulseService.EstimateResult result = service.estimate(key);
            String body = "{"
                    + "\"key\":" + Json.quote(result.key()) + ","
                    + "\"estimatedCount\":" + result.estimatedCount() + ","
                    + "\"limit\":" + result.limit() + ","
                    + "\"nodeId\":" + Json.quote(result.nodeId())
                    + "}";
            send(exchange, 200, "application/json; charset=utf-8", body);
        } catch (PulseService.InvalidKeyException exception) {
            sendError(exchange, 400, exception.getMessage());
        } catch (IllegalArgumentException exception) {
            sendError(exchange, 400, "invalid query string");
        } catch (Exception exception) {
            internalError(exchange, exception);
        }
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handleCheck(HttpExchange exchange) {
        try {
            if (!requirePath(exchange, "/check")) {
                return;
            }
            if (!requireMethod(exchange, "GET")) {
                return;
            }
            String key = queryParameters(exchange).get("key");
            PulseService.CheckResult result = service.check(key);
            String body = "{"
                    + "\"allowed\":" + result.allowed() + ","
                    + "\"key\":" + Json.quote(result.key()) + ","
                    + "\"estimatedCount\":" + result.estimatedCount() + ","
                    + "\"limit\":" + result.limit() + ","
                    + "\"remaining\":" + result.remaining() + ","
                    + "\"nodeId\":" + Json.quote(result.nodeId()) + ","
                    + "\"bucket\":" + result.bucket()
                    + "}";
            send(exchange, 200, "application/json; charset=utf-8", body);
        } catch (PulseService.InvalidKeyException exception) {
            sendError(exchange, 400, exception.getMessage());
        } catch (IllegalArgumentException exception) {
            sendError(exchange, 400, "invalid query string");
        } catch (Exception exception) {
            internalError(exchange, exception);
        }
    }

    private void handleGossip(HttpExchange exchange) {
        try {
            if (!requirePath(exchange, "/gossip")) {
                return;
            }
            if (!requireMethod(exchange, "POST")) {
                return;
            }
            if (mac != null) {
                String signature = exchange.getRequestHeaders().getFirst("X-Pulse-Signature");
                String timestampStr = exchange.getRequestHeaders().getFirst("X-Pulse-Timestamp");
                String messageId = exchange.getRequestHeaders().getFirst("X-Pulse-Message-Id");
                
                if (signature == null || timestampStr == null || messageId == null) {
                    sendError(exchange, 401, "missing signature or metadata");
                    return;
                }
                
                long sentAt;
                try {
                    sentAt = Long.parseLong(timestampStr);
                } catch (NumberFormatException e) {
                    sendError(exchange, 401, "invalid timestamp");
                    return;
                }
                
                long now = System.currentTimeMillis();
                if (Math.abs(now - sentAt) > 30_000) {
                    sendError(exchange, 401, "expired timestamp");
                    return;
                }
                
                byte[] body = readBounded(exchange, config.maxGossipBytes());
                
                Mac m = mac.get();
                m.update("POST".getBytes(StandardCharsets.UTF_8));
                m.update((byte) '\n');
                m.update("/gossip".getBytes(StandardCharsets.UTF_8));
                m.update((byte) '\n');
                m.update(timestampStr.getBytes(StandardCharsets.UTF_8));
                m.update((byte) '\n');
                m.update(messageId.getBytes(StandardCharsets.UTF_8));
                m.update((byte) '\n');
                byte[] expectedSig = m.doFinal(body);
                byte[] providedSig;
                try {
                    providedSig = Base64.getUrlDecoder().decode(signature);
                } catch (IllegalArgumentException e) {
                    sendError(exchange, 401, "invalid signature format");
                    return;
                }
                
                if (!MessageDigest.isEqual(expectedSig, providedSig)) {
                    sendError(exchange, 401, "invalid signature");
                    return;
                }
                
                if (seenMessages.putIfAbsent(messageId, now) != null) {
                    sendError(exchange, 409, "replay detected");
                    return;
                }
                if (seenMessages.size() > 5000) {
                    long cutoff = now - 30_000;
                    seenMessages.entrySet().removeIf(e -> e.getValue() < cutoff);
                }
                
                GossipCodec.Envelope envelope = codec.decode(body);
                BucketedGCounter.MergeResult result = counter.merge(envelope.cells());
                metrics.recordGossipReceived(body.length, result);
                sendEmpty(exchange, 204);
            } else {
                byte[] body = readBounded(exchange, config.maxGossipBytes());
                GossipCodec.Envelope envelope = codec.decode(body);
                BucketedGCounter.MergeResult result = counter.merge(envelope.cells());
                metrics.recordGossipReceived(body.length, result);
                sendEmpty(exchange, 204);
            }
        } catch (PayloadTooLargeException exception) {
            sendError(exchange, 413, exception.getMessage());
        } catch (GossipCodec.InvalidGossipException exception) {
            sendError(exchange, 400, exception.getMessage());
        } catch (Exception exception) {
            internalError(exchange, exception);
        }
    }

    private void handleHealth(HttpExchange exchange) {
        try {
            if (!requirePath(exchange, "/health")) {
                return;
            }
            if (!requireMethod(exchange, "GET")) {
                return;
            }
            BucketedGCounter.StateSize size = counter.size();
            String body = "{"
                    + "\"status\":\"ok\","
                    + "\"nodeId\":" + Json.quote(config.nodeId()) + ","
                    + "\"port\":" + port() + ","
                    + "\"peers\":" + config.peers().size() + ","
                    + "\"limit\":" + config.limit() + ","
                    + "\"windowBuckets\":" + config.windowBuckets() + ","
                    + "\"bucketMs\":" + config.bucketMs() + ","
                    + "\"keys\":" + size.keys() + ","
                    + "\"cells\":" + size.cells()
                    + "}";
            send(exchange, 200, "application/json; charset=utf-8", body);
        } catch (Exception exception) {
            internalError(exchange, exception);
        }
    }

    private void handleMetrics(HttpExchange exchange) {
        try {
            if (!requirePath(exchange, "/metrics")) {
                return;
            }
            if (!requireMethod(exchange, "GET")) {
                return;
            }
            send(exchange, 200, "text/plain; version=0.0.4; charset=utf-8",
                    metrics.prometheus(counter.size()));
        } catch (Exception exception) {
            internalError(exchange, exception);
        }
    }

    private static Map<String, String> queryParameters(HttpExchange exchange) {
        Map<String, String> parameters = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return parameters;
        }
        for (String pair : raw.split("&")) {
            int separator = pair.indexOf('=');
            String rawName = separator < 0 ? pair : pair.substring(0, separator);
            String rawValue = separator < 0 ? "" : pair.substring(separator + 1);
            parameters.putIfAbsent(
                    URLDecoder.decode(rawName, StandardCharsets.UTF_8),
                    URLDecoder.decode(rawValue, StandardCharsets.UTF_8));
        }
        return parameters;
    }

    private static byte[] readBounded(HttpExchange exchange, int maxBytes) throws IOException {
        String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
        if (contentLength != null) {
            try {
                if (Long.parseLong(contentLength) > maxBytes) {
                    throw new PayloadTooLargeException(maxBytes);
                }
            } catch (NumberFormatException exception) {
                throw new GossipCodec.InvalidGossipException("invalid Content-Length");
            }
        }
        try (InputStream input = exchange.getRequestBody();
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8_192];
            int total = 0;
            int read;
            while ((read = input.read(buffer)) >= 0) {
                total += read;
                if (total > maxBytes) {
                    throw new PayloadTooLargeException(maxBytes);
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static boolean requireMethod(HttpExchange exchange, String method) {
        if (exchange.getRequestMethod().equalsIgnoreCase(method)) {
            return true;
        }
        exchange.getResponseHeaders().set("Allow", method);
        sendError(exchange, 405, "method not allowed");
        return false;
    }

    private static boolean requirePath(HttpExchange exchange, String path) {
        if (exchange.getRequestURI().getPath().equals(path)) {
            return true;
        }
        sendError(exchange, 404, "not found");
        return false;
    }

    private static void sendError(HttpExchange exchange, int status, String message) {
        send(exchange, status, "application/json; charset=utf-8",
                "{\"error\":" + Json.quote(message) + "}");
    }

    private static void internalError(HttpExchange exchange, Exception exception) {
        System.err.println("HTTP handler failed: " + exception);
        sendError(exchange, 500, "internal server error");
    }

    private static void send(
            HttpExchange exchange, int status, String contentType, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        try {
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException exception) {
            System.err.println("Failed to send HTTP response: " + exception.getMessage());
        } finally {
            exchange.close();
        }
    }

    private static void sendEmpty(HttpExchange exchange, int status) {
        try {
            exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            exchange.sendResponseHeaders(status, -1);
        } catch (IOException exception) {
            System.err.println("Failed to send HTTP response: " + exception.getMessage());
        } finally {
            exchange.close();
        }
    }

    private static ThreadFactory namedThreads() {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> new Thread(runnable, "pulse-http-" + sequence.incrementAndGet());
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
    }

    private static final class PayloadTooLargeException extends RuntimeException {
        private PayloadTooLargeException(int maxBytes) {
            super("gossip body exceeds " + maxBytes + " bytes");
        }
    }
}
