package pulse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class GossipCodec {
    private static final String HEADER = "PULSE/1";
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public byte[] encode(BucketedGCounter.Snapshot snapshot) {
        StringBuilder output = new StringBuilder(128 + snapshot.cells().size() * 64);
        output.append(HEADER).append('\n');
        output.append("source\t").append(encodeText(snapshot.sourceNodeId())).append('\n');
        for (BucketedGCounter.Cell cell : snapshot.cells()) {
            output.append("cell\t")
                    .append(encodeText(cell.key())).append('\t')
                    .append(cell.bucket()).append('\t')
                    .append(encodeText(cell.nodeId())).append('\t')
                    .append(cell.count()).append('\n');
        }
        byte[] uncompressed = output.toString().getBytes(StandardCharsets.UTF_8);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(uncompressed.length);
             GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(uncompressed);
            gzip.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress gossip payload", e);
        }
    }

    public Envelope decode(byte[] body) {
        byte[] decompressed;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(body);
             GZIPInputStream gzip = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = gzip.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            decompressed = baos.toByteArray();
        } catch (IOException e) {
            throw new InvalidGossipException("Failed to decompress gossip payload", e);
        }
        String text = decodeUtf8(decompressed);
        String[] lines = text.split("\\n", -1);
        if (lines.length < 2 || !stripCarriageReturn(lines[0]).equals(HEADER)) {
            throw new InvalidGossipException("unsupported or missing gossip header");
        }
        String[] sourceLine = fields(lines[1], 2);
        if (!sourceLine[0].equals("source")) {
            throw new InvalidGossipException("missing gossip metadata");
        }
        String source = decodeText(sourceLine[1]);
        if (source.isBlank() || source.getBytes(StandardCharsets.UTF_8).length > 128) {
            throw new InvalidGossipException("invalid source node ID");
        }
        List<BucketedGCounter.Cell> cells = new ArrayList<>();
        for (int index = 2; index < lines.length; index++) {
            String line = stripCarriageReturn(lines[index]);
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = fields(line, 5);
            if (!parts[0].equals("cell")) {
                throw new InvalidGossipException("unknown record at line " + (index + 1));
            }
            String key = decodeText(parts[1]);
            String nodeId = decodeText(parts[3]);
            if (key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > 512) {
                throw new InvalidGossipException("invalid key at line " + (index + 1));
            }
            if (nodeId.isBlank() || nodeId.getBytes(StandardCharsets.UTF_8).length > 128) {
                throw new InvalidGossipException("invalid node ID at line " + (index + 1));
            }
            long bucket = parseLong(parts[2], "bucket");
            long count = parseLong(parts[4], "count");
            if (count <= 0) {
                throw new InvalidGossipException("count must be positive");
            }
            cells.add(new BucketedGCounter.Cell(key, bucket, nodeId, count));
        }
        return new Envelope(source, List.copyOf(cells));
    }

    private static String[] fields(String line, int expected) {
        String[] parts = stripCarriageReturn(line).split("\\t", -1);
        if (parts.length != expected) {
            throw new InvalidGossipException("malformed gossip record");
        }
        return parts;
    }

    private static String encodeText(String value) {
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeText(String value) {
        try {
            return decodeUtf8(DECODER.decode(value));
        } catch (IllegalArgumentException exception) {
            throw new InvalidGossipException("invalid Base64 text", exception);
        }
    }

    private static String decodeUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw new InvalidGossipException("body is not valid UTF-8", exception);
        }
    }

    private static long parseLong(String value, String field) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new InvalidGossipException(field + " must be an integer", exception);
        }
    }

    private static String stripCarriageReturn(String line) {
        return line.endsWith("\r") ? line.substring(0, line.length() - 1) : line;
    }

    public record Envelope(
            String sourceNodeId, List<BucketedGCounter.Cell> cells) {}

    public static final class InvalidGossipException extends RuntimeException {
        public InvalidGossipException(String message) {
            super(message);
        }

        public InvalidGossipException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
