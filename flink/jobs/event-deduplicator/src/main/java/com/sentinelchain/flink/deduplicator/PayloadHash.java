package com.sentinelchain.flink.deduplicator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.avro.generic.GenericRecord;

/**
 * Deterministic content signature of a canonical event, used to detect real changes vs replays.
 *
 * <p>The hash covers {@code event_time} plus every canonical payload field (location, severity,
 * status, …) — everything a downstream consumer reacts to — but deliberately excludes the envelope
 * identifiers ({@code event_id}, {@code trace_id}) and {@code source_version}. So an exact replay
 * (at-least-once from Job 1, or a service restart) hashes identically and is dropped, while a
 * magnitude correction or a status flip to {@code cancelled} changes the hash and flows through.
 *
 * <p>Fields are concatenated in a fixed order with a delimiter, then SHA-256'd; the same policy the
 * Python ingestion cursor uses, kept stable so the value is comparable across restarts.
 */
public final class PayloadHash {

    private static final char SEP = '|';

    private PayloadHash() {}

    public static String of(GenericRecord event) {
        GenericRecord p = (GenericRecord) event.get("payload");
        String canonical =
                new StringBuilder()
                        .append(event.get("event_time")).append(SEP)
                        .append(p.get("latitude")).append(SEP)
                        .append(p.get("longitude")).append(SEP)
                        .append(p.get("severity")).append(SEP)
                        .append(p.get("severity_scale")).append(SEP)
                        .append(p.get("depth_km")).append(SEP)
                        .append(p.get("place")).append(SEP)
                        .append(p.get("source_url")).append(SEP)
                        .append(p.get("status"))
                        .toString();
        return sha256Hex(canonical);
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a required algorithm on every JVM; this cannot happen.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
