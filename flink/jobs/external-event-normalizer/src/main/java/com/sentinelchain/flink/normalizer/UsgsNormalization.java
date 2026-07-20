package com.sentinelchain.flink.normalizer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public final class UsgsNormalization {

    /** Canonical event category emitted on events.normalized.v1 (drives §11.4 threshold rules). */
    static final String CANONICAL_EVENT_TYPE = "earthquake";

    static final String SEVERITY_SCALE = "magnitude_mw";

    /** Service name stamped on data-quality violations (audit.data_quality.v1 `source`). */
    static final String DETECTOR = "external-event-normalizer";

    /** A magnitude outside this range is a data error, not a real quake (Richter is unbounded above in theory, but real events sit well within). */
    private static final double MIN_MAGNITUDE = -2.0;
    private static final double MAX_MAGNITUDE = 12.0;

    /** Tolerated clock skew before an event_time in the future is treated as a data error. */
    private static final long MAX_FUTURE_SKEW_MILLIS = Duration.ofHours(1).toMillis();

    private UsgsNormalization() {}

    public static List<String> validate(GenericRecord raw, long nowMillis) {
        List<String> reasons = new ArrayList<>();

        Object sourceEventId = raw.get("source_event_id");
        if (sourceEventId == null || sourceEventId.toString().isBlank()) {
            reasons.add("missing_source_event_id");
        }

        GenericRecord payload = (GenericRecord) raw.get("payload");
        Double latitude = asDouble(payload.get("latitude"));
        Double longitude = asDouble(payload.get("longitude"));

        if (latitude == null || latitude < -90.0 || latitude > 90.0) {
            reasons.add("latitude_out_of_range");
        }
        if (longitude == null || longitude < -180.0 || longitude > 180.0) {
            reasons.add("longitude_out_of_range");
        }

        Double magnitude = asDouble(payload.get("magnitude"));
        // Magnitude may legitimately be absent for a very recent event; only flag bad values.
        if (magnitude != null && (magnitude < MIN_MAGNITUDE || magnitude > MAX_MAGNITUDE)) {
            reasons.add("magnitude_out_of_range");
        }

        long eventTime = ((Number) raw.get("event_time")).longValue();
        if (eventTime > nowMillis + MAX_FUTURE_SKEW_MILLIS) {
            reasons.add("event_time_in_future");
        }

        return reasons;
    }

    /** Map a valid raw USGS envelope to the canonical events.normalized.v1 record. */
    public static GenericRecord toNormalized(
            GenericRecord raw, Schema normalizedSchema, long normalizedAtMillis) {
        GenericRecord rawPayload = (GenericRecord) raw.get("payload");
        Object magnitude = rawPayload.get("magnitude");

        Schema payloadSchema = normalizedSchema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("latitude", rawPayload.get("latitude"));
        payload.put("longitude", rawPayload.get("longitude"));
        payload.put("severity", magnitude);
        payload.put("severity_scale", magnitude != null ? SEVERITY_SCALE : null);
        payload.put("depth_km", rawPayload.get("depth_km"));
        payload.put("place", rawPayload.get("place"));
        payload.put("source_url", rawPayload.get("source_url"));
        payload.put("status", "active");

        GenericRecord out = new GenericData.Record(normalizedSchema);
        out.put("event_id", raw.get("event_id"));
        out.put("event_type", CANONICAL_EVENT_TYPE);
        out.put("event_version", 1);
        out.put("source", raw.get("source"));
        out.put("source_event_id", raw.get("source_event_id"));
        out.put("source_version", raw.get("source_version"));
        out.put("event_time", raw.get("event_time"));
        out.put("ingested_at", raw.get("ingested_at"));
        out.put("normalized_at", normalizedAtMillis);
        out.put("trace_id", raw.get("trace_id"));
        out.put("correlation_id", raw.get("correlation_id"));
        out.put("tenant_id", raw.get("tenant_id"));
        out.put("payload", payload);
        return out;
    }

    /** Build the audit.data_quality.v1 record for a raw envelope that failed {@link #validate}. */
    public static GenericRecord toViolation(
            GenericRecord raw, List<String> reasons, Schema auditSchema, long detectedAtMillis) {
        Object sourceEventId = raw.get("source_event_id");
        String sourceEventIdStr =
                sourceEventId != null && !sourceEventId.toString().isBlank()
                        ? sourceEventId.toString()
                        : null;

        Schema payloadSchema = auditSchema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("origin_source", raw.get("source"));
        payload.put("source_event_id", sourceEventIdStr);
        payload.put("reasons", new ArrayList<CharSequence>(reasons));
        payload.put("raw", raw.toString());

        GenericRecord out = new GenericData.Record(auditSchema);
        out.put("event_id", UUID.randomUUID().toString());
        out.put("event_type", "data_quality.violation");
        out.put("event_version", 1);
        out.put("source", DETECTOR);
        out.put("source_event_id", sourceEventIdStr != null ? sourceEventIdStr : "unknown");
        out.put("source_version", raw.get("source_version"));
        out.put("event_time", detectedAtMillis);
        out.put("ingested_at", raw.get("ingested_at"));
        out.put("trace_id", raw.get("trace_id"));
        out.put("correlation_id", raw.get("correlation_id"));
        out.put("tenant_id", raw.get("tenant_id"));
        out.put("payload", payload);
        return out;
    }

    private static Double asDouble(Object value) {
        return value == null ? null : ((Number) value).doubleValue();
    }
}
