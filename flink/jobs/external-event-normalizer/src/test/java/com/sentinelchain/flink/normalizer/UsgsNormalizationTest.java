package com.sentinelchain.flink.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import java.time.Instant;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the pure normalization + §28 data-quality logic (no Flink runtime). */
class UsgsNormalizationTest {

    private static final Schema RAW = AvroSchemas.forTopic("ext.usgs.raw.v1");
    private static final Schema NORMALIZED = AvroSchemas.forTopic("events.normalized.v1");
    private static final Schema AUDIT = AvroSchemas.forTopic("audit.data_quality.v1");

    private static final long NOW = Instant.parse("2026-07-01T00:00:00Z").toEpochMilli();
    private static final long EVENT_TIME = Instant.parse("2026-06-30T23:50:00Z").toEpochMilli();

    @Test
    void validRecordPassesValidation() {
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, 6.2, EVENT_TIME);
        assertTrue(UsgsNormalization.validate(raw, NOW).isEmpty());
    }

    @Test
    void nullMagnitudeIsAllowed() {
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, null, EVENT_TIME);
        assertTrue(UsgsNormalization.validate(raw, NOW).isEmpty());
    }

    @Test
    void outOfRangeCoordinatesAreRejected() {
        GenericRecord raw = rawRecord("us7000abcd", 200.0, 999.0, 6.2, EVENT_TIME);
        List<String> reasons = UsgsNormalization.validate(raw, NOW);
        assertTrue(reasons.contains("latitude_out_of_range"));
        assertTrue(reasons.contains("longitude_out_of_range"));
    }

    @Test
    void impossibleMagnitudeIsRejected() {
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, 42.0, EVENT_TIME);
        assertTrue(UsgsNormalization.validate(raw, NOW).contains("magnitude_out_of_range"));
    }

    @Test
    void eventTimeFarInFutureIsRejected() {
        long future = Instant.parse("2026-07-01T05:00:00Z").toEpochMilli(); // > now + 1h skew
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, 6.2, future);
        assertTrue(UsgsNormalization.validate(raw, NOW).contains("event_time_in_future"));
    }

    @Test
    void blankSourceEventIdIsRejected() {
        GenericRecord raw = rawRecord("  ", 35.1, 139.2, 6.2, EVENT_TIME);
        assertTrue(UsgsNormalization.validate(raw, NOW).contains("missing_source_event_id"));
    }

    @Test
    void toNormalizedMapsCanonicalFields() {
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, 6.2, EVENT_TIME);
        GenericRecord out = UsgsNormalization.toNormalized(raw, NORMALIZED, NOW);

        assertEquals("earthquake", out.get("event_type").toString());
        assertEquals("usgs", out.get("source").toString());
        assertEquals("us7000abcd", out.get("source_event_id").toString());
        assertEquals(EVENT_TIME, ((Number) out.get("event_time")).longValue());
        assertEquals(NOW, ((Number) out.get("normalized_at")).longValue());

        GenericRecord payload = (GenericRecord) out.get("payload");
        assertEquals(35.1, (double) payload.get("latitude"));
        assertEquals(139.2, (double) payload.get("longitude"));
        assertEquals(6.2, (double) payload.get("severity"));
        assertEquals("magnitude_mw", payload.get("severity_scale").toString());
        assertEquals("active", payload.get("status").toString());
    }

    @Test
    void toNormalizedLeavesSeverityScaleNullWhenNoMagnitude() {
        GenericRecord raw = rawRecord("us7000abcd", 35.1, 139.2, null, EVENT_TIME);
        GenericRecord out = UsgsNormalization.toNormalized(raw, NORMALIZED, NOW);
        GenericRecord payload = (GenericRecord) out.get("payload");
        assertNull(payload.get("severity"));
        assertNull(payload.get("severity_scale"));
    }

    @Test
    void toViolationCarriesReasonsAndOrigin() {
        GenericRecord raw = rawRecord("us7000abcd", 200.0, 139.2, 6.2, EVENT_TIME);
        List<String> reasons = UsgsNormalization.validate(raw, NOW);
        GenericRecord violation = UsgsNormalization.toViolation(raw, reasons, AUDIT, NOW);

        assertEquals("data_quality.violation", violation.get("event_type").toString());
        assertEquals("external-event-normalizer", violation.get("source").toString());
        assertEquals("us7000abcd", violation.get("source_event_id").toString());

        GenericRecord payload = (GenericRecord) violation.get("payload");
        assertEquals("usgs", payload.get("origin_source").toString());
        @SuppressWarnings("unchecked")
        List<Object> emitted = (List<Object>) payload.get("reasons");
        assertTrue(emitted.stream().map(Object::toString).anyMatch("latitude_out_of_range"::equals));
    }

    private static GenericRecord rawRecord(
            String sourceEventId, double lat, double lon, Double magnitude, long eventTimeMillis) {
        Schema payloadSchema = RAW.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("source_event_id", sourceEventId);
        payload.put("magnitude", magnitude);
        payload.put("latitude", lat);
        payload.put("longitude", lon);
        payload.put("depth_km", 38.0);
        payload.put("place", "Near the east coast of Honshu");
        payload.put("event_time", "2026-06-30T23:50:00Z");
        payload.put("updated_time", "2026-06-30T23:58:00Z");
        payload.put("source_url", "https://example.test/us7000abcd");

        GenericRecord raw = new GenericData.Record(RAW);
        raw.put("event_id", "11111111-1111-1111-1111-111111111111");
        raw.put("event_type", "usgs.earthquake.raw");
        raw.put("event_version", 1);
        raw.put("source", "usgs");
        raw.put("source_event_id", sourceEventId);
        raw.put("source_version", "2026-06-30T23:58:00Z");
        raw.put("event_time", eventTimeMillis);
        raw.put("ingested_at", eventTimeMillis + 300_000L);
        raw.put("trace_id", "22222222-2222-2222-2222-222222222222");
        raw.put("correlation_id", null);
        raw.put("tenant_id", "demo");
        raw.put("payload", payload);
        return raw;
    }
}
