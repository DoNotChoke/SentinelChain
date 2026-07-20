package com.sentinelchain.flink.deduplicator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import com.sentinelchain.flink.deduplicator.Deduplication.Decision;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the pure dedup decision and the content signature (no Flink state). */
class DeduplicationTest {

    private static final Schema EVENT = AvroSchemas.forTopic("events.normalized.v1");
    private static final long EVENT_TIME = Instant.parse("2026-07-01T09:00:00Z").toEpochMilli();

    @Test
    void firstSightingEmitsNew() {
        assertEquals(Decision.EMIT_NEW, Deduplication.decide(null, "hash"));
    }

    @Test
    void identicalContentIsDropped() {
        assertEquals(Decision.DROP, Deduplication.decide("hash", "hash"));
    }

    @Test
    void changedContentIsAnUpdate() {
        assertEquals(Decision.EMIT_UPDATE, Deduplication.decide("hashA", "hashB"));
    }

    @Test
    void replayHashesIdentically() {
        GenericRecord a = event(6.2, "active");
        GenericRecord b = event(6.2, "active");
        assertEquals(PayloadHash.of(a), PayloadHash.of(b));
    }

    @Test
    void magnitudeCorrectionChangesHash() {
        assertNotEquals(PayloadHash.of(event(6.2, "active")), PayloadHash.of(event(6.5, "active")));
    }

    @Test
    void cancellationChangesHash() {
        assertNotEquals(
                PayloadHash.of(event(6.2, "active")), PayloadHash.of(event(6.2, "cancelled")));
    }

    @Test
    void envelopeIdentifiersDoNotAffectHash() {
        GenericRecord a = event(6.2, "active");
        GenericRecord b = event(6.2, "active");
        b.put("event_id", "different-uuid");
        b.put("trace_id", "different-trace");
        b.put("source_version", "2026-07-01T10:00:00Z");
        assertEquals(PayloadHash.of(a), PayloadHash.of(b));
    }

    private static GenericRecord event(double magnitude, String status) {
        Schema payloadSchema = EVENT.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("latitude", 35.1);
        payload.put("longitude", 139.2);
        payload.put("severity", magnitude);
        payload.put("severity_scale", "magnitude_mw");
        payload.put("depth_km", 38.0);
        payload.put("place", "Near the east coast of Honshu");
        payload.put("source_url", "https://example.test/us7000abcd");
        payload.put("status", status);

        GenericRecord event = new GenericData.Record(EVENT);
        event.put("event_id", "11111111-1111-1111-1111-111111111111");
        event.put("event_type", "earthquake");
        event.put("event_version", 1);
        event.put("source", "usgs");
        event.put("source_event_id", "us7000abcd");
        event.put("source_version", "2026-07-01T09:08:00Z");
        event.put("event_time", EVENT_TIME);
        event.put("ingested_at", EVENT_TIME + 300_000L);
        event.put("normalized_at", EVENT_TIME + 360_000L);
        event.put("trace_id", "22222222-2222-2222-2222-222222222222");
        event.put("correlation_id", null);
        event.put("tenant_id", "demo");
        event.put("payload", payload);
        return event;
    }
}
