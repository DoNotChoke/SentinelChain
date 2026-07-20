package com.sentinelchain.flink.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import com.sentinelchain.flink.common.geo.Haversine;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** Tests the impact-record builder on a known event/facility pair (no Flink runtime). */
class ImpactCandidatesTest {

    private static final Schema EVENT = AvroSchemas.forTopic("events.deduplicated.v1");
    private static final Schema IMPACT = AvroSchemas.forTopic("risk.impact_candidates.v1");
    private static final long NOW = Instant.parse("2026-07-01T09:20:00Z").toEpochMilli();

    @Test
    void buildsDeterministicImpactWithinRadius() {
        // Quake at Tokyo (mag 6.2 → 120 km), facility at Yokohama (~28 km away).
        GenericRecord event = quake(35.681, 139.767, 6.2);
        Facility facility = new Facility("fac-1", "sup-1", 35.466, 139.622, "active");
        double distance = Haversine.km(35.681, 139.767, 35.466, 139.622);

        GenericRecord impact = ImpactCandidates.build(event, facility, distance, 120.0, IMPACT, NOW);

        assertEquals("us7000abcd::fac-1", impact.get("impact_id").toString());
        assertEquals("facility", impact.get("asset_type").toString());
        assertEquals("fac-1", impact.get("asset_id").toString());
        assertEquals("sup-1", impact.get("supplier_id").toString());
        assertEquals(true, impact.get("inside_affected_area"));
        assertEquals(120.0, (double) impact.get("radius_km"));
        assertEquals(6.2, (double) impact.get("severity"));

        double score = (double) impact.get("geospatial_score");
        assertTrue(score > 0.7 && score < 1.0, "near facility should score high but not 1: " + score);
    }

    private static GenericRecord quake(double lat, double lon, double magnitude) {
        Schema payloadSchema = EVENT.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("latitude", lat);
        payload.put("longitude", lon);
        payload.put("severity", magnitude);
        payload.put("severity_scale", "magnitude_mw");
        payload.put("depth_km", 38.0);
        payload.put("place", "Tokyo");
        payload.put("source_url", "https://example.test/us7000abcd");
        payload.put("status", "active");

        GenericRecord event = new GenericData.Record(EVENT);
        event.put("event_id", "msg-1");
        event.put("event_type", "earthquake");
        event.put("event_version", 1);
        event.put("source", "usgs");
        event.put("source_event_id", "us7000abcd");
        event.put("source_version", "2026-07-01T09:08:00Z");
        event.put("event_time", NOW);
        event.put("ingested_at", NOW);
        event.put("normalized_at", NOW);
        event.put("trace_id", "trace-1");
        event.put("correlation_id", null);
        event.put("tenant_id", "demo");
        event.put("payload", payload);
        return event;
    }
}
