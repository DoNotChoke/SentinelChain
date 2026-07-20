package com.sentinelchain.flink.geo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Builds {@code risk.impact_candidates.v1} records from an event / facility pair (PLAN §11.4).
 *
 * <p>Kept free of Flink types so the record shape is testable on its own. The {@code impact_id} is
 * deterministic ({@code source_event_id::asset_id}) so a replay produces the same id and downstream
 * stays idempotent (ADR-007).
 */
public final class ImpactCandidates {

    static final String ASSET_TYPE_FACILITY = "facility";

    private ImpactCandidates() {}

    public static GenericRecord build(
            GenericRecord event,
            Facility facility,
            double distanceKm,
            double radiusKm,
            Schema schema,
            long calculatedAtMillis) {
        GenericRecord payload = (GenericRecord) event.get("payload");
        Object sourceEventId = event.get("source_event_id");
        double geospatialScore = GeoScore.of(distanceKm, radiusKm);

        GenericRecord out = new GenericData.Record(schema);
        out.put("impact_id", sourceEventId + "::" + facility.facilityId);
        out.put("event_id", event.get("event_id"));
        out.put("source", event.get("source"));
        out.put("source_event_id", sourceEventId);
        out.put("event_type", event.get("event_type"));
        out.put("event_time", event.get("event_time"));
        out.put("asset_type", ASSET_TYPE_FACILITY);
        out.put("asset_id", facility.facilityId);
        out.put("supplier_id", facility.supplierId);
        out.put("event_latitude", payload.get("latitude"));
        out.put("event_longitude", payload.get("longitude"));
        out.put("asset_latitude", facility.latitude);
        out.put("asset_longitude", facility.longitude);
        out.put("distance_km", distanceKm);
        out.put("radius_km", radiusKm);
        out.put("inside_affected_area", distanceKm <= radiusKm);
        out.put("severity", payload.get("severity"));
        out.put("geospatial_score", geospatialScore);
        out.put("trace_id", event.get("trace_id"));
        out.put("tenant_id", event.get("tenant_id"));
        out.put("calculated_at", calculatedAtMillis);
        return out;
    }
}
