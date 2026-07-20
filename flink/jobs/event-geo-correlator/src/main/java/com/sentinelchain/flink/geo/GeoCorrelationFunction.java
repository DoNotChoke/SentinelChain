package com.sentinelchain.flink.geo;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import com.sentinelchain.flink.common.geo.Haversine;
import java.util.Map;
import java.util.OptionalDouble;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Correlates each deduplicated event against the facility current-state held in broadcast state,
 * emitting one {@code risk.impact_candidates.v1} record per facility inside the event's impact
 * radius (PLAN §11.4).
 *
 * <p>The facility side is a compacted changelog broadcast to every parallel instance; the event
 * side is the high-volume stream. For each event we look up the threshold radius for its
 * type/severity ({@link ImpactThreshold}), then Haversine-scan the facilities. Facilities seed from
 * the earliest offset of the compacted topic, so in the demo (facilities seeded, then the quake
 * injected) they are present before the event — a genuinely-late facility update simply affects
 * later events, the known trade-off of a broadcast join.
 */
public class GeoCorrelationFunction
        extends BroadcastProcessFunction<GenericRecord, FacilityUpdate, GenericRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * Broadcast state descriptor, shared by {@code broadcast(...)} in the job and the reads/writes
     * here — must be the same descriptor on both sides.
     */
    public static final MapStateDescriptor<String, Facility> FACILITY_STATE =
            new MapStateDescriptor<>(
                    "facilities", Types.STRING, TypeInformation.of(Facility.class));

    private final String impactTopic;

    private transient Schema impactSchema;
    private transient Counter impactsEmitted;
    private transient Counter eventsCorrelated;

    public GeoCorrelationFunction(String impactTopic) {
        this.impactTopic = impactTopic;
    }

    @Override
    public void open(Configuration parameters) {
        impactSchema = AvroSchemas.forTopic(impactTopic);
        var metrics = getRuntimeContext().getMetricGroup();
        impactsEmitted = metrics.counter("geo_impacts_emitted");
        eventsCorrelated = metrics.counter("geo_events_correlated");
    }

    /** Facility changelog: upsert or delete the broadcast entry. */
    @Override
    public void processBroadcastElement(
            FacilityUpdate update, Context ctx, Collector<GenericRecord> out) throws Exception {
        BroadcastState<String, Facility> state = ctx.getBroadcastState(FACILITY_STATE);
        if (update.deleted) {
            state.remove(update.facilityId);
        } else {
            state.put(update.facilityId, update.toFacility());
        }
    }

    /** Event: emit a candidate for every facility within the impact radius. */
    @Override
    public void processElement(GenericRecord event, ReadOnlyContext ctx, Collector<GenericRecord> out)
            throws Exception {
        String eventType = String.valueOf(event.get("event_type"));
        GenericRecord payload = (GenericRecord) event.get("payload");
        Double severity = nullableDouble(payload.get("severity"));

        OptionalDouble radius = ImpactThreshold.radiusKm(eventType, severity);
        if (radius.isEmpty()) {
            return; // event too weak / no rule → no candidates
        }
        double radiusKm = radius.getAsDouble();
        double eventLat = asDouble(payload.get("latitude"));
        double eventLon = asDouble(payload.get("longitude"));
        long now = ctx.currentProcessingTime();

        ReadOnlyBroadcastState<String, Facility> facilities = ctx.getBroadcastState(FACILITY_STATE);
        for (Map.Entry<String, Facility> entry : facilities.immutableEntries()) {
            Facility facility = entry.getValue();
            double distanceKm =
                    Haversine.km(eventLat, eventLon, facility.latitude, facility.longitude);
            if (distanceKm <= radiusKm) {
                out.collect(
                        ImpactCandidates.build(
                                event, facility, distanceKm, radiusKm, impactSchema, now));
                impactsEmitted.inc();
            }
        }
        eventsCorrelated.inc();
    }

    private static double asDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    private static Double nullableDouble(Object value) {
        return value == null ? null : ((Number) value).doubleValue();
    }
}
