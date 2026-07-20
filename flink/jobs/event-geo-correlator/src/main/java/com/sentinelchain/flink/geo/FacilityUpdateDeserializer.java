package com.sentinelchain.flink.geo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Decodes the JSON facility current-state topic ({@code ops.facilities.current.v1}, produced by
 * Job 3's {@code upsert-kafka} sink) into {@link FacilityUpdate}.
 *
 * <p>Needs the record's key and value together (not value-only), because a compacted delete arrives
 * as a null value with the id in the JSON key — that becomes a tombstone. The value JSON carries the
 * upsert. Flink's own Jackson is shaded away, so this uses its own {@link ObjectMapper}, created in
 * {@link #open} since the mapper is not part of the serializable closure.
 */
public class FacilityUpdateDeserializer
        implements KafkaRecordDeserializationSchema<FacilityUpdate> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<FacilityUpdate> out)
            throws java.io.IOException {
        String facilityId = readFacilityId(record);
        if (facilityId == null) {
            return; // unparseable key — nothing we can key state on
        }
        if (record.value() == null) {
            out.collect(FacilityUpdate.tombstone(facilityId));
            return;
        }
        JsonNode v = mapper.readTree(record.value());
        out.collect(
                FacilityUpdate.upsert(
                        facilityId,
                        text(v, "supplier_id"),
                        v.path("latitude").asDouble(),
                        v.path("longitude").asDouble(),
                        text(v, "status")));
    }

    private String readFacilityId(ConsumerRecord<byte[], byte[]> record) throws java.io.IOException {
        // upsert-kafka key.format = json → key is {"facility_id":"..."}. Fall back to the value if
        // the key is somehow absent.
        if (record.key() != null) {
            String fromKey = text(mapper.readTree(record.key()), "facility_id");
            if (fromKey != null) {
                return fromKey;
            }
        }
        return record.value() == null ? null : text(mapper.readTree(record.value()), "facility_id");
    }

    private static String text(JsonNode node, String field) {
        JsonNode f = node.get(field);
        return f == null || f.isNull() ? null : f.asText();
    }

    @Override
    public TypeInformation<FacilityUpdate> getProducedType() {
        return TypeInformation.of(FacilityUpdate.class);
    }
}
