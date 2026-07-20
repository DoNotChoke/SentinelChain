package com.sentinelchain.flink.geo;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import com.sentinelchain.flink.common.avro.ConfluentAvroSerde;
import com.sentinelchain.flink.common.config.JobConfig;
import com.sentinelchain.flink.common.kafka.RecordFieldKeySerializer;
import java.time.Duration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Job 4 — event-geo-correlator (PLAN §18 Job 4, §11.4).
 *
 * <p>Joins {@code events.deduplicated.v1} against the facility current-state
 * ({@code ops.facilities.current.v1}, held in broadcast state) and emits one
 * {@code risk.impact_candidates.v1} record per facility within the event's impact radius.
 *
 * <p>Overridable via {@code --bootstrap-servers}, {@code --schema-registry-url},
 * {@code --consumer-group-id}. The two sources use distinct consumer groups derived from it.
 */
public final class GeoCorrelatorJob {

    static final String EVENTS_TOPIC = "events.deduplicated.v1";
    static final String FACILITIES_TOPIC = "ops.facilities.current.v1";
    static final String OUTPUT_TOPIC = "risk.impact_candidates.v1";
    static final String CONSUMER_GROUP = "flink-event-geo-correlator";

    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofMinutes(5);

    private GeoCorrelatorJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(ParameterTool.fromArgs(args), CONSUMER_GROUP);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Schema eventSchema = AvroSchemas.forTopic(EVENTS_TOPIC);
        Schema impactSchema = AvroSchemas.forTopic(OUTPUT_TOPIC);

        KafkaSource<GenericRecord> eventSource =
                KafkaSource.<GenericRecord>builder()
                        .setBootstrapServers(cfg.bootstrapServers())
                        .setTopics(EVENTS_TOPIC)
                        .setGroupId(cfg.consumerGroupId())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(
                                ConfluentAvroSerde.valueDeserializer(
                                        eventSchema, cfg.schemaRegistryUrl()))
                        .build();

        KafkaSource<FacilityUpdate> facilitySource =
                KafkaSource.<FacilityUpdate>builder()
                        .setBootstrapServers(cfg.bootstrapServers())
                        .setTopics(FACILITIES_TOPIC)
                        .setGroupId(cfg.consumerGroupId() + "-facilities")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(new FacilityUpdateDeserializer())
                        .build();

        WatermarkStrategy<GenericRecord> watermarks =
                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                (record, ts) -> ((Number) record.get("event_time")).longValue())
                        .withIdleness(Duration.ofMinutes(1));

        BroadcastStream<FacilityUpdate> facilities =
                env.fromSource(facilitySource, WatermarkStrategy.noWatermarks(), FACILITIES_TOPIC)
                        .broadcast(GeoCorrelationFunction.FACILITY_STATE);

        DataStream<GenericRecord> events =
                env.fromSource(eventSource, watermarks, EVENTS_TOPIC);

        events.connect(facilities)
                .process(new GeoCorrelationFunction(OUTPUT_TOPIC))
                .name("geo-correlate")
                .returns(new GenericRecordAvroTypeInfo(impactSchema))
                .sinkTo(kafkaSink(OUTPUT_TOPIC, impactSchema, cfg))
                .name("sink-risk.impact_candidates.v1");

        env.execute("event-geo-correlator");
    }

    private static KafkaSink<GenericRecord> kafkaSink(String topic, Schema schema, JobConfig cfg) {
        return KafkaSink.<GenericRecord>builder()
                .setBootstrapServers(cfg.bootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<GenericRecord>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(new RecordFieldKeySerializer("impact_id"))
                                .setValueSerializationSchema(
                                        ConfluentAvroSerde.valueSerializer(
                                                topic, schema, cfg.schemaRegistryUrl()))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
