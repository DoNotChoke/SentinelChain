package com.sentinelchain.flink.deduplicator;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import com.sentinelchain.flink.common.avro.ConfluentAvroSerde;
import com.sentinelchain.flink.common.config.JobConfig;
import com.sentinelchain.flink.common.kafka.RecordFieldKeySerializer;
import java.time.Duration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Job 2 — event-deduplicator (PLAN §18 Job 2, §11.2).
 *
 * <p>Reads {@code events.normalized.v1}, keys by {@code source + source_event_id}, and re-emits to
 * {@code events.deduplicated.v1} only records whose content changed — dropping exact replays (Job 1
 * is at-least-once). State is keyed and TTL'd; being checkpointed, the guard survives a restart, so
 * a post-recovery replay is still dropped (the "no duplicate alert on replay" acceptance, ADR-007).
 *
 * <p>Overridable via {@code --bootstrap-servers}, {@code --schema-registry-url},
 * {@code --consumer-group-id}, {@code --state-ttl-days} (env {@code DEDUP_STATE_TTL_DAYS}).
 */
public final class DeduplicatorJob {

    static final String INPUT_TOPIC = "events.normalized.v1";
    static final String OUTPUT_TOPIC = "events.deduplicated.v1";
    static final String CONSUMER_GROUP = "flink-event-deduplicator";

    /** Default state TTL — matches raw-topic retention (PLAN §10.1), so a key lives as long as its source events could be replayed. */
    private static final long DEFAULT_STATE_TTL_DAYS = 30;

    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofMinutes(5);

    private DeduplicatorJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        JobConfig cfg = JobConfig.from(params, CONSUMER_GROUP);
        long ttlMillis = Duration.ofDays(resolveTtlDays(params)).toMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Schema inputSchema = AvroSchemas.forTopic(INPUT_TOPIC);
        Schema outputSchema = AvroSchemas.forTopic(OUTPUT_TOPIC);

        KafkaSource<GenericRecord> source =
                KafkaSource.<GenericRecord>builder()
                        .setBootstrapServers(cfg.bootstrapServers())
                        .setTopics(INPUT_TOPIC)
                        .setGroupId(cfg.consumerGroupId())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(
                                ConfluentAvroSerde.valueDeserializer(
                                        inputSchema, cfg.schemaRegistryUrl()))
                        .build();

        WatermarkStrategy<GenericRecord> watermarks =
                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                (record, ts) -> ((Number) record.get("event_time")).longValue())
                        .withIdleness(Duration.ofMinutes(1));

        env.fromSource(source, watermarks, "events.normalized.v1")
                .keyBy(dedupKey())
                .process(new DeduplicateFunction(ttlMillis))
                .name("deduplicate")
                .returns(new GenericRecordAvroTypeInfo(outputSchema))
                .sinkTo(kafkaSink(OUTPUT_TOPIC, outputSchema, cfg))
                .name("sink-events.deduplicated.v1");

        env.execute("event-deduplicator");
    }

    /** Dedup key = source + ":" + source_event_id (PLAN §11.2). */
    private static KeySelector<GenericRecord, String> dedupKey() {
        return event -> event.get("source") + ":" + event.get("source_event_id");
    }

    private static long resolveTtlDays(ParameterTool params) {
        if (params.has("state-ttl-days")) {
            return params.getLong("state-ttl-days");
        }
        String env = System.getenv("DEDUP_STATE_TTL_DAYS");
        return env != null && !env.isBlank() ? Long.parseLong(env) : DEFAULT_STATE_TTL_DAYS;
    }

    private static KafkaSink<GenericRecord> kafkaSink(String topic, Schema schema, JobConfig cfg) {
        return KafkaSink.<GenericRecord>builder()
                .setBootstrapServers(cfg.bootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<GenericRecord>builder()
                                .setTopic(topic)
                                .setKeySerializationSchema(
                                        new RecordFieldKeySerializer("source_event_id"))
                                .setValueSerializationSchema(
                                        ConfluentAvroSerde.valueSerializer(
                                                topic, schema, cfg.schemaRegistryUrl()))
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
