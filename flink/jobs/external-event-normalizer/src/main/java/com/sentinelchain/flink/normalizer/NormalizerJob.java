package com.sentinelchain.flink.normalizer;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * Flink Job 1 — external-event-normalizer
 */
public final class NormalizerJob {

    static final String RAW_TOPIC = "ext.usgs.raw.v1";
    static final String NORMALIZED_TOPIC = "events.normalized.v1";
    static final String AUDIT_TOPIC = "audit.data_quality.v1";
    static final String CONSUMER_GROUP = "flink-external-event-normalizer";

    /** Late-event tolerance for the event-time watermark. */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofMinutes(5);

    private NormalizerJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(ParameterTool.fromArgs(args), CONSUMER_GROUP);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Schema rawSchema = AvroSchemas.forTopic(RAW_TOPIC);
        Schema normalizedSchema = AvroSchemas.forTopic(NORMALIZED_TOPIC);
        Schema auditSchema = AvroSchemas.forTopic(AUDIT_TOPIC);

        KafkaSource<GenericRecord> source =
                KafkaSource.<GenericRecord>builder()
                        .setBootstrapServers(cfg.bootstrapServers())
                        .setTopics(RAW_TOPIC)
                        .setGroupId(cfg.consumerGroupId())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(
                                ConfluentAvroSerde.valueDeserializer(
                                        rawSchema, cfg.schemaRegistryUrl()))
                        .build();

        WatermarkStrategy<GenericRecord> watermarks =
                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                (record, ts) -> ((Number) record.get("event_time")).longValue())
                        .withIdleness(Duration.ofMinutes(1));

        OutputTag<GenericRecord> auditTag =
                new OutputTag<>("data-quality", new GenericRecordAvroTypeInfo(auditSchema));

        SingleOutputStreamOperator<GenericRecord> normalized =
                env.fromSource(source, watermarks, "ext.usgs.raw.v1")
                        .process(new NormalizeFunction(NORMALIZED_TOPIC, AUDIT_TOPIC, auditTag))
                        .name("normalize-usgs")
                        .returns(new GenericRecordAvroTypeInfo(normalizedSchema));

        normalized
                .sinkTo(kafkaSink(NORMALIZED_TOPIC, normalizedSchema, cfg))
                .name("sink-events.normalized.v1");
        normalized
                .getSideOutput(auditTag)
                .sinkTo(kafkaSink(AUDIT_TOPIC, auditSchema, cfg))
                .name("sink-audit.data_quality.v1");

        env.execute("external-event-normalizer");
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
