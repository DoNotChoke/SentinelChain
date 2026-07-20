package com.sentinelchain.flink.normalizer;

import com.sentinelchain.flink.common.avro.AvroSchemas;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class NormalizeFunction extends ProcessFunction<GenericRecord, GenericRecord> {

    private static final long serialVersionUID = 1L;

    private final String normalizedTopic;
    private final String auditTopic;
    private final OutputTag<GenericRecord> auditTag;

    private transient Schema normalizedSchema;
    private transient Schema auditSchema;

    public NormalizeFunction(
            String normalizedTopic, String auditTopic, OutputTag<GenericRecord> auditTag) {
        this.normalizedTopic = normalizedTopic;
        this.auditTopic = auditTopic;
        this.auditTag = auditTag;
    }

    @Override
    public void open(Configuration parameters) {
        this.normalizedSchema = AvroSchemas.forTopic(normalizedTopic);
        this.auditSchema = AvroSchemas.forTopic(auditTopic);
    }

    @Override
    public void processElement(GenericRecord raw, Context ctx, Collector<GenericRecord> out) {
        long now = ctx.timerService().currentProcessingTime();
        List<String> reasons = UsgsNormalization.validate(raw, now);
        if (reasons.isEmpty()) {
            out.collect(UsgsNormalization.toNormalized(raw, normalizedSchema, now));
        } else {
            ctx.output(auditTag, UsgsNormalization.toViolation(raw, reasons, auditSchema, now));
        }
    }
}
