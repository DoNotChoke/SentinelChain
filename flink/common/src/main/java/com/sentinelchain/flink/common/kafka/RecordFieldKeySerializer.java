package com.sentinelchain.flink.common.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;


public final class RecordFieldKeySerializer implements SerializationSchema<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private final String field;

    public RecordFieldKeySerializer(String field) {
        this.field = field;
    }

    @Override
    public byte[] serialize(GenericRecord record) {
        Object value = record.get(field);
        if (value == null) {
            throw new IllegalArgumentException("key field '" + field + "' is null: " + record);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
