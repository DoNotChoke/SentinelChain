package com.sentinelchain.flink.common.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;


public final class ConfluentAvroSerde {
    private ConfluentAvroSerde() {};

    public static DeserializationSchema<GenericRecord> valueDeserializer (
        Schema readerSchema, String schemaRegistryUrl
    ) {
        return ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, schemaRegistryUrl);
    }

    public static SerializationSchema<GenericRecord> valueSerializer(
            String topic, Schema schema, String schemaRegistryUrl
    ) {
        return ConfluentRegistryAvroSerializationSchema.forGeneric(
                topic + "-value", schema, schemaRegistryUrl);
    }
}