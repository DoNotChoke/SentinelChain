package com.sentinelchain.flink.common.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;


public final class AvroSchemas {
    private AvroSchemas() {}

    public static Schema forTopic(String topic) {
        String resource = "/avro/" + topic + ".avsc";
        try (InputStream in = AvroSchemas.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException(
                        "no Avro schema on classpath for topic '" + topic + "' (expected "
                                + resource + "). Schemas live in schemas/avro (ADR-001).");
            }
            return new Schema.Parser().parse(in);
        } catch(IOException e) {
            throw new UncheckedIOException("failed reading Avro schema " + resource, e);
        }
    }
}