package com.sentinelchain.flink.common.config;

import java.io.Serializable;
import org.apache.flink.api.java.utils.ParameterTool;


public final class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String consumerGroupId;

    private JobConfig(String bootstrapServers, String schemaRegistryUrl, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.consumerGroupId = consumerGroupId;
    }

    public static JobConfig from(ParameterTool params, String defaultConsumerGroupId) {
        return new JobConfig(
                resolve(params, "bootstrap-servers", "KAFKA_BOOTSTRAP_INTERNAL", "kafka:29092"),
                resolve(params, "schema-registry-url", "SCHEMA_REGISTRY_URL",
                        "http://schema-registry:8081"),
                resolve(params, "consumer-group-id", "CONSUMER_GROUP_ID", defaultConsumerGroupId));
    }

    private static String resolve(ParameterTool params, String flag, String env, String fallback) {
        if (params.has(flag)) {
            return params.get(flag);
        }
        String fromEnv = System.getenv(env);
        return fromEnv != null && !fromEnv.isBlank() ? fromEnv : fallback;
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public String schemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }
}
