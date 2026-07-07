# Flink image for SentinelChain SQL jobs.
# Base flink:1.18-java17 already ships flink-json (debezium-json format); we add the Kafka
# SQL connector uber-jar so `connector = 'kafka' | 'upsert-kafka'` is available.
FROM flink:1.18-java17

ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    /opt/flink/lib/flink-sql-connector-kafka.jar
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka.jar
