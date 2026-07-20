package com.sentinelchain.flink.deduplicator;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful deduplicator keyed by {@code source + source_event_id} (PLAN §11.2, ADR-007).
 *
 * <p>Holds one {@link DedupState} per key in Flink managed state and applies {@link Deduplication}:
 * emit the first sighting, drop exact replays, re-emit real content changes. State expires after a
 * configurable TTL so keys for events no longer updated upstream do not accumulate forever —
 * checkpointed, so the guard survives a restart (a replay after recovery is still dropped).
 *
 * <p>Metrics (PLAN §26): counters for emitted-new, emitted-update, and dropped-duplicate.
 */
public class DeduplicateFunction
        extends KeyedProcessFunction<String, GenericRecord, GenericRecord> {

    private static final long serialVersionUID = 1L;

    private final long stateTtlMillis;

    private transient ValueState<DedupState> state;
    private transient Counter emittedNew;
    private transient Counter emittedUpdate;
    private transient Counter droppedDuplicate;

    public DeduplicateFunction(long stateTtlMillis) {
        this.stateTtlMillis = stateTtlMillis;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<DedupState> descriptor =
                new ValueStateDescriptor<>("dedup-state", DedupState.class);
        StateTtlConfig ttl =
                StateTtlConfig.newBuilder(Time.milliseconds(stateTtlMillis))
                        // Refresh the TTL whenever the key is touched, so hot keys stay alive.
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
        descriptor.enableTimeToLive(ttl);
        state = getRuntimeContext().getState(descriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        emittedNew = metrics.counter("dedup_emitted_new");
        emittedUpdate = metrics.counter("dedup_emitted_update");
        droppedDuplicate = metrics.counter("dedup_dropped_duplicate");
    }

    @Override
    public void processElement(GenericRecord event, Context ctx, Collector<GenericRecord> out)
            throws Exception {
        String incomingHash = PayloadHash.of(event);
        DedupState previous = state.value();
        String previousHash = previous == null ? null : previous.payloadHash;

        long now = ctx.timerService().currentProcessingTime();

        switch (Deduplication.decide(previousHash, incomingHash)) {
            case EMIT_NEW -> {
                out.collect(event);
                remember(event, incomingHash, now);
                emittedNew.inc();
            }
            case EMIT_UPDATE -> {
                out.collect(event);
                remember(event, incomingHash, now);
                emittedUpdate.inc();
            }
            case DROP -> {
                // Touch state so the replay refreshes the TTL but nothing is emitted downstream.
                previous.lastSeenAt = now;
                state.update(previous);
                droppedDuplicate.inc();
            }
        }
    }

    private void remember(GenericRecord event, String hash, long now) throws Exception {
        Object sourceVersion = event.get("source_version");
        state.update(
                new DedupState(hash, sourceVersion == null ? null : sourceVersion.toString(), now));
    }
}
