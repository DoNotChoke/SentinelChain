package com.sentinelchain.flink.deduplicator;

import java.util.Objects;

/**
 * Per-key deduplication state (PLAN §11.2): the last content hash emitted for a
 * {@code source + source_event_id}, the upstream version that produced it, and when it was last
 * seen.
 *
 * <p>A plain Flink POJO — public, public no-arg constructor, public fields — so Flink uses its
 * efficient {@code PojoSerializer} (and can evolve the state schema) rather than falling back to
 * Kryo. {@code sourceVersion} / {@code lastSeenAt} are not used in the decision itself but are kept
 * for observability and to leave room for an ordering guard later.
 */
public class DedupState {

    public String payloadHash;
    public String sourceVersion;
    public long lastSeenAt;

    public DedupState() {}

    public DedupState(String payloadHash, String sourceVersion, long lastSeenAt) {
        this.payloadHash = payloadHash;
        this.sourceVersion = sourceVersion;
        this.lastSeenAt = lastSeenAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DedupState other)) {
            return false;
        }
        return lastSeenAt == other.lastSeenAt
                && Objects.equals(payloadHash, other.payloadHash)
                && Objects.equals(sourceVersion, other.sourceVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payloadHash, sourceVersion, lastSeenAt);
    }
}
