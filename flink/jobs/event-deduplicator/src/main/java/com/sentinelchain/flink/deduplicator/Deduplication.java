package com.sentinelchain.flink.deduplicator;

/**
 * The dedup decision, kept as a pure function so it is unit-testable without Flink state (PLAN
 * §11.2).
 *
 * <p>Comparison is on the content hash ({@link PayloadHash}), not {@code source_version}: a pure
 * version bump carrying identical content is noise and is dropped, while any real content change
 * (including a status flip to {@code cancelled}) is re-emitted as an update.
 */
public final class Deduplication {

    /** What to do with an incoming event given the last content hash seen for its key. */
    public enum Decision {
        /** First time this key is seen — emit and remember it. */
        EMIT_NEW,
        /** Content identical to what was last emitted — drop (replay). */
        DROP,
        /** Content changed since last emit — emit the update and remember it. */
        EMIT_UPDATE
    }

    private Deduplication() {}

    /**
     * @param previousHash the last emitted content hash for this key, or {@code null} if unseen
     * @param incomingHash the incoming event's content hash
     */
    public static Decision decide(String previousHash, String incomingHash) {
        if (previousHash == null) {
            return Decision.EMIT_NEW;
        }
        return previousHash.equals(incomingHash) ? Decision.DROP : Decision.EMIT_UPDATE;
    }
}
