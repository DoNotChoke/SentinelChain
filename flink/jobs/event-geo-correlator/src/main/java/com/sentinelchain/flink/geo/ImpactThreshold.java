package com.sentinelchain.flink.geo;

import java.util.OptionalDouble;

/**
 * The impact radius rules (PLAN §11.4), kept as a pure function so they are unit-testable.
 *
 * <p>MVP covers earthquakes (the only live source): magnitude ≥7 → 250 km, ≥6 → 120 km, ≥5 → 50 km;
 * below 5, or with no reported magnitude, there is no candidate. Wildfire/flood radii are wired here
 * as {@code TODO} for when those sources land, so the correlator itself stays source-agnostic.
 */
public final class ImpactThreshold {

    public static final String EARTHQUAKE = "earthquake";

    private ImpactThreshold() {}

    /**
     * The radius within which {@code eventType}/{@code severity} can affect an asset, or empty when
     * no rule applies (too weak, or unknown severity).
     */
    public static OptionalDouble radiusKm(String eventType, Double severity) {
        if (EARTHQUAKE.equals(eventType)) {
            return earthquakeRadiusKm(severity);
        }
        // Other event types (wildfire, flood, …) are added with their sources.
        return OptionalDouble.empty();
    }

    private static OptionalDouble earthquakeRadiusKm(Double magnitude) {
        if (magnitude == null) {
            return OptionalDouble.empty();
        }
        if (magnitude >= 7.0) {
            return OptionalDouble.of(250.0);
        }
        if (magnitude >= 6.0) {
            return OptionalDouble.of(120.0);
        }
        if (magnitude >= 5.0) {
            return OptionalDouble.of(50.0);
        }
        return OptionalDouble.empty();
    }
}
