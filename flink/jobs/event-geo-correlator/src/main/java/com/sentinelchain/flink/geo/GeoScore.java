package com.sentinelchain.flink.geo;

/**
 * Proximity score for an impact candidate (PLAN §11.4 output {@code geospatial_score}).
 *
 * <p>A simple, explainable linear falloff: {@code 1} at the epicentre, {@code 0} at the threshold
 * edge, clamped to {@code [0, 1]}. Being monotonic in distance it feeds the risk score (§11.6) as
 * the {@code distance_score} factor without hiding a black box.
 */
public final class GeoScore {

    private GeoScore() {}

    public static double of(double distanceKm, double radiusKm) {
        if (radiusKm <= 0) {
            return 0.0;
        }
        double score = 1.0 - (distanceKm / radiusKm);
        return Math.max(0.0, Math.min(1.0, score));
    }
}
