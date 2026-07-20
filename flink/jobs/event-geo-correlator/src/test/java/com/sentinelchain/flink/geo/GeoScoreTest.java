package com.sentinelchain.flink.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class GeoScoreTest {

    @Test
    void oneAtTheEpicentre() {
        assertEquals(1.0, GeoScore.of(0.0, 50.0), 1e-9);
    }

    @Test
    void zeroAtTheEdge() {
        assertEquals(0.0, GeoScore.of(50.0, 50.0), 1e-9);
    }

    @Test
    void halfwayIsAHalf() {
        assertEquals(0.5, GeoScore.of(25.0, 50.0), 1e-9);
    }

    @Test
    void clampedToZeroBeyondTheRadius() {
        assertEquals(0.0, GeoScore.of(80.0, 50.0), 1e-9);
    }

    @Test
    void nonPositiveRadiusScoresZero() {
        assertEquals(0.0, GeoScore.of(10.0, 0.0), 1e-9);
    }
}
