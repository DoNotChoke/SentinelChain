package com.sentinelchain.flink.common.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HaversineTest {

    @Test
    void zeroDistanceForSamePoint() {
        assertEquals(0.0, Haversine.km(35.68, 139.76, 35.68, 139.76), 1e-9);
    }

    @Test
    void tokyoToYokohamaIsAboutTwentyEightKm() {
        // Tokyo Station (35.681, 139.767) → Yokohama Station (35.466, 139.622): ~27–28 km.
        double d = Haversine.km(35.681, 139.767, 35.466, 139.622);
        assertEquals(27.5, d, 1.5);
    }

    @Test
    void oneDegreeOfLatitudeIsAboutOneEleven() {
        // A degree of latitude is ~111 km anywhere on the globe.
        assertEquals(111.19, Haversine.km(0.0, 0.0, 1.0, 0.0), 0.5);
    }

    @Test
    void isSymmetric() {
        double ab = Haversine.km(10.0, 20.0, 30.0, 40.0);
        double ba = Haversine.km(30.0, 40.0, 10.0, 20.0);
        assertEquals(ab, ba, 1e-9);
    }

    @Test
    void longitudeConvergesTowardThePoles() {
        // One degree of longitude spans less distance at 60°N than at the equator.
        double atEquator = Haversine.km(0.0, 0.0, 0.0, 1.0);
        double at60North = Haversine.km(60.0, 0.0, 60.0, 1.0);
        assertTrue(at60North < atEquator);
    }
}
