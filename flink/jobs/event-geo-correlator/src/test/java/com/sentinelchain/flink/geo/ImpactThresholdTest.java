package com.sentinelchain.flink.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.OptionalDouble;
import org.junit.jupiter.api.Test;

class ImpactThresholdTest {

    @Test
    void earthquakeRadiiFollowTheMagnitudeBands() {
        assertEquals(250.0, ImpactThreshold.radiusKm("earthquake", 7.4).getAsDouble());
        assertEquals(250.0, ImpactThreshold.radiusKm("earthquake", 7.0).getAsDouble());
        assertEquals(120.0, ImpactThreshold.radiusKm("earthquake", 6.2).getAsDouble());
        assertEquals(50.0, ImpactThreshold.radiusKm("earthquake", 5.0).getAsDouble());
    }

    @Test
    void weakEarthquakeHasNoRule() {
        assertTrue(ImpactThreshold.radiusKm("earthquake", 4.9).isEmpty());
    }

    @Test
    void missingMagnitudeHasNoRule() {
        assertTrue(ImpactThreshold.radiusKm("earthquake", null).isEmpty());
    }

    @Test
    void unknownEventTypeHasNoRule() {
        OptionalDouble r = ImpactThreshold.radiusKm("wildfire", 0.9);
        assertTrue(r.isEmpty());
    }
}
