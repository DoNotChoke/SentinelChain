package com.sentinelchain.flink.common.geo;

/**
 * Great-circle distance between two WGS84 points via the Haversine formula (PLAN §11.4).
 *
 * <p>The mean Earth radius (6371.0088 km) makes this an approximation — good to well under 1% for
 * the tens-to-hundreds-of-km ranges the geo-correlator cares about, and far cheaper than a full
 * geodesic. Distances are symmetric and in kilometres.
 */
public final class Haversine {

    /** IUGG mean Earth radius, in kilometres. */
    private static final double EARTH_RADIUS_KM = 6371.0088;

    private Haversine() {}

    public static double km(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double sinLat = Math.sin(dLat / 2);
        double sinLon = Math.sin(dLon / 2);
        double a =
                sinLat * sinLat
                        + Math.cos(Math.toRadians(lat1))
                                * Math.cos(Math.toRadians(lat2))
                                * sinLon * sinLon;
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
