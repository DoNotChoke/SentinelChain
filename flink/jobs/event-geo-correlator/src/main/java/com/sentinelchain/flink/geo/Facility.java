package com.sentinelchain.flink.geo;

/**
 * The slice of facility current-state the geo-correlator needs: identity, location, owning
 * supplier and lifecycle status.
 *
 * <p>A plain Flink POJO (public, no-arg constructor, public fields) so it is stored efficiently in
 * broadcast state. Sourced from {@code ops.facilities.current.v1} (Job 3).
 */
public class Facility {

    public String facilityId;
    public String supplierId;
    public double latitude;
    public double longitude;
    public String status;

    public Facility() {}

    public Facility(
            String facilityId,
            String supplierId,
            double latitude,
            double longitude,
            String status) {
        this.facilityId = facilityId;
        this.supplierId = supplierId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.status = status;
    }
}
