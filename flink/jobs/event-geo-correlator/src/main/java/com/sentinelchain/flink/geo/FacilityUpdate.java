package com.sentinelchain.flink.geo;

/**
 * A change to facility current-state read off {@code ops.facilities.current.v1}: either an upsert
 * (the current row) or a tombstone (the compacted-topic delete, {@code deleted = true}, only the id
 * is known).
 *
 * <p>Flink POJO so it flows through the broadcast stream with the efficient serializer.
 */
public class FacilityUpdate {

    public String facilityId;
    public String supplierId;
    public double latitude;
    public double longitude;
    public String status;
    public boolean deleted;

    public FacilityUpdate() {}

    public static FacilityUpdate upsert(
            String facilityId,
            String supplierId,
            double latitude,
            double longitude,
            String status) {
        FacilityUpdate u = new FacilityUpdate();
        u.facilityId = facilityId;
        u.supplierId = supplierId;
        u.latitude = latitude;
        u.longitude = longitude;
        u.status = status;
        u.deleted = false;
        return u;
    }

    public static FacilityUpdate tombstone(String facilityId) {
        FacilityUpdate u = new FacilityUpdate();
        u.facilityId = facilityId;
        u.deleted = true;
        return u;
    }

    public Facility toFacility() {
        return new Facility(facilityId, supplierId, latitude, longitude, status);
    }
}
