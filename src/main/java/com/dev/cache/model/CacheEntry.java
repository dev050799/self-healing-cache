package com.dev.cache.model;

public record CacheEntry(
        String value,
        long expiresAtEpochMs,
        long version,
        String originNodeId,
        boolean tombstone) {

    public boolean isExpired(long nowMs) {
        return nowMs >= expiresAtEpochMs;
    }

    public static CacheEntry of(String value, long expiresAtEpochMs, long version, String originNodeId) {
        return new CacheEntry(value, expiresAtEpochMs, version, originNodeId, false);
    }

    public static CacheEntry tombstone(long expiresAtEpochMs, long version, String originNodeId) {
        return new CacheEntry(null, expiresAtEpochMs, version, originNodeId, true);
    }
}
