package com.dev.cache.store;

import com.dev.cache.model.CacheEntry;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class StorageEngine {

    private final ConcurrentHashMap<String, CacheEntry> map = new ConcurrentHashMap<>();
    private final EvictionManager eviction;

    public StorageEngine(EvictionManager eviction) {
        this.eviction = eviction;
    }

    public CacheEntry get(String key) {
        CacheEntry e = map.get(key);
        if (e == null) {
            return null;
        }
        if (e.isExpired(System.currentTimeMillis())) {
            map.remove(key, e);
            eviction.remove(key);
            return null;
        }
        eviction.recordAccess(key);
        return e.tombstone() ? null : e;
    }

    public CacheEntry getRaw(String key) {
        return map.get(key);
    }

    public boolean apply(String key, CacheEntry incoming) {
        AtomicBoolean accepted = new AtomicBoolean(false);
        map.compute(key, (k, existing) -> {
            if (existing == null || isNewer(incoming, existing)) {
                accepted.set(true);
                return incoming;
            }
            return existing;
        });
        if (accepted.get()) {
            eviction.recordAccess(key);
            enforceEviction();
        }
        return accepted.get();
    }

    private boolean isNewer(CacheEntry a, CacheEntry b) {
        if (a.version() != b.version()) {
            return a.version() > b.version();
        }
        return originOf(a).compareTo(originOf(b)) > 0;
    }

    private String originOf(CacheEntry e) {
        return e.originNodeId() == null ? "" : e.originNodeId();
    }

    private void enforceEviction() {
        for (String victim : eviction.selectVictims(map.size())) {
            map.remove(victim);
            eviction.remove(victim);
        }
    }

    public int sweepExpired() {
        long now = System.currentTimeMillis();
        int removed = 0;
        for (Map.Entry<String, CacheEntry> e : map.entrySet()) {
            if (e.getValue().isExpired(now)) {
                if (map.remove(e.getKey(), e.getValue())) {
                    eviction.remove(e.getKey());
                    removed++;
                }
            }
        }
        return removed;
    }

    public Map<String, CacheEntry> snapshot() {
        return new HashMap<>(map);
    }

    public int size() {
        return map.size();
    }
}
