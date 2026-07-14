package com.dev.cache.ring;

import com.dev.cache.config.CacheProperties;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Component
public class HashRingManager {

    private final CacheProperties properties;
    private volatile HashRing hashRing;

    public HashRingManager(CacheProperties properties) {
        this.properties = properties;
        this.hashRing = new HashRing(List.of(), properties.getRing().getVnodesPerNode());
    }

    public void rebuild(Collection<String> aliveNodeIds) {
        this.hashRing = new HashRing(aliveNodeIds, properties.getRing().getVnodesPerNode());
    }

    public String primaryFor(String key) {
        return hashRing.primaryFor(key);
    }

    public List<String> replicaSet(String key) {
        return hashRing.replicaSet(key, properties.getReplication().getFactor());
    }

    public boolean isEmpty() {
        return hashRing.isEmpty();
    }
}
