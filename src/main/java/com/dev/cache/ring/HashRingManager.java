package com.dev.cache.ring;

import com.dev.cache.config.CacheProperties;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Component
public class HashRingManager {

    private final CacheProperties props;
    private volatile HashRing ring;

    public HashRingManager(CacheProperties props) {
        this.props = props;
        this.ring = new HashRing(List.of(), props.getRing().getVnodesPerNode());
    }

    public void rebuild(Collection<String> aliveNodeIds) {
        this.ring = new HashRing(aliveNodeIds, props.getRing().getVnodesPerNode());
    }

    public String primaryFor(String key) {
        return ring.primaryFor(key);
    }

    public List<String> replicaSet(String key) {
        return ring.replicaSet(key, props.getReplication().getFactor());
    }

    public boolean isEmpty() {
        return ring.isEmpty();
    }
}
