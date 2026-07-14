package com.dev.cache.ring;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class HashRing {

    private final NavigableMap<Long, String> ring = new TreeMap<>();
    private final int distinctNodes;

    public HashRing(Collection<String> nodeIds, int vnodesPerNode) {
        LinkedHashSet<String> distinctNodeIds = new LinkedHashSet<>(nodeIds);
        this.distinctNodes = distinctNodeIds.size();
        for (String id : distinctNodeIds) {
            for (int i = 0; i < vnodesPerNode; i++) {
                ring.put(hash(id + "#" + i), id);
            }
        }
    }

    public boolean isEmpty() {
        return ring.isEmpty();
    }

    public String primaryFor(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        var entry = ring.ceilingEntry(hash(key));
        return (entry != null ? entry : ring.firstEntry()).getValue();
    }

    public List<String> replicaSet(String key, int rf) {
        if (ring.isEmpty()) {
            return List.of();
        }

        long h = hash(key);
        int target = Math.min(rf, distinctNodes);
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String node : ring.tailMap(h, true).values()) {
            out.add(node);
            if (out.size() >= target) {
                return List.copyOf(out);
            }
        }
        for (String node : ring.values()) {
            out.add(node);
            if (out.size() >= target) {
                break;
            }
        }
        return List.copyOf(out);
    }

    static long hash(String key) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xffL);
            }
            return h;
        } catch (Exception e) {
            throw new IllegalStateException("Error occurred while hashing the key", e);
        }
    }
}
