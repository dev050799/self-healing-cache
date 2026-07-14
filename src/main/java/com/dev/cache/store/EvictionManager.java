package com.dev.cache.store;

import com.dev.cache.config.CacheProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

@Component
public class EvictionManager {

    private final CacheProperties props;
    private final LinkedHashMap<String, Boolean> recency =
            new LinkedHashMap<>(1024, .75f, true);

    public EvictionManager(CacheProperties props) {
        this.props = props;
    }

    public synchronized void recordAccess(String key) {
        recency.put(key, Boolean.TRUE);
    }

    public synchronized void remove(String key) {
        recency.remove(key);
    }

    public synchronized List<String> selectVictims(int currentSize) {
        int max = props.getEviction().getMaxEntries();
        if (props.getEviction().getPolicy() == CacheProperties.Eviction.Policy.NONE || max <= 0 || currentSize <= max) {
            return List.of();
        }
        int toEvict = currentSize - max;
        List<String> victims = new ArrayList<>(toEvict);
        Iterator<String> eldestFirst = recency.keySet().iterator();
        while (eldestFirst.hasNext() && victims.size() < toEvict) {
            victims.add(eldestFirst.next());
        }
        return victims;
    }
}
