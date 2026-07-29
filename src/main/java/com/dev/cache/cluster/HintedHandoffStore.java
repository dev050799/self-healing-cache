package com.dev.cache.cluster;

import com.dev.cache.model.CacheEntry;
import com.dev.cache.transport.InternodeClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class HintedHandoffStore {

    private final MembershipService membership;
    private final InternodeClient client;

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, CacheEntry>> hints =
            new ConcurrentHashMap<>();

    public HintedHandoffStore(MembershipService membership, InternodeClient client) {
        this.membership = membership;
        this.client = client;
    }

    public void store(String targetNodeId, String key, CacheEntry entry) {
        hints.computeIfAbsent(targetNodeId, k -> new ConcurrentHashMap<>()).put(key, entry);
    }

    public int pendingFor(String nodeId) {
        var m = hints.get(nodeId);
        return m == null ? 0 : m.size();
    }

    @Scheduled(fixedDelayString = "${cache.gossip.period-ms:1000}")
    public void replay() {
        for (var targetEntry : hints.entrySet()) {
            String targetId = targetEntry.getKey();
            if (!membership.isAlive(targetId)) {
                continue;
            }
            String baseUrl = membership.baseUrlOf(targetId);
            if (baseUrl == null) {
                continue;
            }

            Map<String, CacheEntry> pending = targetEntry.getValue();
            for (var e : pending.entrySet()) {
                try {
                    client.replicate(baseUrl, e.getKey(), e.getValue());
                    pending.remove(e.getKey(), e.getValue());
                } catch (Exception ex) {
                    break;
                }
            }
            if (pending.isEmpty()) {
                hints.remove(targetId, pending);
                log.info("Replayed all hints to recovered node {}", targetId);
            }
        }
    }
}
