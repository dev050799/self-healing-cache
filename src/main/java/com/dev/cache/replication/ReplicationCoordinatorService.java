package com.dev.cache.replication;

import com.dev.cache.cluster.HintedHandoffStore;
import com.dev.cache.cluster.MembershipService;
import com.dev.cache.config.CacheProperties;
import com.dev.cache.model.CacheEntry;
import com.dev.cache.model.ConsistencyMode;
import com.dev.cache.ring.HashRingManager;
import com.dev.cache.store.StorageEngine;
import com.dev.cache.transport.InternodeClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@Service
@Slf4j
public class ReplicationCoordinatorService {

    private static final long TOMBSTONE_GRACE_MS = 10 * 60 * 1000L;

    private final CacheProperties props;
    private final HashRingManager ring;
    private final MembershipService membership;
    private final StorageEngine storage;
    private final InternodeClient client;
    private final HintedHandoffStore hints;

    public ReplicationCoordinatorService(CacheProperties props, HashRingManager ring,
                                         MembershipService membership, StorageEngine storage,
                                         InternodeClient client, HintedHandoffStore hints) {
        this.props = props;
        this.ring = ring;
        this.membership = membership;
        this.storage = storage;
        this.client = client;
        this.hints = hints;
    }

    public void put(String key, String value, long ttl, ConsistencyMode mode) {
        long now = System.currentTimeMillis();
        CacheEntry entry = CacheEntry.of(value, now + ttl * 1000, now, membership.selfId());
        write(key, entry, mode);
    }

    public void delete(String key, ConsistencyMode mode) {
        long now = System.currentTimeMillis();
        CacheEntry tombstone = CacheEntry.tombstone(now + TOMBSTONE_GRACE_MS, now, membership.selfId());
        write(key, tombstone, mode);
    }

    private void write(String key, CacheEntry entry, ConsistencyMode consistencyMode) {
        List<String> replicas = ring.replicaSet(key);
        if (replicas.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "No nodes available");
        }
        int required = (consistencyMode == ConsistencyMode.QUORUM) ? props.getReplication().getWriteQuorum() : 1;
        int acks = 0;

        for (String nodeId : replicas) {
            try {
                if (nodeId.equals(membership.selfId())) {
                    storage.apply(key, entry);
                    acks++;
                } else {
                    String baseUrl = membership.baseUrlOf(nodeId);
                    if (baseUrl == null) {
                        throw new IllegalStateException("unknown node " + nodeId);
                    }
                    client.replicate(baseUrl, key, entry);
                    acks++;
                }
            } catch (Exception e) {
                hints.store(nodeId, key, entry);
                log.debug("Write to replica {} failed, stored hint: {}", nodeId, e.getMessage());
            }
        }

        if (acks < required) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Only " + acks + " of " + required + " required acks");
        }
    }

    public Optional<String> get(String key, ConsistencyMode mode) {
        List<String> replicas = ring.replicaSet(key);
        if (replicas.isEmpty()) return Optional.empty();
        return (mode == ConsistencyMode.QUORUM) ? quorumRead(key, replicas) : eventualRead(key, replicas);
    }

    private Optional<String> eventualRead(String key, List<String> replicas) {
        int budget = props.getClient().getMaxRetries();
        int attempts = 0;
        for (String nodeId : replicas) {
            if (attempts >= budget)
                break;
            attempts++;
            try {
                CacheEntry e = readFrom(nodeId, key);
                if (e != null) return Optional.of(e.value());
            } catch (Exception ex) {
                log.debug("Read from {} failed, trying next replica", nodeId);
            }
        }
        return Optional.empty();
    }

    private Optional<String> quorumRead(String key, List<String> replicas) {
        int needed = props.getReplication().getReadQuorum();
        List<NodeReply> replies = new ArrayList<>();

        for (String nodeId : replicas) {
            try {
                CacheEntry raw = readRawFrom(nodeId, key);
                replies.add(new NodeReply(nodeId, raw));
            } catch (Exception ex) {
                log.error("Error Occurred: {}", ex.getMessage());
            }
        }
        if (replies.size() < needed) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Read Quorum not met: " + replies.size() + "/" + needed);
        }

        CacheEntry winner = replies.stream()
                .map(NodeReply::entry)
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(CacheEntry::version)
                        .thenComparing(e -> e.originNodeId() == null ? "" : e.originNodeId()))
                .orElse(null);

        readRepair(key, winner, replies);

        long now = System.currentTimeMillis();
        if (winner == null || winner.tombstone() || winner.isExpired(now))
            return Optional.empty();
        return Optional.of(winner.value());
    }

    private void readRepair(String key, CacheEntry winner, List<NodeReply> replies) {
        if (winner == null) return;
        for (NodeReply r : replies) {
            boolean stale = r.entry() == null || r.entry().version() < winner.version();
            if (!stale || r.nodeId().equals(membership.selfId())) {
                if (r.nodeId().equals(membership.selfId())) {
                    storage.apply(key, winner);
                }
                continue;
            }
            try {
                String baseUrl = membership.baseUrlOf(r.nodeId());
                if (baseUrl != null) {
                    client.replicate(baseUrl, key, winner);
                }
            } catch (Exception exception) {
                hints.store(r.nodeId(), key, winner);
            }
        }
    }

    private CacheEntry readFrom(String nodeId, String key) {
        if (nodeId.equals(membership.selfId())) return storage.getRaw(key);
        String baseUrl = membership.baseUrlOf(nodeId);
        if (baseUrl == null) throw new IllegalStateException("unknown node " + nodeId);
        return client.read(baseUrl, key);
    }

    private CacheEntry readRawFrom(String nodeId, String key) {
        if (nodeId.equals(membership.selfId())) return storage.getRaw(key);
        String baseUrl = membership.baseUrlOf(nodeId);
        if (baseUrl == null) throw new IllegalStateException("unknown node " + nodeId);
        return client.readRaw(baseUrl, key);
    }

    private record NodeReply(String nodeId, CacheEntry entry) {
    }
}
