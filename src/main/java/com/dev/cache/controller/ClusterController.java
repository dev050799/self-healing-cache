package com.dev.cache.controller;

import com.dev.cache.cluster.MembershipService;
import com.dev.cache.config.CacheProperties;
import com.dev.cache.model.ConsistencyMode;
import com.dev.cache.model.Member;
import com.dev.cache.replication.ReplicationCoordinatorService;
import com.dev.cache.ring.HashRingManager;
import com.dev.cache.store.StorageEngine;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private final MembershipService membership;
    private final HashRingManager ring;
    private final StorageEngine storage;
    private final CacheProperties props;
    private final ReplicationCoordinatorService coordinator;

    public ClusterController(MembershipService membership, HashRingManager ring,
                             StorageEngine storage, CacheProperties props,
                             ReplicationCoordinatorService coordinator) {
        this.membership = membership;
        this.ring = ring;
        this.storage = storage;
        this.props = props;
        this.coordinator = coordinator;
    }

    @GetMapping("/members")
    public List<Member> members() {
        return membership.allMembers();
    }

    @GetMapping("/info")
    public Map<String, Object> info() {
        return Map.of(
                "nodeId", membership.selfId(),
                "aliveNodes", membership.aliveMembers().size(),
                "localKeys", storage.size(),
                "replicationFactor", props.getReplication().getFactor(),
                "consistency", props.getReplication().getConsistency().name());
    }

    @GetMapping("/owners/{key}")
    public ResponseEntity<Map<String, Object>> owners(@PathVariable String key) {
        Optional<String> value = coordinator.get(key, ConsistencyMode.EVENTUAL);
        if (value.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "key", key,
                            "errorMessage", "Key not found"));
        }
        return ResponseEntity.status(HttpStatus.OK).body(Map.of(
                "key", key,
                "replicationSet", ring.replicaSet(key)));
    }
}
