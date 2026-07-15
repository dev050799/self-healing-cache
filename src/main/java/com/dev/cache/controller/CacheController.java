package com.dev.cache.controller;

import com.dev.cache.config.CacheProperties;
import com.dev.cache.model.ConsistencyMode;
import com.dev.cache.replication.ReplicationCoordinatorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/cache")
public class CacheController {

    private final ReplicationCoordinatorService coordinator;
    private final CacheProperties props;

    public CacheController(ReplicationCoordinatorService coordinator, CacheProperties props) {
        this.coordinator = coordinator;
        this.props = props;
    }

    @PutMapping("/{key}")
    public ResponseEntity<String> put(@PathVariable String key,
                                      @RequestBody(required = false) String value,
                                      @RequestParam(defaultValue = "3600") long ttl,
                                      @RequestParam(required = false) ConsistencyMode consistencyMode) {
        coordinator.put(key, value == null ? "" : value, ttl, mode(consistencyMode));
        return ResponseEntity.ok("OK");
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key,
                                      @RequestParam(required = false) ConsistencyMode consistencyMode) {
        Optional<String> value = coordinator.get(key, mode(consistencyMode));
        return value.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key,
                                         @RequestParam(required = false) ConsistencyMode consistencyMode) {
        coordinator.delete(key, mode(consistencyMode));
        return ResponseEntity.ok("OK");
    }

    private ConsistencyMode mode(ConsistencyMode consistencyMode) {
        return consistencyMode != null ? consistencyMode : props.getReplication().getConsistency();
    }
}
