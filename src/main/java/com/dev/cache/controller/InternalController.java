package com.dev.cache.controller;

import com.dev.cache.cluster.MembershipService;
import com.dev.cache.model.CacheEntry;
import com.dev.cache.model.Member;
import com.dev.cache.store.StorageEngine;
import com.dev.cache.transport.InternodeClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/internal")
public class InternalController {

    private final StorageEngine storage;
    private final MembershipService membership;
    private final InternodeClient client;


    public InternalController(StorageEngine storage, MembershipService membership, InternodeClient client) {
        this.storage = storage;
        this.membership = membership;
        this.client = client;
    }

    @PostMapping("/replicate/{key}")
    public boolean replicate(@PathVariable String key, @RequestBody CacheEntry entry) {
        return storage.apply(key, entry);
    }

    @GetMapping("/read/{key}")
    public ResponseEntity<CacheEntry> read(@PathVariable String key) {
        CacheEntry e = storage.get(key);
        return e == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(e);
    }

    @GetMapping("/read-raw/{key}")
    public ResponseEntity<CacheEntry> readRaw(@PathVariable String key) {
        CacheEntry e = storage.getRaw(key);
        return e == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(e);
    }

    @GetMapping("/ping")
    public String ping() {
        return "ping";
    }

    @PostMapping("/ping-req")
    public boolean pingReq(@RequestParam String targetUrl) {
        try {
            client.ping(targetUrl);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    @PostMapping("/gossip")
    public List<Member> gossip(@RequestBody List<Member> incoming) {
        membership.merge(incoming);
        return membership.allMembers();
    }
}
