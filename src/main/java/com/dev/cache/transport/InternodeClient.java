package com.dev.cache.transport;

import com.dev.cache.model.CacheEntry;
import com.dev.cache.model.Member;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

@Component
public class InternodeClient {

    private static final ParameterizedTypeReference<List<Member>> MEMEBER_LIST =
            new ParameterizedTypeReference<>() {
            };

    private final RestClient http;

    public InternodeClient(RestClient internodeRestClient) {
        this.http = internodeRestClient;
    }

    public boolean replicate(String baseUrl, String key, CacheEntry entry) {
        Boolean accepted = http.post()
                .uri(baseUrl + "/internal/replicate/{key}", key)
                .body(entry)
                .retrieve()
                .body(Boolean.class);
        return Boolean.TRUE.equals(accepted);
    }

    public CacheEntry read(String baseUrl, String key) {
        return http.get()
                .uri(baseUrl + "/internal/read/{key}", key)
                .retrieve()
                .onStatus(s -> s.value() == 404, (req, res) -> {
                })
                .body(CacheEntry.class);
    }

    public CacheEntry readRaw(String baseUrl, String key) {
        return http.get()
                .uri(baseUrl + "/internal/read-raw/{key}", key)
                .retrieve()
                .onStatus(s -> s.value() == 404, (req, res) -> {
                })
                .body(CacheEntry.class);
    }

    public void ping(String baseUrl) {
        http.get().uri(baseUrl + "/internal/ping").retrieve().toBodilessEntity();
    }

    public boolean pingReq(String baseUrl, String targetUrl) {
        Boolean alive = http.post().uri(baseUrl + "/internal/ping-req?targetUrl={u}", targetUrl)
                .retrieve()
                .body(Boolean.class);
        return Boolean.TRUE.equals(alive);
    }

    public List<Member> gossip(String baseUrl, List<Member> myView) {
        return http.post()
                .uri(baseUrl + "/internal/gossip")
                .body(myView)
                .retrieve()
                .body(MEMEBER_LIST);
    }
}
