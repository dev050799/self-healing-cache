package com.dev.cache.config;

import com.dev.cache.model.ConsistencyMode;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "cache")
@Data
public class CacheProperties {

    private String nodeId;
    private String host = "localhost";
    private int port = 8080;
    private List<String> seeds;

    private final Ring ring = new Ring();
    private final Replication replication = new Replication();
    private final Client client = new Client();
    private final Gossip gossip = new Gossip();
    private final Expiry expiry = new Expiry();
    private final Eviction eviction = new Eviction();

    public String resolvedNodeId() {
        return (nodeId == null || nodeId.isBlank()) ? host + ":" + port : nodeId;
    }

    @Data
    public static class Ring {
        private int vnodesPerNode = 128;
    }

    @Data
    public static class Replication {
        private int factor = 3;
        private ConsistencyMode consistency = ConsistencyMode.EVENTUAL;
        private int writeQuorum = 2;
        private int readQuorum = 2;
    }

    @Data
    public static class Client {
        private int connectTimeoutMs = 300;
        private int requestTimeoutMs = 500;
        private int maxRetries = 1;
    }

    @Data
    public static class Gossip {
        private int periodMs = 1000;
        private int suspicionTimeoutMs = 5000;
        private int indirectProbeCount = 2;
    }

    @Data
    public static class Expiry {
        private int sweepPeriodMs = 1000;
    }

    @Data
    public static class Eviction {
        private Policy policy = Policy.LRU;
        private int maxEntries = 100_000;

        public enum Policy {NONE, LRU}
    }
}
