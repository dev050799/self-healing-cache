package com.dev.cache.model;

public record Member(String nodeId,
                     String host,
                     int port,
                     MemberState state,
                     long incarnation,
                     long lastUpdateMs) {

    public String address() {
        return host + ":" + port;
    }

    public String baseUrl() {
        return "http://" + address();
    }

    public Member withState(MemberState newState, long now) {
        return new Member(nodeId, host, port, newState, incarnation, now);
    }

    public Member alive(long newIncarnation, long now) {
        return new Member(nodeId, host, port, MemberState.ALIVE, newIncarnation, now);
    }
}
