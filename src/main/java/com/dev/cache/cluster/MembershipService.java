package com.dev.cache.cluster;

import com.dev.cache.config.CacheProperties;
import com.dev.cache.model.Member;
import com.dev.cache.model.MemberState;
import com.dev.cache.ring.HashRingManager;
import com.dev.cache.transport.InternodeClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MembershipService {

    private final CacheProperties props;
    private final InternodeClient client;
    private final HashRingManager ringManager;

    private final ConcurrentHashMap<String, Member> members = new ConcurrentHashMap<>();
    private final String selfId;
    private volatile Member self;
    private volatile String lastAliveSignature = "";

    public MembershipService(CacheProperties props, InternodeClient client, HashRingManager ringManager) {
        this.props = props;
        this.client = client;
        this.ringManager = ringManager;
        this.selfId = props.resolvedNodeId();
        long now = System.currentTimeMillis();
        this.self = new Member(selfId, props.getHost(), props.getPort(), MemberState.ALIVE, now, now);
        members.put(selfId, self);
        rebuildRingIfChanged();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void joinViaSeeds() {
        for (String seed : props.getSeeds()) {
            String baseUrl = "http://" + seed;
            if (seed.equals(self.address())) {
                continue;
            }
            try {
                List<Member> theirView = client.gossip(baseUrl, allMembers());
                merge(theirView);
                log.info("Joined Cluster via seed {}", seed);
            } catch (Exception e) {
                log.error("Seed {} unreachable at startup: {}", seed, e.getMessage());
            }
        }
    }

    @Scheduled(fixedDelayString = "${cache.gossip.period-ms:1000}")
    public void tick() {
        long now = System.currentTimeMillis();
        promoteSuspectsToDead(now);

        Member target = pickRandomAlivePeer();
        if (target == null) return;

        List<Member> myView = allMembers();
        try {
            List<Member> theirView = client.gossip(target.baseUrl(), myView);
            merge(theirView);
            markReachable(target.nodeId(), now);
        } catch (Exception directFail) {
            if (!indirectProbe(target)) {
                suspect(target.nodeId(), now);
                log.info("Suspecting {} (direct + indirect probes failed)", target.nodeId());
            }
        }
    }

    private boolean indirectProbe(Member target) {
        List<Member> helpers = aliveMembers().stream()
                .filter(m -> !m.nodeId().equals(selfId) && !m.nodeId().equals(target.nodeId()))
                .collect(Collectors.toList());
        Collections.shuffle(helpers);
        int k = Math.min(props.getGossip().getIndirectProbeCount(), helpers.size());
        for (int i = 0; i < k; i++) {
            try {
                if (client.pingReq(helpers.get(i).baseUrl(), target.baseUrl())) {
                    return true;
                }
            } catch (Exception e) {
                log.warn("helper itself maybe down; try the next");
            }
        }
        return false;
    }

    public synchronized void merge(List<Member> incoming) {
        if (incoming == null) {
            return;
        }

        long now = System.currentTimeMillis();
        for (Member m : incoming) {
            if (m.nodeId().equals(selfId)) {
                refuteIfNeeded(m, now);
                continue;
            }
            members.merge(m.nodeId(), m, (current, candidate) -> winner(current, candidate));
        }
        rebuildRingIfChanged();
    }

    private Member winner(Member current, Member candidate) {

        if (candidate.incarnation() > current.incarnation()) {
            return candidate;
        }
        if (candidate.incarnation() == current.incarnation()
                && priority(candidate.state()) > priority(current.state())) {
            return candidate;
        }

        return current;
    }

    private int priority(MemberState s) {
        return switch (s) {
            case ALIVE -> 0;
            case SUSPECT -> 1;
            case DEAD -> 2;
        };
    }

    private void refuteIfNeeded(Member rumorAboutSelf, long now) {
        if (rumorAboutSelf.state() != MemberState.ALIVE
                || rumorAboutSelf.incarnation() > self.incarnation()) {
            long newInc = Math.max(rumorAboutSelf.incarnation(), self.incarnation()) + 1;
            self = self.alive(newInc, now);
            members.put(selfId, self);
            log.info("Refuting false suspicion; bumped incarnation to {} ", newInc);
        }
    }

    private synchronized void suspect(String nodeId, long now) {
        Member m = members.get(nodeId);
        if (m != null && m.state() == MemberState.ALIVE) {
            members.put(nodeId, m.withState(MemberState.SUSPECT, now));
        }
    }

    private synchronized void markReachable(String nodeId, long now) {
        Member m = members.get(nodeId);
        if (m != null && m.state() == MemberState.SUSPECT) {
            members.put(nodeId, m.withState(MemberState.ALIVE, now));
            rebuildRingIfChanged();
        }
    }

    private synchronized void promoteSuspectsToDead(long now) {
        boolean changed = false;
        for (Member m : members.values()) {
            if (m.state() == MemberState.SUSPECT
                    && now - m.lastUpdateMs() > props.getGossip().getSuspicionTimeoutMs()) {
                members.put(m.nodeId(), m.withState(MemberState.DEAD, now));
                log.info("Promoted member {} to DEAD due to suspicion timeout", m.nodeId());
                changed = true;
            }
        }
        if (changed) {
            rebuildRingIfChanged();
        }
    }

    private void rebuildRingIfChanged() {
        Set<String> alive = members.values().stream()
                .filter(member -> member.state() == MemberState.ALIVE)
                .map(Member::nodeId)
                .collect(Collectors.toCollection(TreeSet::new));
        String signature = String.join(",", alive);
        if (!signature.equals(lastAliveSignature)) {
            ringManager.rebuild(alive);
            lastAliveSignature = signature;
            log.info("Rebuilt hash ring with alive members: {}", alive);
        }
    }

    public List<Member> allMembers() {
        return new ArrayList<>(members.values());
    }

    public List<Member> aliveMembers() {
        return members.values().stream()
                .filter(m -> m.state() == MemberState.ALIVE)
                .collect(Collectors.toList());
    }

    public boolean isAlive(String nodeId) {
        Member m = members.get(nodeId);
        return m != null && m.state() == MemberState.ALIVE;
    }

    public String baseUrlOf(String nodeId) {
        Member m = members.get(nodeId);
        return m == null ? null : m.baseUrl();
    }

    public String selfId() {
        return selfId;
    }

    private Member pickRandomAlivePeer() {
        List<Member> peers = aliveMembers().stream()
                .filter(m -> !m.nodeId().equals(selfId))
                .toList();
        if (peers.isEmpty()) {
            return null;
        }
        return peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
    }
}
