# self-healing-cache

A distributed, self-healing key-value cache built with Spring Boot. Nodes form a peer-to-peer cluster using a SWIM-style gossip protocol, distribute keys via consistent hashing with virtual nodes, and replicate writes across a configurable replica set — with hinted handoff and read repair to recover automatically from transient node failures.

## Features

- **Consistent hashing ring** with virtual nodes (`HashRing`, `HashRingManager`) for even key distribution and minimal reshuffling when membership changes.
- **Gossip-based membership** (`MembershipService`) — SWIM-inspired: periodic direct pings, indirect probing via helper nodes, suspicion timeouts, and incarnation numbers so a node can refute false failure reports about itself.
- **Replication with tunable consistency** (`ReplicationCoordinatorService`) — every write/read targets the key's replica set; callers choose `EVENTUAL` (first successful replica) or `QUORUM` (read/write quorum with version-based conflict resolution).
- **Hinted handoff** (`HintedHandoffStore`) — writes that fail to reach a down replica are stashed locally and replayed automatically once the target node rejoins the cluster.
- **Read repair** — on quorum reads, replicas holding stale versions are updated in the background with the winning entry.
- **TTL expiry** (`ExpiryService`) and **tombstone-based deletes** with a grace period, so deletions propagate correctly across replicas.
- **LRU eviction** (`EvictionManager`) bounded by a configurable max entry count.
- **Virtual threads** enabled for request handling (Spring Boot 3.5 / Java 25).

## Architecture

```
Client
  │  PUT/GET/DELETE /cache/{key}
  ▼
CacheController ──► ReplicationCoordinatorService
                          │
              ┌───────────┼────────────┐
              ▼           ▼            ▼
        HashRingManager  StorageEngine  InternodeClient
        (replica set)    (local KV +    (HTTP calls to
                          eviction)      other nodes)
                          │
                          ▼
                    HintedHandoffStore
                    (replay on recovery)

MembershipService ──gossip/ping──► other nodes (/internal/gossip, /internal/ping*)
        │
        ▼
  HashRingManager.rebuild(aliveNodes)
```

Each node runs the full stack — there's no separate coordinator — so any node can accept a request, resolve the key's replica set, and forward writes/reads to the relevant peers over internal HTTP endpoints.

## API

### Client-facing (`/cache`)

| Method | Path | Description |
|---|---|---|
| `PUT` | `/cache/{key}?ttl=3600&consistencyMode=EVENTUAL` | Store a value (body = raw string) with a TTL in seconds. |
| `GET` | `/cache/{key}?consistencyMode=EVENTUAL` | Fetch a value; 404 if missing/expired. |
| `DELETE` | `/cache/{key}?consistencyMode=EVENTUAL` | Tombstone a key (propagates to replicas). |

`consistencyMode` is `EVENTUAL` or `QUORUM`; defaults to `cache.replication.consistency`.

### Cluster introspection (`/cluster`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/cluster/members` | All known members and their state (ALIVE/SUSPECT/DEAD). |
| `GET` | `/cluster/info` | Self node id, alive peer count, local key count, replication config. |
| `GET` | `/cluster/owners/{key}` | Replica set responsible for a key. |

### Internal, node-to-node (`/internal`)

Used by peers only — `replicate`, `read`, `read-raw`, `ping`, `ping-req`, `gossip`.

## Configuration

All settings live under the `cache.*` prefix in `application.yaml` (see [`CacheProperties`](src/main/java/com/dev/cache/config/CacheProperties.java)) and can be overridden with environment variables:

| Property | Env var | Default | Description |
|---|---|---|---|
| `cache.node-id` | `CACHE_NODE_ID` | `host:port` | Stable identity for this node. |
| `cache.host` / `cache.port` | `CACHE_HOST` / `CACHE_PORT` | `localhost` / `8080` | Address other nodes use to reach this one. |
| `cache.seeds` | `CACHE_SEEDS` | `localhost:8080` | Comma-separated seed addresses used to join the cluster on startup. |
| `cache.ring.vnodes-per-node` | — | `128` | Virtual nodes per physical node on the hash ring. |
| `cache.replication.factor` | `CACHE_RF` | `3` | Number of replicas per key. |
| `cache.replication.consistency` | `CACHE_CONSISTENCY` | `EVENTUAL` | Default consistency mode. |
| `cache.replication.write-quorum` / `read-quorum` | — | `2` / `2` | Quorum size for `QUORUM` mode. |
| `cache.gossip.period-ms` | — | `1000` | Gossip tick / hinted-handoff replay interval. |
| `cache.gossip.suspicion-timeout-ms` | — | `5000` | Time before a SUSPECT node is promoted to DEAD. |
| `cache.gossip.indirect-probe-count` | — | `2` | Helper nodes used for indirect probing. |
| `cache.expiry.sweep-period-ms` | — | `1000` | TTL sweep interval. |
| `cache.eviction.policy` / `max-entries` | — | `LRU` / `100000` | Local eviction policy and capacity. |

## Running locally

Requires Java 25+ and Maven.

**Single node:**

```bash
./mvnw spring-boot:run
```

**Multi-node cluster** (three nodes on one machine, gossiping via seeds):

```bash
CACHE_PORT=8081 CACHE_SEEDS=localhost:8081 ./mvnw spring-boot:run &
CACHE_PORT=8082 CACHE_SEEDS=localhost:8081 ./mvnw spring-boot:run &
CACHE_PORT=8083 CACHE_SEEDS=localhost:8081 ./mvnw spring-boot:run &
```

Then exercise it:

```bash
curl -X PUT "http://localhost:8081/cache/foo?ttl=60" -d "bar"
curl "http://localhost:8082/cache/foo"                 # served from a replica
curl "http://localhost:8083/cluster/members"            # view cluster state
```

Kill one node and writes/reads keep working via the remaining replicas; hinted handoff replays missed writes once it comes back up.

## Build & test

```bash
./mvnw clean install   # build
./mvnw test             # run tests
```

Actuator health/info/metrics are exposed at `/actuator/*`.
