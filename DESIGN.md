# StreamSim: A Distributed Video-Streaming System
### Architecture, System Model & Implementation

## 1. Purpose

StreamSim is a Netflix-like video-streaming simulation. Viewers browse and search a catalog, then stream a title. Videos are split into chunks. Those chunks are replicated across storage nodes and also shared between viewers (P2P). Metadata lives in its own replicated service coordinated by an elected leader. A publish-subscribe broker notifies viewers about new releases and recommendations. The goal is to keep media **available, fast to deliver, and consistent** while nodes, peers, and links fail.

## 2. Main Services / Components

| Component | Responsibility | File |
|---|---|---|
| **Viewer client** | Browse, search, play. Fetches chunks (peers first, then servers) and verifies SHA-256 hashes. Can stay online as a P2P seeder and subscribe to notifications. | `client.py` |
| **API gateway** | Single entry point for clients. Routes reads to any metadata replica. Builds playback manifests ordered by node health. Publishes `plays` events. Re-replicates chunks off nodes that stay down. | `gateway.py` |
| **Metadata replicas** | Titles, genres, search, chunk manifests. Three replicas; the elected leader serializes all writes and multicasts them to the group. | `metadata.py` ×3 |
| **Chunk storage nodes** | Store and serve video chunks from local disk | `chunk_node.py` ×3 |
| **Ingest service** | Splits uploaded videos into chunks, replicates them, registers the title with the leader, publishes `new-releases` | `ingest.py` |
| **Pub-sub broker** | Topics `new-releases`, `plays`, `recs.<user>`. Pushes each published message to all of the topic's subscribers. | `broker.py` |
| **Recommendation service** | Subscribes to `plays` and `new-releases`; publishes personalized suggestions to `recs.<user>` | `recommender.py` |
| **Peer tracker** | Tracks which peers hold which chunks, so clients can fetch from peers | `tracker.py` |
| **Coordination** | Bully leader election among metadata replicas; the leader is the single writer for the shared catalog | inside `metadata.py` |

Communication uses **TCP sockets** with newline-delimited JSON messages (`type`, `req_id`, `sender`, `payload`).

## 3. Clients, Servers, Peers, Shared Resources

- **Clients:**
  - Viewer apps are clients.
  - The gateway, ingest service, and recommender are clients of the metadata replicas.
  - Ingest and the gateway are also clients of the storage nodes and the broker.
- **Servers:** the gateway, metadata replicas, chunk storage nodes, pub-sub broker, and peer tracker.
- **Peers:**
  - *Viewer peers:* viewers who already hold chunks upload them to other viewers (P2P distribution).
  - *Storage peers:* chunk nodes have identical roles and hold replicas of each other's data.
  - *Metadata replicas:* peers that elect one leader among themselves.
- **Shared resources:**
  - Video chunks, shared by all viewers and replicated across nodes and peers.
  - The metadata catalog, shared by every service. Writes are coordinated through the leader.
  - Storage capacity and upload bandwidth, including bandwidth contributed by peers.
  - Pub-sub topics.

## 4. System Model

### 4.1 Organization: *Hybrid*
- **Centralized:** the API gateway is the entry point, and the metadata leader is the single writer for catalog updates. The broker and tracker are central rendezvous points.
- **Distributed:** chunk storage is spread across replicated nodes, and metadata is replicated across nodes.
- **Decentralized:** P2P chunk sharing between viewers. The tracker only knows *who* has a chunk; the bytes go peer-to-peer.

This mix keeps control-plane logic simple, while the data plane (the actual video bytes) scales out.

### 4.2 Timing: *Semi-synchronous (partially synchronous)*
- There is no global clock, and process speeds and message delays are unbounded in theory.
- In practice we assume delays are **usually** bounded:
  - Requests time out after 2 s and are treated as lost.
  - The gateway sends heartbeats every 3 s and **suspects** a node that doesn't answer within 1 s.
  - Metadata followers ping the leader every 1.5 s. During an election, a replica waits 3 s for a `COORDINATOR` before starting over.
- The failure detector is **unreliable**: a slow node can be falsely suspected. The system recovers when that node responds again (a falsely suspected leader simply wins the next election).

### 4.3 Node-failure assumptions: *Crash-recovery, non-Byzantine servers*
- Servers fail by **crashing**. They may restart and recover state from disk (chunks on disk, each replica's `catalog.json`). A restarted metadata replica catches up by copying the catalog from the most up-to-date replica.
- At most **1 of 3** chunk nodes fails at a time. With replication factor 2, every chunk keeps at least one live copy. Re-replication then restores the second copy, so a *later* second failure is also survived.
- The metadata service tolerates the leader crashing: a new leader is elected and writes continue.
- **Peers are unreliable and untrusted.**
  - They can leave at any time (churn). Tracker entries expire after 30 s without a re-announce, and clients skip peers that don't answer.
  - They can send bad data. Every chunk is checked against the SHA-256 hash in the manifest. Bad chunks are rejected and re-fetched elsewhere, and the bad peer is ignored for the rest of that video.
- The gateway, broker, and tracker are assumed not to fail. The system degrades gracefully if the tracker or broker is down: playback still works, but there is no P2P and no notifications.

### 4.4 Link-failure assumptions: *Fair-loss links over TCP*
- TCP delivers messages uncorrupted, in order, and without duplicates within a connection.
- Messages **can be lost or delayed**, and connections can drop. Timeouts detect this.
  - Clients fail over to another replica.
  - Metadata writers retry with exponential backoff (0.25 s → 2 s, up to 10 s) while an election is in progress.
- Links between the viewer and a peer are the least reliable (home networks), so a peer is always an *optional* source and a server replica is the fallback.
- **Network partitions inside the server cluster are out of scope.** A partitioned node is indistinguishable from a crashed node, and split-brain is not handled. A real system would require a majority quorum for elections and writes.

### 4.5 Storage assumptions
- Each chunk node has **its own local disk**, with no shared storage. Chunks are fsync'd before being acknowledged.
- Chunks are **immutable** once written, so replicas never conflict. Integrity is checked with SHA-256.
- Each metadata replica persists its own catalog with atomic file writes (temp file + rename), along with a sequence number of applied writes.
- Peers cache chunks in **volatile, bounded** memory (16 chunks, LRU), which can disappear at any time.
- Disk corruption is out of scope.

## 5. Architecture Diagram

```mermaid
flowchart TB
    subgraph Viewers["Viewers (clients + P2P peers)"]
      V1[Viewer A]
      V2[Viewer B]
    end
    GW[API Gateway<br/>routing, manifests,<br/>heartbeats, re-replication]
    subgraph Meta["Metadata replicas (Bully election)"]
      ML[Leader meta3]
      MF[Followers meta1, meta2]
    end
    subgraph Storage["Chunk storage (R=2)"]
      C1[(chunk1)]
      C2[(chunk2)]
      C3[(chunk3)]
    end
    PS[Pub-sub broker]
    TR[Peer tracker]
    IN[Ingest service]
    REC[Recommendation service]

    V1 -->|browse / search / play| GW
    GW -->|reads: any replica| MF
    GW -->|writes: SET_CHUNKS| ML
    ML -.REPLICATE multicast.-> MF
    ML <-.ELECTION / COORDINATOR.-> MF
    V1 -->|GET_CHUNK| C1 & C2 & C3
    V1 <-->|P2P GET_CHUNK| V2
    V1 & V2 -->|ANNOUNCE / LOOKUP| TR
    GW -->|re-replicate| Storage
    IN -->|PUT_CHUNK| Storage
    IN -->|ADD_TITLE| ML
    IN -->|new-releases| PS
    GW -->|plays| PS
    PS -.plays, new-releases.-> REC
    REC -->|recs.user| PS
    PS -.notify.-> V1 & V2
```

## 6. Scalability, Dependability, Resource Sharing

### Scalability
- **Separate control and data planes:** small metadata requests go through the gateway, while large chunk transfers go **directly** from storage nodes (or peers) to the client. Video bytes never bottleneck the gateway.
- **Horizontal storage:** add chunk nodes to add capacity, since placement is computed per chunk.
- **Read scaling:** reads are spread round-robin across all metadata replicas (the client prints which replica served each read). Only writes go to the leader.
- **P2P offloading:** popular titles get *more* sources as more people watch them, so load spreads naturally.
- **Pub-sub decoupling:** publishers don't need to know how many viewers exist, and the recommender is just another subscriber.
- **Concurrency:** every service uses a threaded socket server.

### Dependability
- **Replication:** every chunk is stored on 2 nodes (`REPLICATION_FACTOR`). If a preferred node is down at upload time, ingest uses a backup node and warns if it can't reach 2 copies.
- **Failover:** if a chunk source fails, the client tries the next one.
- **Re-replication:** a chunk node that stays down 5 s has every chunk it held copied (hash-verified) from a survivor onto another live node, and the manifest is updated through the leader.
- **Failure detection:** gateway heartbeats move suspected-down nodes to the end of each manifest's source list. Followers detect a dead leader with their own heartbeats.
- **Coordination:** Bully election keeps catalog writes available after the leader crashes. A new leader first syncs from the most up-to-date replica, so acknowledged writes aren't lost.
- **Integrity:** SHA-256 checks protect against corrupted or malicious chunks.
- **Durability:** chunks are fsync'd, and catalogs use atomic writes; both survive restarts.
- **Robustness:** malformed messages get an error reply instead of crashing a handler.
- **Observability:** every socket message is logged with a timestamp and a request ID that follows the request across services.

### Resource Sharing
- Many viewers share one catalog and one pool of stored chunks through a location-transparent API; they never need to know where data lives.
- Viewers share their own upload bandwidth and cached chunks with each other (P2P).
- Access to the shared catalog is coordinated: a lock inside each replica, plus a single elected leader that orders all writes across replicas.
- Pub-sub topics are shared channels that many subscribers read from.

## 7. Implementation

### Requirement checklist
| Requirement | Where it's met |
|---|---|
| Browse, search, request media | `BROWSE`, `SEARCH`, `PLAY` (`client.py` → `gateway.py` → `metadata.py`) |
| Metadata distributed across services | 3 metadata replicas, each with its own catalog on its own disk |
| Chunks stored and retrieved from multiple nodes | 3 chunk nodes, R=2, hash-based placement; clients fetch directly |
| Sockets / RPC / RMI | TCP sockets with JSON messages (`common.py`) |
| Marshalling / unmarshalling | `marshal()` / `unmarshal()` in `common.py`; chunk bytes base64-encoded |
| Request-reply | `send_request()`: one request, one reply, matched by `req_id`, with a timeout |
| Publish-subscribe | `broker.py` + `subscribe()` in `common.py`; topics `new-releases`, `plays`, `recs.<user>` |
| Group communication | `multicast()` in `common.py`: the leader multicasts `REPLICATE` and `COORDINATOR` to the replica group; elections multicast `ELECTION` and `STATE` |
| P2P | `tracker.py` + `client.py play --seed`; peers serve chunks from their cache |
| Leader election | Bully algorithm in `metadata.py` |
| Coordinated access to shared metadata | Only the leader accepts writes; it orders them with sequence numbers |
| Replication + fault tolerance | Chunk R=2, client failover, re-replication, metadata replication, write retry with backoff |
| Message logging with timestamps | Every SEND/RECV/FAIL/MCAST goes to stdout and `logs/<process>.log` as `HH:MM:SS.mmm` |

### Message types
| Service | Types |
|---|---|
| Gateway | `HEALTH`, `BROWSE`, `SEARCH`, `PLAY` |
| Metadata replica | `HEALTH`, `LIST`, `SEARCH`, `GET_MANIFEST`, `ADD_TITLE`\*, `SET_CHUNKS`\*, `REPLICATE`, `ELECTION`, `COORDINATOR`, `STATE`, `SYNC` (\* leader only) |
| Chunk node / seeding peer | `HEALTH`, `PUT_CHUNK`, `GET_CHUNK` |
| Broker | `HEALTH`, `SUBSCRIBE` (connection stays open for `NOTIFY` pushes), `PUBLISH` |
| Tracker | `HEALTH`, `ANNOUNCE`, `LEAVE`, `LOOKUP` |

### Playback flow
1. The client sends `PLAY v001` (with its user name) to the gateway.
2. The gateway sends `GET_MANIFEST` to a metadata replica (round-robin, failing over to the next replica).
3. The gateway orders each chunk's sources so live nodes come first and returns the manifest. In the background, it publishes a `plays` event.
4. The client asks the tracker which peers have the chunks.
5. The client sends `GET_CHUNK` to peers first, then storage nodes, verifying each hash and failing over as needed.
6. The recommender receives the `plays` event and publishes `recs.<user>`. The broker pushes it to the viewer's open subscription.

### Write flow (e.g. a new upload)
1. Ingest stores each chunk on 2 nodes (`PUT_CHUNK`).
2. Ingest sends `ADD_TITLE` to the leader. A follower would reply `not leader` with a hint to the real leader. If there is no leader, ingest backs off and retries.
3. The leader assigns sequence number *n*, saves the write, and multicasts `REPLICATE{seq=n}` to the group.
4. Each follower applies the write if *n* is the next sequence number. If it detects a gap, it pulls a full `SYNC` instead.
5. Ingest publishes the title to `new-releases`.

### Bully election
1. A replica that starts up, or sees the leader stop answering, sends `ELECTION` to every higher-priority replica (`meta1` < `meta2` < `meta3`).
2. Any higher replica that replies "ok" takes over the election. The starter then waits for a `COORDINATOR`.
3. A replica with no live higher replica wins. It asks the group for their `STATE` (sequence numbers), copies the catalog from the most up-to-date one if it is behind, and then multicasts `COORDINATOR`.
4. A restarted higher replica bullies its way back to leader the same way.

### Known limitations
- There is no majority quorum, so a network partition could produce two leaders.
- A write is acknowledged once it is replicated to the *reachable* followers.
- The gateway, broker, and tracker are single instances.
- Viewing history in the recommender is kept in memory only.
