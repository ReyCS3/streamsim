# StreamSim: Distributed Video Streaming

All communication uses TCP sockets (newline-delimited JSON).

## Requirements

- Python 3.8 or newer
- No external dependencies are required.

## Starting the Services

The easiest way to start the complete system is:

    python run_all.py --keep

This starts:

- API Gateway
- Metadata Replicas 1, 2 and 3 (they elect a leader)
- Chunk Nodes 1, 2 and 3
- Pub-Sub Broker
- Peer Tracker
- Recommendation Service
- A viewer ("alice") subscribed to new releases and her recommendations

It also runs the ingest service to upload the sample videos. Every run starts fresh (`logs/` and `data/` are deleted first).

Once the services are running, open another terminal to use the client:

    python client.py browse
    python client.py search sci-fi
    python client.py play v003
    python client.py --user bob play v002 --seed 60     # watch, then share chunks as a P2P peer
    python client.py --user carol play v002             # gets chunks from bob
    python client.py --user carol listen                # print notifications as they arrive
    python ingest.py v005 "Neon Tide" sci-fi 2026       # upload a new title (triggers a NEW RELEASE)

## Test Cases

### Test 1: Normal Operation

Run:

    python run_all.py

Expected result:

- All services start, and the three metadata replicas elect `meta3` as leader.
- Four sample videos are added. Alice receives a **NEW RELEASE** notification for each one.
- The client can browse and search the catalog (different replicas answer different reads).
- Video chunks are retrieved and verified successfully.
- After alice plays a sci-fi title, the recommender pushes **RECOMMENDED: Starlight Rangers** to her.

### Test 2: Chunk Node Failure + Re-replication

Run:

    python run_all.py --fail chunk2

Expected result:

- chunk2 is stopped during the test.
- The client continues retrieving video chunks from the remaining replicas.
- After chunk2 has been down for 5 s, the gateway copies its chunks onto the other nodes (`REPAIR` in `logs/gateway.log`).
- A second node is then killed. Every title still plays from the one remaining node, because the lost copies were re-created.

### Test 3: Metadata Leader Failure (Leader Election)

Run:

    python run_all.py --kill-leader

Expected result:

- The leader (`meta3`) is killed. Searches keep working immediately, because followers can serve reads.
- The followers detect the failure and run a Bully election. `meta2` becomes the leader.
- A new title is uploaded. The write goes to `meta2`, which multicasts it to the other replica.
- `meta3` is restarted. It copies the catalog it missed, wins the election again, and has all 5 titles.

### Test 4: Peer-to-Peer Distribution

Run:

    python run_all.py --p2p

Expected result:

- bob watches a video and stays online as a seeder. mallory does the same but sends **corrupted** chunks.
- carol plays the video and gets all 4 chunks from peers. mallory's bad chunk fails the SHA-256 check and is rejected, and carol ignores her after that.
- bob and mallory crash. dave plays the same video, and the client falls back to the storage nodes.

### Test 5: Everything

    python run_all.py --all

### Test 6: Manual Client Requests

Run:

    python run_all.py --keep

In another terminal, run any of the client commands above.

The message exchanges can also be viewed in:

    logs/

## Processes
| File | Port | Role |
|---|---|---|
| `gateway.py` | 7000 | Client entry point, routing, manifests, heartbeats, re-replication |
| `metadata.py --name metaN` | 7101–7103 | Replicated catalog; Bully leader election; leader multicasts writes |
| `chunk_node.py --name chunkN` | 7201–7203 | Chunk storage |
| `broker.py` | 7300 | Pub-sub broker (topics `new-releases`, `plays`, `recs.<user>`) |
| `tracker.py` | 7400 | P2P tracker: which peer has which chunk |
| `recommender.py` | (none) | Subscribes to `plays` and `new-releases`; publishes `recs.<user>` |
| `ingest.py` | (none) | Uploads videos (chunks, then metadata leader, then `new-releases`) |
| `client.py` | random port when seeding | Viewer: browse/search/play/listen; also a P2P peer |

Shared code lives in `common.py` (marshalling, request-reply, multicast, pub-sub subscribe, logging) and `config.py` (addresses, replication factor, timeouts).

Output:
- `logs/<process>.log` holds timestamped message logs.
- `data/` holds the chunks and one `catalog.json` per metadata replica.

Design writeup: `DESIGN.md`.
