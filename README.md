# StreamSim: Distributed Video Streaming (Phase 1 skeleton)

All communication uses TCP sockets (newline-delimited JSON).

## Requirements

- Python 3.8 or newer
- No external dependencies are required.

## Starting the Services

The easiest way to start the complete system is:

    python run_all.py --keep

This starts:

- API Gateway
- Metadata Server
- Chunk Node 1
- Chunk Node 2
- Chunk Node 3

It also runs the ingest service to upload the sample videos.

Once the services are running, open another terminal to use the client:

    python client.py browse
    python client.py search sci-fi
    python client.py play v003

## Test Cases

### Test 1: Normal Operation

Run:

    python run_all.py

Expected result:

- All services start successfully.
- Four sample videos are added.
- The client can browse and search the catalog.
- Video chunks are retrieved successfully.

### Test 2: Chunk Node Failure

Run:

    python run_all.py --fail chunk2

Expected result:

- chunk2 is stopped during the test.
- The client continues retrieving video chunks from the remaining replicas.
- The videos can still be played successfully.

### Test 3: Manual Client Requests

Run:

    python run_all.py --keep

In another terminal:

    python client.py browse
    python client.py search sci-fi
    python client.py play v003

The message exchanges can also be viewed in:

    logs/


## Processes
| File | Port | Role |
|---|---|---|
| `gateway.py` | 7000 | Client entry point, routing, manifests, heartbeats |
| `metadata.py` | 7100 | Catalog, search, chunk manifests |
| `chunk_node.py --name chunkN` | 7201–7203 | Chunk storage |
| `ingest.py` | (none) | Uploads sample videos (chunks + metadata) |
| `client.py` | (none) | Viewer: browse/search/play |

Shared code lives in `common.py` (socket protocol + logging) and `config.py` (addresses, replication factor, timeouts).

Output: `logs/<process>.log` holds message logs, and `data/` holds chunks and `catalog.json`. Delete `data/` to reset.

Design writeup: `DESIGN.md`.
