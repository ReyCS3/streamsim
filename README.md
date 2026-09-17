# StreamSim: Distributed Video Streaming (Phase 1 skeleton)

Pure Python 3.8+, **no dependencies**. All communication uses TCP sockets (newline-delimited JSON).

## Run
```bash
python run_all.py                # start services, ingest 4 titles, run client demo
python run_all.py --fail chunk2  # also kill a chunk node and play every title
python run_all.py --keep         # leave services running for manual testing
```

With `--keep` running, in another terminal:
```bash
python client.py browse
python client.py search sci-fi
python client.py play v003
```

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
