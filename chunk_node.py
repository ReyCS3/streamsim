# Chunk storage node: saves video chunks to disk and sends them back when asked.
# Run one per node:  python chunk_node.py --name chunk1

# Handles: HEALTH, PUT_CHUNK, GET_CHUNK

import argparse
import base64
import os
import re

from common import DATA_DIR, make_server
from config import CHUNK_NODES


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, choices=list(CHUNK_NODES))
    name = ap.parse_args().name

    # Each node gets its own folder, e.g. data/chunk1/
    store_dir = os.path.join(DATA_DIR, name)
    os.makedirs(store_dir, exist_ok=True)

    # Turns a chunk id into a file path.
    # The regex blocks ids like "../../secret" from escaping the folder.
    def path_for(chunk_id):
        if not re.fullmatch(r"[\w-]+", chunk_id or ""):
            raise ValueError("invalid chunk_id")
        return os.path.join(store_dir, f"{chunk_id}.bin")

    def route(mtype, p, req_id):
        # Heartbeat from the gateway
        if mtype == "HEALTH":
            return "ok", {"node": name, "chunks": len(os.listdir(store_dir))}

        # Store a chunk (sent by ingest). Bytes arrive base64-encoded since JSON can't hold raw bytes.
        if mtype == "PUT_CHUNK":
            with open(path_for(p["chunk_id"]), "wb") as f:
                f.write(base64.b64decode(p["data"]))
                f.flush()
                os.fsync(f.fileno())  # force it onto disk before saying "ok"
            return "ok", {"node": name, "chunk_id": p["chunk_id"]}

        # Send a chunk back (requested by a client)
        if mtype == "GET_CHUNK":
            path = path_for(p["chunk_id"])
            if not os.path.exists(path):
                return "error", {"error": "chunk not found", "node": name}
            with open(path, "rb") as f:
                data = base64.b64encode(f.read()).decode()
            return "ok", {"node": name, "chunk_id": p["chunk_id"], "data": data}

        return "error", {"error": f"unknown type {mtype}"}

    srv = make_server(CHUNK_NODES[name], name, route)
    srv.logger.info(f"STARTED on {CHUNK_NODES[name]} ({len(os.listdir(store_dir))} chunks on disk)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
