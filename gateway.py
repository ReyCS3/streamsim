# API gateway: the one address clients talk to.
# - Forwards browse/search to the metadata service
# - For play, gets the manifest and sorts each chunk's nodes so live ones come first
# - Pings chunk nodes in the background to know which are up

# Handles: BROWSE, SEARCH, PLAY

import threading
import time

from common import make_server, send_request
from config import CHUNK_NODES, GATEWAY, HEARTBEAT_INTERVAL, METADATA

NAME = "gateway"
alive = {n: True for n in CHUNK_NODES}  # gateway's best guess of which nodes are up


# Background loop: ping every chunk node, log when one goes up or down.
# "Suspected" because a slow node looks the same as a dead one.
def heartbeat_loop(logger):
    while True:
        for n, addr in CHUNK_NODES.items():
            up = send_request(logger, NAME, n, addr, "HEALTH", timeout=1.0, quiet=True) is not None
            if up != alive[n]:
                logger.info(f"NODE    {n} is now {'UP' if up else 'SUSPECTED DOWN'}")
                alive[n] = up
        time.sleep(HEARTBEAT_INTERVAL)


def route(mtype, p, req_id):
    log = srv.logger

    # Browse and search just get passed along to metadata
    # (same req_id so the request can be traced across logs)
    forward = {"BROWSE": "LIST", "SEARCH": "SEARCH"}
    if mtype in forward:
        resp = send_request(log, NAME, "metadata", METADATA, forward[mtype], p, req_id)
        if not resp:
            return "error", {"error": "metadata unavailable"}
        return resp["status"], resp["data"]

    if mtype == "PLAY":
        # Step 1: get the manifest from metadata
        resp = send_request(log, NAME, "metadata", METADATA, "GET_MANIFEST", p, req_id)
        if not resp:
            return "error", {"error": "metadata unavailable"}
        if resp["status"] != "ok":
            return resp["status"], resp["data"]

        # Step 2: for each chunk, list live nodes first and attach their addresses
        # so the client can fetch chunks directly from storage (not through the gateway)
        video = resp["data"]
        for chunk in video["chunks"]:
            nodes = sorted(chunk["nodes"], key=lambda n: not alive.get(n, False))
            chunk["sources"] = [{"node": n, "addr": list(CHUNK_NODES[n])} for n in nodes]
            del chunk["nodes"]
        return "ok", video

    return "error", {"error": f"unknown type {mtype}"}


srv = make_server(GATEWAY, NAME, route)

if __name__ == "__main__":
    threading.Thread(target=heartbeat_loop, args=(srv.logger,), daemon=True).start()
    srv.logger.info(f"STARTED on {GATEWAY}, chunk nodes={list(CHUNK_NODES)}")
    srv.serve_forever()
