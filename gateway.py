# API gateway: the one address clients talk to.
# - Forwards browse/search to the metadata service
# - For play, gets the manifest and sorts each chunk's nodes so live ones come first
# - Pings chunk nodes in the background to know which are up

# Handles: BROWSE, SEARCH, PLAY


import base64
import hashlib
import threading
import time

from common import call_metadata, make_server, new_id, send_request
from config import (BROKER, CHUNK_NODES, GATEWAY, HEARTBEAT_INTERVAL, METADATA_REPLICAS,
                    REREPLICATE_AFTER)

NAME = "gateway"
alive = {n: True for n in list(CHUNK_NODES) + list(METADATA_REPLICAS)}  # gateway's best guess of which nodes are up
down_since = {}   
repairs = {}


def meta_down():
    return [n for n in METADATA_REPLICAS if not alive[n]]

# Background loop: ping every chunk node, log when one goes up or down.
# "Suspected" because a slow node looks the same as a dead one.
def heartbeat_loop(logger):
    while True:
        for n, addr in list(CHUNK_NODES.items()) + list(METADATA_REPLICAS.items()):
                up = send_request(logger, NAME, n, addr, "HEALTH", timeout=1.0, quiet=True) is not None
                if up != alive[n]:
                    logger.info(f"NODE    {n} is now {'UP' if up else 'SUSPECTED DOWN'}")
                    alive[n] = up
                    if up:
                        down_since.pop(n, None)
                        repairs.pop(n, None)
                    else:
                        down_since[n] = time.time()
    
            # Any chunk node down long enough? Re-create its copies somewhere else (once per outage).
        for n, since in list(down_since.items()):
            if n in CHUNK_NODES and n not in repairs and time.time() - since >= REREPLICATE_AFTER:
                repairs[n] = "running"
                threading.Thread(target=rereplicate, args=(logger, n), daemon=True).start()
        time.sleep(HEARTBEAT_INTERVAL)

# Get a chunk from one of `nodes` and check its hash
def fetch_chunk(logger, nodes, chunk, req_id):
    for n in nodes:
        r = send_request(logger, NAME, n, CHUNK_NODES[n], "GET_CHUNK",
                         {"chunk_id": chunk["chunk_id"]}, req_id)
        if r and r["status"] == "ok":
            data = base64.b64decode(r["data"]["data"])
            if hashlib.sha256(data).hexdigest() == chunk["sha256"]:
                return data
    return None

# Copy every chunk that lived on `dead` onto another live node, so each chunk is back to
# REPLICATION_FACTOR copies. Then tell the metadata leader the new locations.
def rereplicate(logger, dead):
    req_id = new_id()  # one id for the whole repair, so it's easy to follow in the logs
    logger.info(f"REPAIR  id={req_id} {dead} down for {REREPLICATE_AFTER:.0f}s+, re-replicating its chunks")
    resp = call_metadata(logger, NAME, "LIST", {}, req_id, down=meta_down())
    if not resp or resp["status"] != "ok":
        logger.info(f"REPAIR  id={req_id} aborted: metadata unavailable")
        repairs[dead] = "failed"
        return

    copied, lost = 0, 0
    for title in resp["data"]["titles"]:
        m = call_metadata(logger, NAME, "GET_MANIFEST", {"video_id": title["video_id"]}, req_id,
                          down=meta_down())
        if not m or m["status"] != "ok":
            continue
        chunks = m["data"]["chunks"]
        changed = False
        for c in chunks:
            if dead not in c["nodes"]:
                continue
            survivors = [n for n in c["nodes"] if n != dead and alive[n]]
            spares = [n for n in CHUNK_NODES if n not in c["nodes"] and alive[n]]
            if not survivors:
                logger.info(f"REPAIR  id={req_id} {c['chunk_id']} LOST: no live copy left")
                lost += 1
                continue
            if not spares:
                logger.info(f"REPAIR  id={req_id} {c['chunk_id']} skipped: no spare node to copy to")
                continue
            data = fetch_chunk(logger, survivors, c, req_id)
            if data is None:
                continue
            target = spares[0]
            put = send_request(logger, NAME, target, CHUNK_NODES[target], "PUT_CHUNK",
                               {"chunk_id": c["chunk_id"], "data": base64.b64encode(data).decode()},
                               req_id)
            if put and put["status"] == "ok":
                c["nodes"] = [n for n in c["nodes"] if n != dead] + [target]
                changed = True
                copied += 1
        if changed:  # a write, so it goes to the metadata leader
            call_metadata(logger, NAME, "SET_CHUNKS",
                          {"video_id": title["video_id"], "chunks": chunks}, req_id, write=True)

    repairs[dead] = "done"
    logger.info(f"REPAIR  id={req_id} done: {copied} chunk(s) copied off {dead}, {lost} lost")


# Tell the broker someone started watching (runs in the background so PLAY isn't slowed down)
def publish_play(logger, user, video, req_id):
    send_request(logger, NAME, "broker", BROKER, "PUBLISH",
                 {"topic": "plays",
                  "message": {"user": user, "video_id": video["video_id"], "genre": video["genre"]}},
                 req_id)


def route(mtype, p, req_id):
    log = srv.logger

    if mtype == "HEALTH":
        return "ok", {"alive": alive, "repairs": repairs}

    # Browse and search just get passed along to a metadata replica
    # (same req_id so the request can be traced across logs)
    forward = {"BROWSE": "LIST", "SEARCH": "SEARCH"}
    if mtype in forward:
        resp = call_metadata(log, NAME, forward[mtype], p, req_id, down=meta_down())
        if not resp:
            return "error", {"error": "metadata unavailable"}
        return resp["status"], resp["data"]

    if mtype == "PLAY":
        # Step 1: get the manifest from metadata
        resp = call_metadata(log, NAME, "GET_MANIFEST", {"video_id": p.get("video_id")}, req_id,
                             down=meta_down())
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

        # Step 3: publish a "plays" event for the recommendation service
        if p.get("user"):
            threading.Thread(target=publish_play, args=(log, p["user"], video, req_id),
                             daemon=True).start()
        return "ok", video

    return "error", {"error": f"unknown type {mtype}"}


srv = make_server(GATEWAY, NAME, route)

if __name__ == "__main__":
    threading.Thread(target=heartbeat_loop, args=(srv.logger,), daemon=True).start()
    srv.logger.info(f"STARTED on {GATEWAY}, chunk nodes={list(CHUNK_NODES)}")
    srv.serve_forever()
