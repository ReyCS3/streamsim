# Peer tracker (P2P): remembers which viewer peers hold which chunks.
# The tracker never sees the chunk bytes, only who has what.
#

# Handles: HEALTH, ANNOUNCE, LEAVE, LOOKUP

import threading
import time

from common import make_server
from config import PEER_TTL, TRACKER

NAME = "tracker"
lock = threading.Lock()
peers = {}  


# Forget peers that stopped announcing (crashed or left without LEAVE)
def expire(log):
    now = time.time()
    with lock:
        for name in [n for n, e in peers.items() if now - e["last_seen"] > PEER_TTL]:
            del peers[name]
            log.info(f"EXPIRE  {name} (no announce for {PEER_TTL:.0f}s)")


def route(mtype, p, req_id):
    log = srv.logger
    expire(log)

    if mtype == "HEALTH":
        with lock:
            return "ok", {"peers": {n: len(e["chunks"]) for n, e in peers.items()}}

    
    if mtype == "ANNOUNCE":
        name, now = p["peer"], time.time()
        with lock:
            entry = peers.get(name) or {"joined": now}
            entry.update(addr=p["addr"], chunks=set(p.get("chunks", [])), last_seen=now)
            peers[name] = entry
            online = len(peers)
        return "ok", {"peer": name, "chunks": len(entry["chunks"]), "peers_online": online}

    
    if mtype == "LEAVE":
        with lock:
            peers.pop(p.get("peer"), None)
        return "ok", {"peer": p.get("peer")}

    
    if mtype == "LOOKUP":
        exclude = p.get("exclude")
        with lock:
            ranked = sorted(peers.items(), key=lambda kv: -kv[1]["joined"])
            sources = {cid: [{"node": n, "addr": e["addr"]} for n, e in ranked
                             if cid in e["chunks"] and n != exclude]
                       for cid in p.get("chunk_ids", [])}
        return "ok", {"sources": sources}

    return "error", {"error": f"unknown type {mtype}"}


srv = make_server(TRACKER, NAME, route)

if __name__ == "__main__":
    srv.logger.info(f"STARTED on {TRACKER}")
    srv.serve_forever()
