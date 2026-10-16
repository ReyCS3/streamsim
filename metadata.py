# Metadata replica: stores the catalog (titles, genres) and each video's chunk list.
# The chunk list ("manifest") says which nodes hold each chunk.
# Run three of them:  python metadata.py --name meta1   (and meta2, meta3)
#
# Reads  (LIST, SEARCH, GET_MANIFEST) are served by ANY replica.
# Writes (ADD_TITLE, SET_CHUNKS) are only accepted by the elected LEADER. The leader gives
# each write the next sequence number, applies it, then multicasts it to the other replicas,
# so every replica applies the same writes in the same order.
#
# Leader election is the Bully algorithm (highest-priority live replica wins):
#   ELECTION     sent to every higher replica. An "ok" reply means "I'm alive, I'll take over".
#   COORDINATOR  multicast to the whole group by the winner: "I'm the leader now".
# Followers ping the leader every LEADER_CHECK_INTERVAL and start an election if it's gone.
#
# Handles: HEALTH, LIST, SEARCH, GET_MANIFEST, ADD_TITLE, SET_CHUNKS,
#          ELECTION, COORDINATOR, REPLICATE, STATE, SYNC

import argparse
import copy
import json
import os
import threading
import time

from common import DATA_DIR, get_logger, make_server, multicast, send_request
from config import ELECTION_TIMEOUT, LEADER_CHECK_INTERVAL, METADATA_REPLICAS

NAMES = list(METADATA_REPLICAS)  # list order = Bully priority (last = highest)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, choices=NAMES)
    me = ap.parse_args().name
    log = get_logger(me)

    prio = NAMES.index(me)
    others = {n: a for n, a in METADATA_REPLICAS.items() if n != me}
    higher = {n: a for n, a in others.items() if NAMES.index(n) > prio}

    # Each replica keeps its own copy on its own disk, e.g. data/meta1/catalog.json
    store_dir = os.path.join(DATA_DIR, me)
    os.makedirs(store_dir, exist_ok=True)
    catalog_path = os.path.join(store_dir, "catalog.json")

    lock = threading.Lock()           # many handler threads read/write the catalog, so guard it
    write_lock = threading.Lock()     # leader: one write at a time, so followers get them in order
    election_lock = threading.Lock()  # only one election at a time on this replica
    coordinator_seen = threading.Event()

    # seq = number of writes applied so far. Replicas with the same seq have the same catalog.
    state = {"seq": 0, "titles": {}, "leader": None, "electing": False}

    # Load saved catalog from disk if it exists (crash-recovery)
    if os.path.exists(catalog_path):
        with open(catalog_path) as f:
            saved = json.load(f)
        state["seq"], state["titles"] = saved["seq"], saved["titles"]

    # Write to a temp file then rename, so a crash mid-write can't corrupt the catalog.
    # Caller must hold `lock`.
    def save():
        tmp = catalog_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"seq": state["seq"], "titles": state["titles"]}, f, indent=2)
        os.replace(tmp, catalog_path)

    # Short version of a title (no chunk list) for browse/search results
    def summary(v):
        return {k: v[k] for k in ("video_id", "title", "genre", "year")}

    # Copy the whole catalog from another replica (used to catch up after being down)
    def sync_from(name, req_id=None):
        resp = send_request(log, me, name, METADATA_REPLICAS[name], "SYNC", {"from": me}, req_id)
        if not resp or resp["status"] != "ok":
            return False
        with lock:
            state["titles"], state["seq"] = resp["data"]["titles"], resp["data"]["seq"]
            save()
        log.info(f"SYNCED  from {name}: seq={state['seq']} titles={len(state['titles'])}")
        return True

    # ---------- Leader: apply a write, then multicast it to the group ----------
    def leader_write(entry, req_id):
        with write_lock:
            with lock:
                seq = state["seq"] + 1
                state["titles"][entry["video_id"]] = entry
                state["seq"] = seq
                save()
            # Group communication: every other replica gets the same update, tagged with its seq
            results = multicast(log, me, others, "REPLICATE",
                                {"from": me, "seq": seq, "entry": entry}, req_id)
        acks = [n for n, r in results.items() if r and r["status"] == "ok"]
        return seq, acks

    # ---------- Bully election ----------
    def run_election(reason):
        with election_lock:
            if state["electing"]:
                return
            state["electing"] = True
        try:
            while True:
                log.info(f"ELECT   starting election ({reason}); asking higher replicas {list(higher)}")
                coordinator_seen.clear()
                answers = multicast(log, me, higher, "ELECTION", {"from": me}, timeout=1.0) if higher else {}
                if any(r and r["status"] == "ok" for r in answers.values()):
                    # A higher replica is alive and will take over. Wait for its COORDINATOR.
                    if coordinator_seen.wait(ELECTION_TIMEOUT):
                        return
                    reason = "higher replica answered but never announced itself"
                    continue
                # Nobody higher answered: I win
                become_leader()
                return
        finally:
            state["electing"] = False

    def become_leader():
        # Before taking over, catch up to the most up-to-date replica so no writes are lost
        states = multicast(log, me, others, "STATE", {"from": me}, timeout=1.0)
        best, best_seq = None, state["seq"]
        for n, r in states.items():
            if r and r["status"] == "ok" and r["data"]["seq"] > best_seq:
                best, best_seq = n, r["data"]["seq"]
        if best:
            sync_from(best)
        with lock:
            state["leader"] = me
            seq = state["seq"]
        log.info(f"LEADER  {me} is now the leader (seq={seq})")
        multicast(log, me, others, "COORDINATOR", {"leader": me, "seq": seq}, timeout=1.0)

    def start_election(reason):
        threading.Thread(target=run_election, args=(reason,), daemon=True).start()

    # Followers: keep checking that the leader is alive
    def monitor_leader():
        time.sleep(0.5)  # give the other replicas a moment to start listening
        run_election("startup")
        while True:
            time.sleep(LEADER_CHECK_INTERVAL)
            leader = state["leader"]
            if leader == me or state["electing"]:
                continue
            if leader is None:
                run_election("no leader known")
                continue
            up = send_request(log, me, leader, METADATA_REPLICAS[leader], "HEALTH",
                              timeout=1.0, quiet=True)
            if up is None:
                log.info(f"NODE    leader {leader} is SUSPECTED DOWN")
                with lock:
                    state["leader"] = None
                run_election(f"leader {leader} down")

    # ---------- Message handling ----------
    def route(mtype, p, req_id):
        if mtype == "HEALTH":
            with lock:
                return "ok", {"node": me, "leader": state["leader"], "seq": state["seq"],
                              "titles": len(state["titles"])}

        # Browse: return every title
        if mtype == "LIST":
            with lock:
                return "ok", {"replica": me, "titles": [summary(v) for v in state["titles"].values()]}

        # Search: match text against title or genre (case-insensitive)
        if mtype == "SEARCH":
            q = p.get("query", "").lower()
            with lock:
                hits = [summary(v) for v in state["titles"].values()
                        if q in v["title"].lower() or q in v["genre"].lower()]
            return "ok", {"replica": me, "query": q, "titles": hits}

        # Play: return the full entry, including where every chunk lives
        if mtype == "GET_MANIFEST":
            with lock:
                v = copy.deepcopy(state["titles"].get(p.get("video_id")))
            if v:
                v["replica"] = me
                return "ok", v
            return "error", {"error": "title not found"}

        # Writes: only the leader accepts them; others point the caller at the leader
        if mtype in ("ADD_TITLE", "SET_CHUNKS"):
            if state["leader"] != me:
                return "error", {"error": "not leader", "leader": state["leader"]}
            if mtype == "ADD_TITLE":
                entry = p["entry"]           # new upload from ingest
            else:
                with lock:                   # chunk locations changed (re-replication)
                    cur = copy.deepcopy(state["titles"].get(p["video_id"]))
                if not cur:
                    return "error", {"error": "title not found"}
                cur["chunks"] = p["chunks"]
                entry = cur
            seq, acks = leader_write(entry, req_id)
            return "ok", {"video_id": entry["video_id"], "seq": seq, "replicated_to": acks}

        # Follower: apply a write the leader multicast to the group
        if mtype == "REPLICATE":
            seq, sender = p["seq"], p["from"]
            with lock:
                mine = state["seq"]
                if seq <= mine:
                    return "ok", {"seq": mine, "note": "already have it"}
                if seq == mine + 1:
                    state["titles"][p["entry"]["video_id"]] = p["entry"]
                    state["seq"] = seq
                    state["leader"] = sender
                    save()
                    return "ok", {"seq": seq}
            # We missed some writes (e.g. we were down), so copy the whole catalog instead
            log.info(f"GAP     have seq={mine}, got seq={seq} from {sender}; syncing")
            sync_from(sender, req_id)
            return "ok", {"seq": state["seq"]}

        if mtype == "STATE":
            with lock:
                return "ok", {"node": me, "seq": state["seq"]}

        if mtype == "SYNC":
            with lock:
                return "ok", {"seq": state["seq"], "titles": copy.deepcopy(state["titles"])}

        # A lower replica started an election. Answer ok (I outrank you), then run my own.
        if mtype == "ELECTION":
            start_election(f"ELECTION from {p.get('from')}")
            return "ok", {"node": me, "answer": "OK"}

        # Someone announced they are the leader
        if mtype == "COORDINATOR":
            new = p["leader"]
            if NAMES.index(new) < prio:
                # A lower replica claims leadership, but I'm alive and outrank it: bully it
                start_election(f"lower replica {new} claimed leadership")
                return "ok", {"node": me, "note": "I outrank you"}
            with lock:
                old = state["leader"]
                state["leader"] = new
                myseq = state["seq"]
            coordinator_seen.set()
            log.info(f"LEADER  {new} is the leader (was {old})")
            if p.get("seq") != myseq:  # I'm behind (or diverged): adopt the leader's catalog
                threading.Thread(target=sync_from, args=(new, req_id), daemon=True).start()
            return "ok", {"node": me, "seq": myseq}

        return "error", {"error": f"unknown type {mtype}"}

    srv = make_server(METADATA_REPLICAS[me], me, route)
    log.info(f"STARTED on {METADATA_REPLICAS[me]} ({len(state['titles'])} titles, seq={state['seq']} loaded)")
    threading.Thread(target=monitor_leader, daemon=True).start()
    srv.serve_forever()


if __name__ == "__main__":
    main()
