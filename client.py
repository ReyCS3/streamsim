# Viewer client (command line). A viewer is also a P2P peer and a pub-sub subscriber.
#   python client.py [--user alice] browse
#   python client.py [--user alice] search <text>
#   python client.py [--user alice] play <video_id> [--seed SECONDS] [--corrupt]
#   python client.py [--user alice] listen [--for SECONDS]
#   python client.py [--user alice] demo
#
# play --seed N   after watching, stay online N seconds and share cached chunks with other viewers
# play --corrupt  (testing only) act as a malicious peer that serves damaged chunks
# listen          print new-release notifications and recommendations as they arrive

import argparse
import base64
import binascii
import collections
import hashlib
import threading
import time

from common import get_logger, make_server, send_request, subscribe
from config import BROKER, GATEWAY, HOST, PEER_CACHE_SIZE, PEER_TTL, TRACKER

# Set in main() once we know the user
USER = "alice"
NAME = "client"
log = None

# Chunks this viewer has watched, kept so it can share them with other peers.
# Volatile (memory only) and bounded: oldest chunks are dropped first.
cache = collections.OrderedDict()  # chunk_id -> bytes


# Send a request to the gateway
def call(mtype, payload=None):
    return send_request(log, NAME, "gateway", GATEWAY, mtype, payload)


def remember(chunk_id, data):
    cache[chunk_id] = data
    cache.move_to_end(chunk_id)
    while len(cache) > PEER_CACHE_SIZE:
        cache.popitem(last=False)


# Print every title in the catalog
def browse():
    resp = call("BROWSE")
    if resp and resp["status"] == "ok":
        print(f"  [{USER}] catalog (served by {resp['data'].get('replica')}):")
        for t in resp["data"]["titles"]:
            print(f"  {t['video_id']}  {t['title']:<20} {t['genre']:<9} {t['year']}")
    else:
        print(f"  [{USER}] browse failed: {resp['data']['error'] if resp else 'gateway unavailable'}")


# Print titles matching a title or genre
def search(query):
    resp = call("SEARCH", {"query": query})
    if resp and resp["status"] == "ok":
        titles = resp["data"]["titles"]
        print(f"  [{USER}] {len(titles)} result(s) for '{query}' (served by {resp['data'].get('replica')}):",
              ", ".join(t["title"] for t in titles))
    else:
        print(f"  [{USER}] search failed: {resp['data']['error'] if resp else 'gateway unavailable'}")

# MILESTONE 3: RING OVERLAY
def get_overlay_members():
    """
    Retrieve active peer membership from the tracker.

    The tracker provides membership information, but does
    not perform resource discovery for overlay lookups.
    """
    response = send_request(
        log,
        NAME,
        "tracker",
        TRACKER,
        "PEERS",
        {}
    )

    if not response or response["status"] != "ok":
        return {}

    return response["data"].get("peers", {})


def ring_successors(current_peer, members):
    """
    Return the other peers in clockwise ring order.

    Peer names are sorted to create a deterministic ring.
    For example:

    peer-alice -> peer-bob -> peer-carol -> peer-dave
                       ^                       |
                       |_______________________|

    The current peer is excluded from its successor list.
    """
    names = sorted(members.keys())

    if current_peer not in names:
        return []

    position = names.index(current_peer)

    return [
        names[(position + offset) % len(names)]
        for offset in range(1, len(names))
    ]


def overlay_lookup(chunk_id):
    """
    Discover a video chunk by sending a lookup into the
    peer overlay.

    The tracker is consulted only to find an entry peer.
    The lookup itself travels between peers.
    """
    members = get_overlay_members()

    if not members:
        log.info(
            f"OVERLAY EMPTY chunk={chunk_id}"
        )
        return []

    entry_peer = sorted(members.keys())[0]
    entry_addr = tuple(members[entry_peer]["addr"])

    log.info(
        f"OVERLAY START chunk={chunk_id} "
        f"entry={entry_peer}"
    )

    response = send_request(
        log,
        NAME,
        entry_peer,
        entry_addr,
        "OVERLAY_LOOKUP",
        {
            "chunk_id": chunk_id,
            "visited": []
        }
    )

    if not response or response["status"] != "ok":
        log.info(
            f"OVERLAY MISS chunk={chunk_id}"
        )
        return []

    source = response["data"].get("source")

    if not source:
        log.info(
            f"OVERLAY NOT_FOUND chunk={chunk_id}"
        )
        return []

    log.info(
        f"OVERLAY FOUND chunk={chunk_id} "
        f"owner={source['node']} "
        f"path={response['data'].get('path', [])}"
    )

    return [dict(source, peer=True)]


def lookup_peers_overlay(chunk_ids):
    """
    Perform distributed resource discovery for each chunk.
    """
    return {
        chunk_id: overlay_lookup(chunk_id)
        for chunk_id in chunk_ids
    }

# Ask the tracker which peers have these chunks. Peers are optional: if the tracker
# is down we just use the storage nodes.
def lookup_peers(chunk_ids):
    r = send_request(log, NAME, "tracker", TRACKER, "LOOKUP",
                     {"chunk_ids": chunk_ids, "exclude": f"peer-{USER}"})
    if not r or r["status"] != "ok":
        return {}
    return {cid: [dict(s, peer=True) for s in srcs] for cid, srcs in r["data"]["sources"].items()}

# Milestone 3: Added optional overlay mode for P2P resource discovery.
# Original def play(video_id, seed=0, corrupt=False): > playback behavior remains unchanged when overlay=False.
def play(video_id, seed=0, corrupt=False, overlay=False):

    # Step 1: ask the gateway for the manifest (chunk list + where each chunk lives)
    resp = call("PLAY", {"video_id": video_id, "user": USER})
    if not resp or resp["status"] != "ok":
        reason = resp["data"]["error"] if resp else "gateway unavailable"
        print(f"  [{USER}] cannot play {video_id}: {reason}")
        return

    video = resp["data"]
    # Step 2: ask the tracker which peers already have these chunks
    
    ### Milestone 3: Added optional P2P overlay lookup for video chunks.
    ### Keeps the original tracker-based lookup when overlay mode is disabled.
    chunk_ids = [
        c["chunk_id"]
        for c in video["chunks"]
    ]

    if overlay:
        log.info(
            f"MILESTONE 3: overlay playback "
            f"user={USER} video={video_id}"
        )
        peer_sources = lookup_peers_overlay(chunk_ids)
    else:
        # Original Milestone 2 behavior remains unchanged.
        peer_sources = lookup_peers(chunk_ids)
    ### old line: peer_sources = lookup_peers([c["chunk_id"] for c in video["chunks"]]) > replaced with line 208 to 221
    
    stats = {"peers": 0, "servers": 0, "failovers": 0, "rejected": 0}
    banned = set()  # peers that sent bad data or didn't answer; skip them for this video

    # Step 3: fetch each chunk, trying peers first (offloads the servers), then storage nodes
    for chunk in video["chunks"]:
        sources = peer_sources.get(chunk["chunk_id"], []) + chunk["sources"]
        for src in sources:
            if src["node"] in banned:
                continue
            r = send_request(log, NAME, src["node"], tuple(src["addr"]), "GET_CHUNK",
                             {"chunk_id": chunk["chunk_id"]})
            if r and r["status"] == "ok":
                try:
                    data = base64.b64decode(r["data"]["data"])
                except (binascii.Error, KeyError, TypeError):
                    data = b""
                # Step 4: check the hash matches the manifest before accepting it
                if hashlib.sha256(data).hexdigest() == chunk["sha256"]:
                    remember(chunk["chunk_id"], data)
                    stats["peers" if src.get("peer") else "servers"] += 1
                    break
                log.info(f"BADHASH chunk={chunk['chunk_id']} from {src['node']}")
                stats["rejected"] += 1
                if src.get("peer"):
                    banned.add(src["node"])
                    log.info(f"BAN     {src['node']} sent bad data; ignoring it for the rest of this video")
            elif r is None and src.get("peer"):
                # Peer didn't answer (probably left): skip it for the rest of this video
                banned.add(src["node"])
                log.info(f"SKIP    {src['node']} unreachable; ignoring it for the rest of this video")
            stats["failovers"] += 1  # this source didn't work, try the next one
        else:
            # for-else: runs only if no source worked
            print(f"  [{USER}] playback stalled: {chunk['chunk_id']} unavailable on all replicas")
            return

    n = len(video["chunks"])
    print(f"  [{USER}] playing '{video['title']}': {n}/{n} chunks verified "
          f"({stats['peers']} from peers, {stats['servers']} from servers), "
          f"{stats['failovers']} failover(s), {stats['rejected']} bad chunk(s) rejected", flush=True)

    if seed:
        run_seeder(seed, corrupt)


# Stay online as a P2P peer: serve cached chunks and keep announcing to the tracker
def run_seeder(seconds, corrupt):
    peer_name = f"peer-{USER}"

    def route(mtype, p, req_id):
        if mtype == "HEALTH":
            return "ok", {"node": peer_name, "chunks": len(cache)}
        if mtype == "GET_CHUNK":
            data = cache.get(p.get("chunk_id"))
            if data is None:
                return "error", {"error": "not cached", "node": peer_name}
            if corrupt:  # simulate a broken or malicious peer
                data = bytes(b ^ 0xFF for b in data)
            return "ok", {"node": peer_name, "chunk_id": p["chunk_id"],
                          "data": base64.b64encode(data).decode()}
                # Milestone 3: Forward resource lookup through the ring.
        if mtype == "OVERLAY_LOOKUP":
            chunk_id = p.get("chunk_id")
            visited = list(p.get("visited", []))

            if not chunk_id:
                return "error", {
                    "error": "missing chunk_id"
                }

            # Prevent routing loops.
            if peer_name in visited:
                return "ok", {
                    "source": None,
                    "path": visited
                }

            visited.append(peer_name)

            log.info(
                f"OVERLAY VISIT peer={peer_name} "
                f"chunk={chunk_id} "
                f"path={visited}"
            )

            # Check this peer's local video-chunk cache.
            if chunk_id in cache:
                log.info(
                    f"OVERLAY HIT peer={peer_name} "
                    f"chunk={chunk_id}"
                )

                return "ok", {
                    "source": {
                        "node": peer_name,
                        "addr": addr
                    },
                    "path": visited
                }

            # Find the next available peer in the ring.
            members = get_overlay_members()

            # If the tracker is temporarily unavailable,
            # the lookup safely ends and playback can
            # fall back to the chunk storage servers.
            if not members:
                return "ok", {
                    "source": None,
                    "path": visited
                }

            successors = ring_successors(
                peer_name,
                members
            )

            for successor in successors:
                if successor in visited:
                    continue

                successor_addr = tuple(
                    members[successor]["addr"]
                )

                log.info(
                    f"OVERLAY FORWARD "
                    f"{peer_name} -> {successor} "
                    f"chunk={chunk_id}"
                )

                response = send_request(
                    log,
                    NAME,
                    successor,
                    successor_addr,
                    "OVERLAY_LOOKUP",
                    {
                        "chunk_id": chunk_id,
                        "visited": visited
                    }
                )

                if response and response["status"] == "ok":
                    result = response["data"]

                    if result.get("source"):
                        return "ok", result

                    # Preserve traversal information.
                    for name in result.get("path", []):
                        if name not in visited:
                            visited.append(name)

                else:
                    log.info(
                        f"OVERLAY FAILURE "
                        f"peer={successor} "
                        f"trying another successor"
                    )

                    if successor not in visited:
                        visited.append(successor)

            log.info(
                f"OVERLAY NOT_FOUND "
                f"peer={peer_name} "
                f"chunk={chunk_id}"
            )

            return "ok", {
                "source": None,
                "path": visited
            }
        return "error", {"error": f"unknown type {mtype}"}

    srv = make_server((HOST, 0), NAME, route)  # port 0 = let the OS pick a free port
    addr = list(srv.server_address)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"  [{USER}] seeding {len(cache)} chunk(s) as {peer_name} on {addr[0]}:{addr[1]} "
          f"for {seconds:.0f}s{' (CORRUPT)' if corrupt else ''}", flush=True)

    end = time.time() + seconds
    while time.time() < end:
        send_request(log, NAME, "tracker", TRACKER, "ANNOUNCE",
                     {"peer": peer_name, "addr": addr, "chunks": list(cache)})
        time.sleep(max(0, min(PEER_TTL / 3, end - time.time())))  # re-announce before the TTL runs out
    send_request(log, NAME, "tracker", TRACKER, "LEAVE", {"peer": peer_name})
    srv.shutdown()


# Subscribe to new releases + my recommendations and print them as they arrive
def listen(duration=None):
    topics = ["new-releases", f"recs.{USER}"]
    print(f"  [{USER}] listening for {topics}", flush=True)

    def on_message(note):
        m = note["payload"]
        if note["topic"] == "new-releases":
            print(f"  [{USER}] NEW RELEASE: '{m['title']}' ({m['genre']}, {m['year']})", flush=True)
        else:
            print(f"  [{USER}] RECOMMENDED: {', '.join(m['titles'])} ({m['reason']})", flush=True)

    end = time.time() + duration if duration else None
    while end is None or time.time() < end:
        remaining = None if end is None else end - time.time()
        if subscribe(log, NAME, BROKER, topics, on_message, duration=remaining):
            return  # ran for the full duration
        time.sleep(1.0)  # broker unreachable or dropped us: reconnect


def main():
    global USER, NAME, log
    ap = argparse.ArgumentParser(description="StreamSim viewer client")
    ap.add_argument("--user", default="alice")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("browse")
    s = sub.add_parser("search")
    s.add_argument("query")
    pl = sub.add_parser("play")
    pl.add_argument("video_id")
    pl.add_argument("--seed", type=float, default=0, help="stay online as a P2P peer for N seconds")
    pl.add_argument("--corrupt", action="store_true", help="(testing) serve damaged chunks as a peer")

    # Milestone 3: Optional ring overlay for P2P resource discovery.
    pl.add_argument(
        "--overlay",
        action="store_true",
        help="discover video chunks through the P2P ring"
    )
    
    li = sub.add_parser("listen")
    li.add_argument("--for", dest="duration", type=float, default=None, help="seconds (default: forever)")
    sub.add_parser("demo")
    args = ap.parse_args()

    USER = args.user
    NAME = f"client-{USER}"
    log = get_logger(NAME)

    cmd = args.cmd or "demo"
    if cmd == "browse":
        browse()
    elif cmd == "search":
        search(args.query)
    # old line     
    # elif cmd == "play":
    #    play(args.video_id, args.seed, args.corrupt)
    # Update for Milestone 3
    elif cmd == "play":
        play(
            args.video_id,
            args.seed,
            args.corrupt,
            args.overlay
        )    
    elif cmd == "listen":
        listen(args.duration)
    elif cmd == "demo":
        browse()
        search("sci-fi")
        play("v001")
        play("v999")  # doesn't exist, shows the error path


if __name__ == "__main__":
    main()
