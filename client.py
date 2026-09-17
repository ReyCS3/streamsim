# Viewer client (command line).
#   python client.py browse
#   python client.py search <text>
#   python client.py play <video_id>
#   python client.py demo

import base64
import hashlib
import sys

from common import get_logger, send_request
from config import GATEWAY

NAME = "client"
log = get_logger(NAME)

USAGE = "usage: client.py browse | search <text> | play <video_id> | demo"


# Send a request to the gateway
def call(mtype, payload=None):
    return send_request(log, NAME, "gateway", GATEWAY, mtype, payload)


# Print every title in the catalog
def browse():
    resp = call("BROWSE")
    if resp and resp["status"] == "ok":
        for t in resp["data"]["titles"]:
            print(f"  {t['video_id']}  {t['title']:<20} {t['genre']:<9} {t['year']}")


# Print titles matching a title or genre
def search(query):
    resp = call("SEARCH", {"query": query})
    if resp and resp["status"] == "ok":
        titles = resp["data"]["titles"]
        print(f"  {len(titles)} result(s) for '{query}':", ", ".join(t["title"] for t in titles))


def play(video_id):
    # Step 1: ask the gateway for the manifest (chunk list + where each chunk lives)
    resp = call("PLAY", {"video_id": video_id})
    if not resp or resp["status"] != "ok":
        reason = resp["data"]["error"] if resp else "gateway unavailable"
        print(f"  cannot play {video_id}: {reason}")
        return

    video = resp["data"]
    received = 0
    failovers = 0  # how many times we had to try a backup node

    # Step 2: fetch each chunk directly from a storage node
    for chunk in video["chunks"]:
        for attempt, src in enumerate(chunk["sources"]):  # try sources in order
            r = send_request(log, NAME, src["node"], tuple(src["addr"]), "GET_CHUNK",
                             {"chunk_id": chunk["chunk_id"]})
            if r and r["status"] == "ok":
                data = base64.b64decode(r["data"]["data"])
                # Step 3: check the hash matches the manifest before accepting it
                if hashlib.sha256(data).hexdigest() == chunk["sha256"]:
                    received += 1
                    failovers += attempt
                    break
                log.info(f"BADHASH chunk={chunk['chunk_id']} from {src['node']}")
        else:
            # for-else: runs only if no source worked
            print(f"  playback stalled: {chunk['chunk_id']} unavailable on all replicas")
            return

    print(f"  playing '{video['title']}': {received}/{len(video['chunks'])} chunks verified, "
          f"{failovers} failover(s)")


if __name__ == "__main__":
    args = sys.argv[1:] or ["demo"]
    if args[0] == "browse":
        browse()
    elif args[0] == "search" and len(args) == 2:
        search(args[1])
    elif args[0] == "play" and len(args) == 2:
        play(args[1])
    elif args[0] == "demo":
        browse()
        search("sci-fi")
        play("v001")
        play("v999")  # doesn't exist, shows the error path
    else:
        print(USAGE)
