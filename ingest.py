# Ingest service: simulates uploading videos.
# For each video: split into chunks -> store each chunk on REPLICATION_FACTOR nodes
#   -> register the title with the metadata LEADER -> publish NEW_RELEASE to the pub-sub broker.
#   python ingest.py                                  uploads the sample catalog
#   python ingest.py v005 "Neon Tide" sci-fi 2026     uploads one title

import base64
import hashlib
import sys

from common import call_metadata, get_logger, new_id, send_request
from config import BROKER, CHUNK_NODES, CHUNK_SIZE, CHUNKS_PER_VIDEO, REPLICATION_FACTOR

NAME = "ingest"
log = get_logger(NAME)
node_names = list(CHUNK_NODES)

# Fake catalog to upload: (video_id, title, genre, year)
SAMPLE_TITLES = [
    ("v001", "Orbit Nine", "sci-fi", 2025),
    ("v002", "The Last Harbor", "drama", 2024),
    ("v003", "Midnight Circuit", "thriller", 2026),
    ("v004", "Starlight Rangers", "sci-fi", 2023),
]


# Makes fake "video" bytes for one chunk (just repeated text)
def fake_chunk(video_id, i):
    seed = f"{video_id}-chunk{i}-".encode()
    return (seed * (CHUNK_SIZE // len(seed) + 1))[:CHUNK_SIZE]


def ingest(video_id, title, genre, year):
    req_id = new_id()  # one id for the whole upload, so it can be traced across every log
    # Pick a starting node from a hash of the id, so videos spread across nodes
    start = int(hashlib.sha1(video_id.encode()).hexdigest(), 16) % len(node_names)
    chunks = []

    for i in range(CHUNKS_PER_VIDEO):
        chunk_id = f"{video_id}-c{i}"
        data = fake_chunk(video_id, i)

        # Chunk i prefers the next REPLICATION_FACTOR nodes in order (wrapping around).
        # The remaining nodes are backups in case a preferred node is down.
        order = [node_names[(start + i + r) % len(node_names)] for r in range(len(node_names))]

        # Send the chunk until REPLICATION_FACTOR nodes confirm; only record nodes that confirmed
        stored = []
        for n in order:
            if len(stored) == REPLICATION_FACTOR:
                break
            resp = send_request(log, NAME, n, CHUNK_NODES[n], "PUT_CHUNK",
                                {"chunk_id": chunk_id, "data": base64.b64encode(data).decode()},
                                req_id)
            if resp and resp["status"] == "ok":
                stored.append(n)

        if not stored:
            log.info(f"ERROR   {chunk_id} could not be stored on any node; aborting {video_id}")
            return False
        if len(stored) < REPLICATION_FACTOR:
            log.info(f"WARN    {chunk_id} under-replicated: only on {stored}")

        # Save the hash so clients can check the chunk wasn't corrupted
        chunks.append({"chunk_id": chunk_id, "sha256": hashlib.sha256(data).hexdigest(), "nodes": stored})

    # Register the title + chunk locations. This is a write, so it must reach the metadata leader.
    entry = {"video_id": video_id, "title": title, "genre": genre, "year": year, "chunks": chunks}
    resp = call_metadata(log, NAME, "ADD_TITLE", {"entry": entry}, req_id, write=True)
    if not resp or resp["status"] != "ok":
        log.info(f"ERROR   could not register {video_id}: no metadata leader reachable")
        return False

    # Tell everyone subscribed to new-releases
    send_request(log, NAME, "broker", BROKER, "PUBLISH",
                 {"topic": "new-releases",
                  "message": {"video_id": video_id, "title": title, "genre": genre, "year": year}},
                 req_id)
    return True


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        ok = all([ingest(*t) for t in SAMPLE_TITLES])
    elif len(args) == 4:
        ok = ingest(args[0], args[1], args[2], int(args[3]))
    else:
        print('usage: ingest.py  |  ingest.py <video_id> "<title>" <genre> <year>')
        sys.exit(2)
    sys.exit(0 if ok else 1)
