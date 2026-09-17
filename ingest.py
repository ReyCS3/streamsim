# Ingest service: simulates uploading videos.
# For each video: split into chunks -> store each chunk on 2 nodes -> register title with metadata.
# Later: also publish a NEW_RELEASE message to the pub-sub broker here.

import base64
import hashlib

from common import get_logger, send_request
from config import CHUNK_NODES, CHUNK_SIZE, CHUNKS_PER_VIDEO, METADATA, REPLICATION_FACTOR

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
    # Pick a starting node from a hash of the id, so videos spread across nodes
    start = int(hashlib.sha1(video_id.encode()).hexdigest(), 16) % len(node_names)
    chunks = []

    for i in range(CHUNKS_PER_VIDEO):
        chunk_id = f"{video_id}-c{i}"
        data = fake_chunk(video_id, i)

        # Chunk i goes on the next REPLICATION_FACTOR nodes in order (wrapping around)
        targets = [node_names[(start + i + r) % len(node_names)] for r in range(REPLICATION_FACTOR)]

        # Send the chunk to each target; only record nodes that confirmed
        stored = []
        for n in targets:
            resp = send_request(log, NAME, n, CHUNK_NODES[n], "PUT_CHUNK",
                                {"chunk_id": chunk_id, "data": base64.b64encode(data).decode()})
            if resp and resp["status"] == "ok":
                stored.append(n)

        # Save the hash so clients can check the chunk wasn't corrupted
        chunks.append({"chunk_id": chunk_id, "sha256": hashlib.sha256(data).hexdigest(), "nodes": stored})

    # Register the title + chunk locations with metadata
    entry = {"video_id": video_id, "title": title, "genre": genre, "year": year, "chunks": chunks}
    send_request(log, NAME, "metadata", METADATA, "ADD_TITLE", {"entry": entry})


if __name__ == "__main__":
    for t in SAMPLE_TITLES:
        ingest(*t)
