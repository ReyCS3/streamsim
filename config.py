# Settings shared by every process.
# To run on multiple machines, change HOST (or the individual addresses).

HOST = "127.0.0.1"

# (host, port) for each service
GATEWAY = (HOST, 7000)
METADATA = (HOST, 7100)
CHUNK_NODES = {
    "chunk1": (HOST, 7201),
    "chunk2": (HOST, 7202),
    "chunk3": (HOST, 7203),
}

REPLICATION_FACTOR = 2     # how many nodes store a copy of each chunk
CHUNKS_PER_VIDEO = 4       # how many chunks each fake video is split into
CHUNK_SIZE = 256           # size of each fake chunk in bytes
HEARTBEAT_INTERVAL = 3.0   # seconds between gateway health checks
REQUEST_TIMEOUT = 2.0      # seconds to wait before treating a message as lost
