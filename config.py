# Settings shared by every process.
# To run on multiple machines, change HOST (or the individual addresses).

HOST = "127.0.0.1"

# (host, port) for each service
GATEWAY = (HOST, 7000)
# Metadata replicas. The order is the Bully priority: later in the list = higher priority,
# so meta3 is the leader whenever it's alive.
METADATA_REPLICAS = {
    "meta1": (HOST, 7101),
    "meta2": (HOST, 7102),
    "meta3": (HOST, 7103),
}
CHUNK_NODES = {
    "chunk1": (HOST, 7201),
    "chunk2": (HOST, 7202),
    "chunk3": (HOST, 7203),
}
BROKER = (HOST, 7300)      # pub-sub broker
TRACKER = (HOST, 7400)     # P2P peer tracker
REPLICATION_FACTOR = 2     # how many nodes store a copy of each chunk
CHUNKS_PER_VIDEO = 4       # how many chunks each fake video is split into
CHUNK_SIZE = 256           # size of each fake chunk in bytes

HEARTBEAT_INTERVAL = 3.0   # seconds between gateway health checks
REQUEST_TIMEOUT = 2.0      # seconds to wait before treating a message as lost

LEADER_CHECK_INTERVAL = 1.5  # metadata followers ping the leader this often
ELECTION_TIMEOUT = 3.0       # how long to wait for COORDINATOR after a higher replica answers
WRITE_RETRY_SECONDS = 10.0   # how long writers keep retrying (with backoff) while there's no leader

REREPLICATE_AFTER = 5.0    # a chunk node down this long gets its chunks copied elsewhere

PEER_TTL = 30.0            # tracker forgets a peer that hasn't re-announced in this many seconds
PEER_CACHE_SIZE = 16       # max chunks a viewer keeps in memory to share with other peers

BACKLOG_SIZE = 20          # broker keeps this many recent messages per topic
