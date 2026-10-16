# Starts every service, uploads sample videos, and runs the client demo.
#   python run_all.py                  normal demo: browse/search/play + pub-sub notifications
#   python run_all.py --p2p            viewers share chunks with each other (incl. a malicious peer)
#   python run_all.py --fail chunk2    kill a chunk node: failover, then re-replication
#   python run_all.py --kill-leader    kill the metadata leader: Bully election, writes keep working
#   python run_all.py --all            all of the above (uses --fail chunk2)
#   python run_all.py --keep           leave services running afterwards (Ctrl+C to stop)
# Every run starts fresh: logs/ and data/ are deleted first.

import argparse
import shutil
import signal
import subprocess
import sys
import time

from common import DATA_DIR, LOG_DIR, get_logger, send_request
from config import CHUNK_NODES, GATEWAY, METADATA_REPLICAS, REREPLICATE_AFTER, TRACKER

ap = argparse.ArgumentParser()
ap.add_argument("--fail", choices=list(CHUNK_NODES), help="chunk node to kill mid-demo")
ap.add_argument("--kill-leader", action="store_true", help="kill the metadata leader mid-demo")
ap.add_argument("--p2p", action="store_true", help="run the peer-to-peer demo")
ap.add_argument("--all", action="store_true", help="run every demo")
ap.add_argument("--keep", action="store_true", help="keep services running")
args = ap.parse_args()
if args.all:
    args.p2p, args.kill_leader, args.fail = True, True, args.fail or "chunk2"

# If someone kills run_all itself, exit normally so the cleanup below still stops every service
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

shutil.rmtree(LOG_DIR, ignore_errors=True)   # fresh logs each run
shutil.rmtree(DATA_DIR, ignore_errors=True)  # fresh chunks + catalogs each run
log = get_logger("run_all")
py = sys.executable  # same Python that's running this script
procs = {}
VIDEOS = ["v001", "v002", "v003", "v004"]


def start(name, *argv):
    procs[name] = subprocess.Popen([py, *argv])


def kill(name):
    procs[name].terminate()
    procs[name].wait()


def banner(text):
    print(f"\n=== {text} ===", flush=True)


def client(*argv):
    subprocess.run([py, "client.py", *argv])


# Ask a service something without logging it (polling)
def ask(name, addr, mtype="HEALTH"):
    r = send_request(log, "run_all", name, addr, mtype, timeout=1.0, quiet=True)
    return r["data"] if r and r["status"] == "ok" else None


# The leader every live replica agrees on, or None if they don't agree (yet)
def agreed_leader(expect_all=False):
    views = {n: ask(n, a) for n, a in METADATA_REPLICAS.items()}
    views = {n: v["leader"] for n, v in views.items() if v}
    if expect_all and len(views) < len(METADATA_REPLICAS):
        return None
    leaders = set(views.values())
    if views and len(leaders) == 1 and None not in leaders:
        return leaders.pop()
    return None


def wait_for(cond, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(0.3)
    print(f"  (timed out waiting for {what})", flush=True)
    return None


# Start each service as its own process
for n in METADATA_REPLICAS:
    start(n, "metadata.py", "--name", n)
for n in CHUNK_NODES:
    start(n, "chunk_node.py", "--name", n)
start("broker", "broker.py")
start("tracker", "tracker.py")
time.sleep(0.5)  # let storage start first so the gateway's first heartbeat doesn't mark nodes down
start("gateway", "gateway.py")
start("recommender", "recommender.py")

try:
    leader = wait_for(lambda: agreed_leader(expect_all=True), 15, "leader election")
    banner(f"metadata replicas elected {leader} as leader (Bully: highest live replica wins)")

    # alice keeps a subscription open for the whole run, so she sees every notification
    start("listener", "client.py", "--user", "alice", "listen")
    time.sleep(0.7)

    banner("ingest: uploading sample videos (alice gets NEW RELEASE notifications)")
    subprocess.run([py, "ingest.py"])

    banner("client demo (alice watches sci-fi -> the recommender pushes suggestions to her)")
    client("demo")
    time.sleep(1.0)  # let the recommendation reach alice's listener

    if args.p2p:
        banner("P2P: bob watches v002, then stays online sharing his cached chunks")
        start("bob", "client.py", "--user", "bob", "play", "v002", "--seed", "120")
        wait_for(lambda: "peer-bob" in ((ask("tracker", TRACKER) or {}).get("peers") or {}), 10, "bob")

        banner("P2P: mallory also seeds v002, but she is MALICIOUS and serves corrupted bytes")
        start("mallory", "client.py", "--user", "mallory", "play", "v002", "--seed", "120", "--corrupt")
        wait_for(lambda: "peer-mallory" in ((ask("tracker", TRACKER) or {}).get("peers") or {}), 10, "mallory")

        banner("P2P: carol plays v002 -> gets chunks from peers; mallory's bad data fails the hash check")
        client("--user", "carol", "play", "v002")

        banner("P2P churn: bob and mallory crash without telling the tracker")
        kill("bob")
        kill("mallory")
        banner("dave plays v002 -> the stale peers don't answer, so he falls back to storage nodes")
        client("--user", "dave", "play", "v002")

    # Kill a node and show videos still play from the other replica
    if args.fail:
        banner(f"killing {args.fail}")
        kill(args.fail)
        banner("playing every title with a node down (client fails over to the other copy)")
        for vid in VIDEOS:
            client("play", vid)

        banner(f"waiting for the gateway to re-replicate {args.fail}'s chunks "
               f"(starts after {REREPLICATE_AFTER:.0f}s down)")
        wait_for(lambda: ((ask("gateway", GATEWAY) or {}).get("repairs") or {}).get(args.fail) == "done",
                 40, "re-replication")
        second = next(n for n in CHUNK_NODES if n != args.fail)
        banner(f"also killing {second}: only ONE storage node left")
        kill(second)
        banner("playing every title again: still works, because the lost copies were re-created")
        for vid in VIDEOS:
            client("play", vid)

    if args.kill_leader:
        old = agreed_leader() or leader
        banner(f"killing the metadata leader {old}")
        kill(old)
        banner("reads keep working immediately: any surviving replica can answer")
        client("search", "sci-fi")

        new = wait_for(lambda: (lambda l: l if l and l != old else None)(agreed_leader()), 20,
                       "new leader")
        banner(f"Bully election finished: {new} is the new leader")

        banner("uploading a new title: the write goes to the new leader, which multicasts it to the group")
        subprocess.run([py, "ingest.py", "v005", "Neon Tide", "sci-fi", "2026"])
        time.sleep(0.5)

        banner(f"restarting {old}: it outranks {new}, so it catches up and bullies its way back to leader")
        start(old, "metadata.py", "--name", old)
        back = wait_for(lambda: agreed_leader(expect_all=True) == old, 20, f"{old} to take over")
        data = ask(old, METADATA_REPLICAS[old], "LIST")
        if back and data:
            print(f"  {old} is leader again and has {len(data['titles'])} titles "
                  f"(including Neon Tide, written while it was down)", flush=True)
        client("search", "neon")

    if args.keep:
        print("\nServices running. Ctrl+C to stop.", flush=True)
        while True:
            time.sleep(1)
    time.sleep(0.5)  # let last log lines flush
except KeyboardInterrupt:
    pass
finally:
    # Always shut everything down, even on Ctrl+C or errors
    for p in procs.values():
        p.terminate()
    for p in procs.values():
        p.wait()
    print("\nLogs written to ./logs/")
