# Starts every service, uploads sample videos, and runs the client demo.
#   python run_all.py               normal demo
#   python run_all.py --fail chunk2 also kills a chunk node, then plays every title
#   python run_all.py --keep        leaves services running (Ctrl+C to stop)

import argparse
import shutil
import subprocess
import sys
import time

from config import CHUNK_NODES

ap = argparse.ArgumentParser()
ap.add_argument("--fail", choices=list(CHUNK_NODES), help="chunk node to kill mid-demo")
ap.add_argument("--keep", action="store_true", help="keep services running")
args = ap.parse_args()

shutil.rmtree("logs", ignore_errors=True)  # fresh logs each run
py = sys.executable  # same Python that's running this script

# Start each service as its own process
procs = {"metadata": subprocess.Popen([py, "metadata.py"])}
for n in CHUNK_NODES:
    procs[n] = subprocess.Popen([py, "chunk_node.py", "--name", n])
time.sleep(0.5)  # let storage start first so the gateway's first heartbeat doesn't mark nodes down
procs["gateway"] = subprocess.Popen([py, "gateway.py"])
time.sleep(1.5)  # give servers time to start listening

try:
    print("\n=== ingest: uploading sample videos ===")
    subprocess.run([py, "ingest.py"])

    print("\n=== client demo ===")
    subprocess.run([py, "client.py", "demo"])

    # Kill a node and show videos still play from the other replica
    if args.fail:
        print(f"\n=== killing {args.fail} ===")
        procs[args.fail].terminate()
        procs[args.fail].wait()
        print("=== playing every title with a node down ===")
        for vid in ["v001", "v002", "v003", "v004"]:
            subprocess.run([py, "client.py", "play", vid])

    if args.keep:
        print("\nServices running. Ctrl+C to stop.")
        while True:
            time.sleep(1)
    time.sleep(0.3)  # let last log lines flush
finally:
    # Always shut everything down, even on Ctrl+C or errors
    for p in procs.values():
        p.terminate()
    print("\nLogs written to ./logs/")
