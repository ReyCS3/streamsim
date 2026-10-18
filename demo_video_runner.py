#!/usr/bin/env python3
"""
StreamSim Silent Video Demonstration Runner
Guides you through the 4 required video demonstrations with clear on-screen
visual banners and zero speaking required.
"""

import subprocess
import sys
import time

py = sys.executable

def banner(num, title, description):
    print("\n" + "#" * 74)
    print(f"  REQUIREMENT {num}: {title.upper()}")
    print("#" * 74)
    print(f">> {description}\n")

def pause(msg="Press ENTER to proceed to the next requirement..."):
    print()
    input(f"--- {msg} ---")
    print()

def main():
    print("\n" + "=" * 74)
    print("   STREAMSIM DISTRIBUTED SYSTEM — MILESTONE 2 VIDEO DEMO")
    print("=" * 74)
    print("Make sure your background cluster is running in the other terminal:")
    print("   python run_all.py --keep")
    pause("Press ENTER when your cluster is running to start the video recording")

    # =========================================================================
    # REQUIREMENT 1: Message Format Before and After Marshalling
    # =========================================================================
    banner(1, "Message Format Before and After Marshalling",
           "Shows in-memory Python Dict vs. Raw Wire Bytes (JSON + \\n) and Base64 chunk bytes")
    subprocess.run([py, "demo_marshalling.py"])
    pause()

    # =========================================================================
    # REQUIREMENT 2: Request-Reply Communication
    # =========================================================================
    banner(2, "Request-Reply Communication",
           "Client sends synchronous requests to Gateway/Metadata and receives matching replies")
    print(">> Executing: python client.py search sci-fi")
    subprocess.run([py, "client.py", "search", "sci-fi"])
    time.sleep(1.5)

    print("\n>> Executing: python client.py play v001")
    subprocess.run([py, "client.py", "play", "v001"])
    pause()

    # =========================================================================
    # REQUIREMENT 3: Publish-Subscribe Notification
    # =========================================================================
    banner(3, "Publish-Subscribe Notifications",
           "Alice maintains open TCP push connection to broker; receives real-time notifications")

    print(">> Starting subscriber 'alice' in background listening for 'new-releases' and 'recs.alice'...")
    listener = subprocess.Popen([py, "client.py", "--user", "alice", "listen"])
    time.sleep(1.0)

    print("\n>> Trigger 1: Ingest service PUBLISHES a new release to broker:")
    print("   python ingest.py v005 'Neon Tide' sci-fi 2026")
    subprocess.run([py, "ingest.py", "v005", "Neon Tide", "sci-fi", "2026"])
    time.sleep(2.0)

    print("\n>> Trigger 2: Alice watches sci-fi -> Gateway publishes 'plays' -> Recommender publishes suggestion:")
    print("   python client.py --user alice play v001")
    subprocess.run([py, "client.py", "--user", "alice", "play", "v001"])
    time.sleep(2.5)

    listener.terminate()
    listener.wait()
    pause()

    # =========================================================================
    # REQUIREMENT 4: Group Communication (Multicast)
    # =========================================================================
    banner(4, "Group Communication (Multicast)",
           "Leader meta3 multicasts REPLICATE write transactions to followers meta1 & meta2 concurrently")

    print(">> Reading leader multicast log entries from logs/meta3.log:\n")
    try:
        with open("logs/meta3.log") as f:
            lines = [l.strip() for l in f if "MCAST" in l or "REPLICATE" in l]
        for l in lines[-12:]:
            print("  " + l)
    except FileNotFoundError:
        print("  (logs/meta3.log will be displayed once the cluster has executed a write)")

    print("\n" + "=" * 74)
    print("   ALL 4 VIDEO REQUIREMENTS COMPLETED SUCCESSFULLY!")
    print("=" * 74 + "\n")

if __name__ == "__main__":
    main()
