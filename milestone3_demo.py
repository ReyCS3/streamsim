"""
CECS 327 - Milestone 3: P2P Integration

Demonstrates:
1. Four peer nodes
2. Peer joining
3. Peer leaving
4. Lookup routing through a ring overlay
5. Discovery of a video chunk through peers

Run the original services first:
    python run_all.py --keep

Then, in another terminal:
    python milestone3_demo.py
"""

import subprocess
import sys
import time

from common import get_logger, send_request
from config import TRACKER

PYTHON = sys.executable
LOGGER = get_logger("milestone3-demo")

processes = {}


def tracker_peers():
    """Get the currently registered overlay members."""
    response = send_request(
        LOGGER,
        "milestone3-demo",
        "tracker",
        TRACKER,
        "PEERS",
        {},
        quiet=True
    )

    if not response or response["status"] != "ok":
        return {}

    return response["data"].get("peers", {})


def wait_for_peer(name, present=True, timeout=20):
    """Wait for a peer to appear or disappear."""
    end = time.time() + timeout

    while time.time() < end:
        exists = f"peer-{name}" in tracker_peers()

        if exists == present:
            return True

        time.sleep(0.25)

    raise RuntimeError(
        f"Timed out waiting for peer-{name} "
        f"present={present}"
    )


def show_ring():
    """Display the current overlay structure."""
    members = sorted(tracker_peers())

    if not members:
        print("  Ring is empty.")
        return

    print("  Overlay ring:")
    print("  " + " -> ".join(
        members + [members[0]]
    ))


def banner(message):
    print("\n" + "=" * 60, flush=True)
    print(message, flush=True)
    print("=" * 60, flush=True)


def start_peer(name, video_id, seconds=90):
    """
    Start a viewer that downloads a video,
    registers with the tracker, and seeds chunks.
    """
    command = [
        PYTHON,
        "client.py",
        "--user",
        name,
        "play",
        video_id,
        "--seed",
        str(seconds)
    ]

    processes[name] = subprocess.Popen(command)

    wait_for_peer(name)

    print(
        f"  JOIN: peer-{name} "
        f"is sharing {video_id}",
        flush=True
    )


def lookup(name, video_id):
    """Run playback using distributed overlay lookup."""
    print(
        f"\n  LOOKUP: {name} requests {video_id}",
        flush=True
    )

    result = subprocess.run(
        [
            PYTHON,
            "client.py",
            "--user",
            name,
            "play",
            video_id,
            "--overlay"
        ],
        check=False
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Overlay playback exited with "
            f"code {result.returncode}"
        )


def graceful_leave(name):
    """
    Wait for the peer's seeding period to end.
    The client sends LEAVE to the tracker.
    """
    process = processes[name]
    process.wait(timeout=30)

    wait_for_peer(name, present=False)

    print(
        f"  LEAVE: peer-{name} "
        f"has left the overlay",
        flush=True
    )


def main():
    banner("MILESTONE 3: P2P RING INTEGRATION")

    if not tracker_peers():
        print(
            "  Tracker reachable. Starting peer demo.",
            flush=True
        )

    # -------------------------------------------------
    # TEST 1: Build the initial ring with three peers.
    # -------------------------------------------------

    banner("TEST 1: START THREE PEERS")

    start_peer("alice", "v001")
    start_peer("bob", "v003")
    start_peer("carol", "v004")

    show_ring()

    # -------------------------------------------------
    # TEST 2: Dave joins as the fourth peer.
    # -------------------------------------------------

    banner("TEST 2: FOURTH PEER JOINS")

    # Dave stays online for a shorter period so we can
    # demonstrate a normal, graceful departure.
    start_peer("dave", "v002", seconds=18)

    show_ring()

    members = tracker_peers()

    assert len(members) >= 4, (
        "Expected at least four active peers"
    )

    print(
        "  PASS: At least four peers are active.",
        flush=True
    )

    # -------------------------------------------------
    # TEST 3: Routed lookup.
    #
    # Eve requests v002, which Dave is sharing.
    # The lookup should travel through:
    #
    # Alice -> Bob -> Carol -> Dave
    #
    # before discovering Dave's chunks.
    # -------------------------------------------------

    banner("TEST 3: MULTI-HOP RESOURCE DISCOVERY")

    lookup("eve", "v002")

    print(
        "  Check logs/client-eve.log and peer logs "
        "for OVERLAY forwarding.",
        flush=True
    )

    # -------------------------------------------------
    # TEST 4: Graceful peer departure.
    # -------------------------------------------------

    banner("TEST 4: PEER LEAVES")

    graceful_leave("dave")

    show_ring()

    # -------------------------------------------------
    # TEST 5: Lookup after departure.
    #
    # Dave is no longer available. The remaining peers
    # should not advertise Dave's chunks. The original
    # chunk storage servers still provide playback.
    # -------------------------------------------------

    banner("TEST 5: LOOKUP AFTER PEER DEPARTURE")

    lookup("frank", "v002")

    print(
        "  Check that overlay lookup reports no peer "
        "source and playback falls back to storage.",
        flush=True
    )

    banner("MILESTONE 3 DEMONSTRATION COMPLETE")


if __name__ == "__main__":
    try:
        main()

    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()

        for process in processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
