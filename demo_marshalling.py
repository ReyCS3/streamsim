#!/usr/bin/env python3
"""
StreamSim Marshalling Demonstration
Demonstrates the exact message format BEFORE and AFTER marshalling,
as required for the Milestone 2 video demonstration.
"""

import base64
import hashlib
from common import marshal, unmarshal

def print_section(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def main():
    print_section("DEMO 1: Standard Control Message (Request / Reply)")

    # 1. Before Marshalling: A native Python dictionary in memory
    python_msg = {
        "type": "SEARCH",
        "req_id": "a4f10c82",
        "sender": "client-alice",
        "payload": {
            "query": "sci-fi"
        }
    }

    print("\n[STEP 1: BEFORE MARSHALLING (Python in-memory Object)]")
    print(f"Type: {type(python_msg)}")
    print(f"Value: {python_msg}")

    # 2. Marshalling: Python dict -> Newline-delimited UTF-8 JSON bytes for the TCP wire
    wire_bytes = marshal(python_msg)

    print("\n[STEP 2: AFTER MARSHALLING (Raw Bytes Sent Over TCP Socket)]")
    print(f"Type: {type(wire_bytes)}")
    print(f"Length: {len(wire_bytes)} bytes")
    print(f"Raw Wire Representation: {wire_bytes}")
    print(f"Decoded String on Wire:\n{wire_bytes.decode('utf-8').rstrip()}")

    # 3. Unmarshalling: TCP bytes -> Python dictionary
    unmarshalled_msg = unmarshal(wire_bytes)

    print("\n[STEP 3: AFTER UNMARSHALLING AT RECEIVER (Back to Python Object)]")
    print(f"Type: {type(unmarshalled_msg)}")
    print(f"Value: {unmarshalled_msg}")
    print(f"Matches Original Object: {python_msg == unmarshalled_msg}")


    print_section("DEMO 2: Binary Video Chunk (Base64 + JSON Marshalling)")

    # 1. Raw binary chunk bytes in memory (e.g. video bytes)
    raw_binary_chunk = b"\x00\x01\x02\x03StreamSimVideoPayloadBytes\xfe\xff" * 2
    chunk_hash = hashlib.sha256(raw_binary_chunk).hexdigest()

    print("\n[STEP 1: RAW BINARY CHUNK (Before Encoding & Marshalling)]")
    print(f"Type: {type(raw_binary_chunk)}")
    print(f"Length: {len(raw_binary_chunk)} raw bytes")
    print(f"SHA-256 Hash: {chunk_hash}")
    print(f"Raw Bytes: {raw_binary_chunk}")

    # 2. Encode to Base64 string so it can safely live inside JSON
    b64_string = base64.b64encode(raw_binary_chunk).decode("ascii")

    chunk_envelope = {
        "type": "PUT_CHUNK",
        "req_id": "c92e104f",
        "sender": "ingest",
        "payload": {
            "chunk_id": "v001-c0",
            "data": b64_string,
            "sha256": chunk_hash
        }
    }

    print("\n[STEP 2: ENVELOPE BEFORE MARSHALLING (Base64-encoded in Python Dict)]")
    print(f"Base64 String Length: {len(b64_string)} chars")
    print(f"Envelope: {chunk_envelope}")

    # 3. Marshalling into wire format
    wire_chunk_bytes = marshal(chunk_envelope)

    print("\n[STEP 3: WIRE BYTES TRANSMITTED OVER TCP]")
    print(f"Length on Wire: {len(wire_chunk_bytes)} bytes")
    print(f"Wire Bytes: {wire_chunk_bytes[:100]}... [truncated]")

    # 4. Receiver unmarshals and decodes
    received_envelope = unmarshal(wire_chunk_bytes)
    recovered_binary = base64.b64decode(received_envelope["payload"]["data"])
    recovered_hash = hashlib.sha256(recovered_binary).hexdigest()

    print("\n[STEP 4: UNMARSHALLED & DECODED AT STORAGE NODE]")
    print(f"Recovered Bytes Length: {len(recovered_binary)} bytes")
    print(f"SHA-256 Integrity Verified: {recovered_hash == chunk_hash} ({recovered_hash[:12]}...)")
    print("=" * 70)

if __name__ == "__main__":
    main()
