# Metadata service: stores the catalog (titles, genres) and each video's chunk list.
# The chunk list ("manifest") says which nodes hold each chunk.
#
# Handles: HEALTH, LIST, SEARCH, GET_MANIFEST, ADD_TITLE
# Right now it's one instance. Later: 3 replicas with leader election.

import json
import os
import threading

from common import DATA_DIR, make_server
from config import METADATA

NAME = "metadata"
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.json")  # catalog saved here so it survives restarts


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    lock = threading.Lock()  # multiple threads touch the catalog, so guard it

    # Load saved catalog from disk if it exists
    catalog = {}
    if os.path.exists(CATALOG_PATH):
        with open(CATALOG_PATH) as f:
            catalog = json.load(f)

    # Short version of a title (no chunk list) for browse/search results
    def summary(v):
        return {k: v[k] for k in ("video_id", "title", "genre", "year")}

    def route(mtype, p, req_id):
        if mtype == "HEALTH":
            return "ok", {"titles": len(catalog)}

        # Browse: return every title
        if mtype == "LIST":
            with lock:
                return "ok", {"titles": [summary(v) for v in catalog.values()]}

        # Search: match text against title or genre (case-insensitive)
        if mtype == "SEARCH":
            q = p.get("query", "").lower()
            with lock:
                hits = [summary(v) for v in catalog.values()
                        if q in v["title"].lower() or q in v["genre"].lower()]
            return "ok", {"query": q, "titles": hits}

        # Play: return the full entry, including where every chunk lives
        if mtype == "GET_MANIFEST":
            with lock:
                v = catalog.get(p.get("video_id"))
            if v:
                return "ok", v
            return "error", {"error": "title not found"}

        # New upload from ingest: add it and save to disk
        if mtype == "ADD_TITLE":
            entry = p["entry"]
            with lock:
                catalog[entry["video_id"]] = entry
                # Write to a temp file then rename, so a crash mid-write can't corrupt the catalog
                tmp = CATALOG_PATH + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(catalog, f, indent=2)
                os.replace(tmp, CATALOG_PATH)
            return "ok", {"video_id": entry["video_id"]}

        return "error", {"error": f"unknown type {mtype}"}

    srv = make_server(METADATA, NAME, route)
    srv.logger.info(f"STARTED on {METADATA} ({len(catalog)} titles loaded)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
