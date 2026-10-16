# Recommendation service: a pub-sub subscriber AND publisher.
# Subscribes to:
#   plays          (from the gateway) -> remember what each user watched, then publish
#                  recs.<user> with unwatched titles in their favorite genre
#   new-releases   (from ingest)      -> tell users who like that genre about the new title
# Publishes to:
#   recs.<user>

import collections
import time

from common import call_metadata, get_logger, send_request, subscribe
from config import BROKER

NAME = "recommender"
log = get_logger(NAME)
watched = collections.defaultdict(set)                    
liked = collections.defaultdict(collections.Counter)      


def publish(topic, message, req_id=None):
    send_request(log, NAME, "broker", BROKER, "PUBLISH", {"topic": topic, "message": message}, req_id)


def on_message(note):
    m, req_id = note["payload"], note.get("req_id")

    if note["topic"] == "plays":
        user = m["user"]
        watched[user].add(m["video_id"])
        liked[user][m["genre"]] += 1
        resp = call_metadata(log, NAME, "LIST", {}, req_id)  
        if not resp or resp["status"] != "ok":
            return
        titles = resp["data"]["titles"]
        fav = liked[user].most_common(1)[0][0]
        picks = [t for t in titles if t["genre"] == fav and t["video_id"] not in watched[user]]
        reason = f"because you watch {fav}"
        if not picks:  
            picks = sorted((t for t in titles if t["video_id"] not in watched[user]),
                           key=lambda t: -t["year"])[:2]
            reason = "newest titles you haven't seen"
        if picks:
            publish(f"recs.{user}", {"user": user, "titles": [t["title"] for t in picks],
                                     "reason": reason}, req_id)

    elif note["topic"] == "new-releases":
        for user, genres in liked.items():
            if m["genre"] in genres and m["video_id"] not in watched[user]:
                publish(f"recs.{user}", {"user": user, "titles": [m["title"]],
                                         "reason": f"new {m['genre']} release"}, req_id)


if __name__ == "__main__":
    log.info(f"STARTED, subscribing to broker at {BROKER}")
    while True:
        subscribe(log, NAME, BROKER, ["plays", "new-releases"], on_message)
        time.sleep(1.0)  
