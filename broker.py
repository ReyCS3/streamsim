# Pub-sub broker: publishers send messages to a topic, the broker pushes them to every
# subscriber of that topic. Publishers and subscribers never need to know about each other.

# SUBSCRIBE is a request-reply that then KEEPS THE CONNECTION OPEN: the broker pushes one
# NOTIFY line down it for every matching message until the subscriber disconnects.
# PUBLISH is a normal request-reply; the reply says how many subscribers got the message.
#
# Handles: HEALTH, SUBSCRIBE, PUBLISH

import collections
import threading

from common import log_msg, make_server, marshal
from config import BACKLOG_SIZE, BROKER

NAME = "broker"
lock = threading.Lock()          
publish_lock = threading.Lock()  
subscribers = collections.defaultdict(list)  
backlog = collections.defaultdict(lambda: collections.deque(maxlen=BACKLOG_SIZE))  # recent msgs
counter = [0]                    # msg_id for the next message


# One connected subscriber (its open socket)
class Subscriber:
    def __init__(self, name, wfile, topics):
        self.name = name
        self.wfile = wfile
        self.topics = topics
        self.lock = threading.Lock()  


def remove(sub):
    with lock:
        for t in sub.topics:
            if sub in subscribers[t]:
                subscribers[t].remove(sub)


# Push one message to one subscriber. Returns True if it was written to the socket.
def deliver(log, sub, note):
    try:
        with sub.lock:
            sub.wfile.write(marshal(note))
            sub.wfile.flush()
    except OSError as e:  # subscriber went away: drop it
        log_msg(log, "FAIL xx", sub.name, note["req_id"], type="NOTIFY", error=str(e))
        remove(sub)
        return False
    log_msg(log, "SEND ->", sub.name, note["req_id"], type="NOTIFY", topic=note["topic"],
            msg_id=note["msg_id"], message=note["payload"])
    return True


def route(mtype, p, req_id):
    log = srv.logger

    if mtype == "HEALTH":
        with lock:
            return "ok", {"topics": {t: [s.name for s in subs] for t, subs in subscribers.items()}}

    if mtype == "SUBSCRIBE":
        topics = p.get("topics") or []
        name = p.get("subscriber", "?")
        if not topics:
            return "error", {"error": "no topics given"}

        # Runs after the "ok" reply is sent; holds the connection open for pushes
        def stream(rfile, wfile):
            sub = Subscriber(name, wfile, topics)
            with lock:
                for t in topics:
                    subscribers[t].append(sub)
                old = [n for t in topics for n in backlog[t]] if p.get("replay") else []
            log.info(f"SUBS    {name} joined {topics}")
            for note in old:  # optional: catch up on recent messages
                deliver(log, sub, note)
            try:
                while rfile.readline():  # block until the subscriber disconnects
                    pass
            except OSError:
                pass
            finally:
                remove(sub)
                log.info(f"SUBS    {name} left {topics}")

        return "ok", {"topics": topics}, stream

    if mtype == "PUBLISH":
        topic = p["topic"]
        with publish_lock:
            with lock:
                counter[0] += 1
                note = {"type": "NOTIFY", "req_id": req_id, "msg_id": counter[0], "topic": topic,
                        "payload": p.get("message", {})}
                backlog[topic].append(note)
                targets = list(subscribers.get(topic, []))
            delivered = sum(deliver(log, s, note) for s in targets)
        return "ok", {"topic": topic, "msg_id": note["msg_id"], "delivered": delivered}

    return "error", {"error": f"unknown type {mtype}"}


srv = make_server(BROKER, NAME, route)

if __name__ == "__main__":
    srv.logger.info(f"STARTED on {BROKER}")
    srv.serve_forever()
