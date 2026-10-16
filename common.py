# Shared code: socket messaging + logging. Every process imports this.

# Message format (one JSON object per line over TCP):
#   request:  {"type": "SEARCH", "req_id": "ab12cd34", "sender": "client", "payload": {...}}
#   response: {"req_id": "ab12cd34", "status": "ok" or "error", "data": {...}}

# req_id stays the same as a request moves between services,
# so you can grep one id to follow a request through every log file.

import json
import logging
import os
import socket
import socketserver
import sys
import uuid
import threading
import time

from config import REQUEST_TIMEOUT

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")    # message logs go here
DATA_DIR = os.path.join(BASE_DIR, "data")   # chunks + catalog go here

# ---------- Marshalling ----------
# Marshal: Python dict -> bytes on the wire (JSON text + "\n" so the reader knows where it ends).
# Unmarshal: one line of bytes -> Python dict. Raises ValueError on malformed input.
# Raw bytes (video chunks) can't go in JSON, so they're base64-encoded into strings first.

def marshal(msg):
    return (json.dumps(msg) + "\n").encode()


def unmarshal(line):
    msg = json.loads(line)
    if not isinstance(msg, dict):
        raise ValueError("message must be a JSON object")
    return msg


def new_id():
    return uuid.uuid4().hex[:8]


# Logger that prints to the terminal AND writes to logs/<name>.log
def get_logger(name):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:  # already set up
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d %(name)-8s %(message)s", "%H:%M:%S")
    for h in (logging.StreamHandler(sys.stdout),
              logging.FileHandler(os.path.join(LOG_DIR, f"{name}.log"), mode="a")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


# Shortens big values (chunk bytes, hashes, long lists) so log lines stay readable
def _brief(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "data" and isinstance(v, str):
                out[k] = f"<{len(v)} b64 chars>"
            elif k == "sha256" and isinstance(v, str):
                out[k] = v[:10] + "..."
            else:
                out[k] = _brief(v)
        return out
    if isinstance(obj, list):
        if len(obj) <= 4:
            return [_brief(x) for x in obj]
        return [_brief(x) for x in obj[:4]] + [f"...+{len(obj)-4}"]
    return obj


# Writes one log line for a message
# direction is "SEND ->", "RECV <-", or "FAIL xx"
def log_msg(logger, direction, peer, req_id, **fields):
    logger.info(f"{direction:<7} peer={peer:<8} id={req_id} " + json.dumps(_brief(fields)))


# Client side: connect to a service, send one request, wait for one response.
# Returns the response dict, or None if the service is down or too slow.
# quiet=True skips logging (used for heartbeats so logs don't get spammed).
def send_request(logger, sender, peer_name, addr, msg_type, payload=None,
                 req_id=None, timeout=REQUEST_TIMEOUT, quiet=False):
    req_id = req_id or new_id()  # new id unless we're forwarding one
    msg = {"type": msg_type, "req_id": req_id, "sender": sender, "payload": payload or {}}
    if not quiet:
        log_msg(logger, "SEND ->", peer_name, req_id, type=msg_type, payload=msg["payload"])
    try:
        with socket.create_connection(addr, timeout=timeout) as sock:
            sock.sendall(marshal(msg))
            line = sock.makefile("rb").readline()  # wait for the reply line
            if not line:
                raise ConnectionError("connection closed by peer")
            resp = unmarshal(line)
    except (OSError, ValueError) as e:  # refused, reset, timeout, or bad JSON
        if not quiet:
            log_msg(logger, "FAIL xx", peer_name, req_id, type=msg_type, error=str(e))
        return None
    if not quiet:
        log_msg(logger, "RECV <-", peer_name, req_id, status=resp["status"], data=resp.get("data"))
    return resp

# Send the same message to every member of a group at once (one thread per member)
# and collect the replies. group is {name: (host, port)}.
# Returns {name: response or None}. All copies share one req_id so the multicast is traceable.
def multicast(logger, sender, group, msg_type, payload=None, req_id=None,
              timeout=REQUEST_TIMEOUT, quiet=False):
    req_id = req_id or new_id()
    if not quiet:
        logger.info(f"MCAST   group={list(group)} id={req_id} type={msg_type}")
    results = {}

    def one(name, addr):
        results[name] = send_request(logger, sender, name, addr, msg_type, payload,
                                     req_id, timeout, quiet)

    threads = [threading.Thread(target=one, args=(n, a)) for n, a in group.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if not quiet:
        ok = [n for n, r in results.items() if r and r["status"] == "ok"]
        logger.info(f"MCAST   id={req_id} delivered to {len(ok)}/{len(group)} {ok}")
    return results


# Talking to the replicated metadata service

_meta = {"leader": None, "rr": 0}  # last known leader, round-robin counter for reads
_meta_lock = threading.Lock()


# Sends a request to the metadata replica group.
# - Reads (write=False) can go to ANY replica, so we rotate between them to spread load
#   and fail over to the next one if a replica is down.
# - Writes (write=True) must reach the leader. A non-leader replies {"error": "not leader",
#   "leader": <hint>}, so we follow the hint. If no leader is reachable (an election is
#   running), we back off and retry until WRITE_RETRY_SECONDS have passed.
# down = replicas we already suspect are down; they're tried last.
def call_metadata(logger, sender, msg_type, payload=None, req_id=None, write=False, down=()):
    req_id = req_id or new_id()
    names = list(METADATA_REPLICAS)
    deadline = time.time() + WRITE_RETRY_SECONDS
    backoff = 0.25
    while True:
        if write:
            hint = _meta["leader"]
            # last known leader first, then highest priority (most likely to win an election)
            order = sorted(names, key=lambda n: (n != hint, -names.index(n)))
        else:
            with _meta_lock:
                start = _meta["rr"] % len(names)
                _meta["rr"] += 1
            order = names[start:] + names[:start]
        order.sort(key=lambda n: n in down)  # stable sort: suspected-down replicas go last

        tried = set()
        while order:
            n = order.pop(0)
            if n in tried:
                continue
            tried.add(n)
            resp = send_request(logger, sender, n, METADATA_REPLICAS[n], msg_type, payload, req_id)
            if resp is None:
                continue
            if resp["status"] == "error" and resp["data"].get("error") == "not leader":
                hint = resp["data"].get("leader")
                if hint in METADATA_REPLICAS and hint not in tried:
                    order.insert(0, hint)  # go straight to the leader it pointed us at
                continue
            if write:
                _meta["leader"] = n
            return resp

        if not write or time.time() + backoff > deadline:
            return None
        logger.info(f"RETRY   id={req_id} no metadata leader reachable, backing off {backoff:.2f}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 2.0)


# Pub-sub (subscriber side)
# Opens a long-lived connection to the broker, subscribes to topics, then calls
# on_message(note) for every NOTIFY the broker pushes.
# Runs for `duration` seconds (None = until the connection drops).
# Returns True if it ran for the full duration, False if the broker was unreachable or dropped us.
def subscribe(logger, sender, addr, topics, on_message, duration=None, replay=False):
    req_id = new_id()
    payload = {"topics": topics, "subscriber": sender, "replay": replay}
    log_msg(logger, "SEND ->", "broker", req_id, type="SUBSCRIBE", payload=payload)
    sock = None
    try:
        sock = socket.create_connection(addr, timeout=REQUEST_TIMEOUT)
        sock.sendall(marshal({"type": "SUBSCRIBE", "req_id": req_id, "sender": sender,
                              "payload": payload}))
        f = sock.makefile("rb")
        line = f.readline()
        if not line:
            raise ConnectionError("connection closed by broker")
        resp = unmarshal(line)
        log_msg(logger, "RECV <-", "broker", req_id, status=resp["status"], data=resp.get("data"))
        if resp["status"] != "ok":
            return False

        deadline = time.time() + duration if duration else None
        while True:
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return True
                sock.settimeout(remaining)
            else:
                sock.settimeout(None)  # wait as long as it takes for the next message
            try:
                line = f.readline()
            except socket.timeout:
                return True
            if not line:
                raise ConnectionError("broker closed the connection")
            note = unmarshal(line)
            log_msg(logger, "RECV <-", "broker", note.get("req_id", "?"), type=note.get("type"),
                    topic=note.get("topic"), msg_id=note.get("msg_id"), message=note.get("payload"))
            try:
                on_message(note)
            except Exception as e:  # a bad notification shouldn't kill the subscriber
                logger.info(f"ERROR   handling notification: {type(e).__name__}: {e}")
    except (OSError, ValueError) as e:
        log_msg(logger, "FAIL xx", "broker", req_id, type="SUBSCRIBE", error=str(e))
        return False
    finally:
        if sock:
            sock.close()


# Server side: runs once per incoming connection.
# Reads the request, logs it, passes it to the service's route() function,
# then logs and sends back the response.
class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        srv = self.server
        line = self.rfile.readline()
        if not line:
            return
        msg = json.loads(line)
        req_id, sender, mtype = msg.get("req_id", "?"), msg.get("sender", "?"), msg.get("type")
        quiet = mtype == "HEALTH"  # don't log heartbeats
        if not quiet:
            log_msg(srv.logger, "RECV <-", sender, req_id, type=mtype, payload=msg.get("payload"))
        try:
            status, data = srv.route(mtype, msg.get("payload") or {}, req_id)
        except Exception as e:  # a bad request shouldn't crash the whole server
            status, data = "error", {"error": f"{type(e).__name__}: {e}"}
        if not quiet:
            log_msg(srv.logger, "SEND ->", sender, req_id, status=status, data=data)
        reply = {"req_id": req_id, "status": status, "data": data}
        self.wfile.write((json.dumps(reply) + "\n").encode())


# Threaded server so each connection gets its own thread (handles many clients at once)
class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True  # lets you restart quickly without "address in use"
    daemon_threads = True       # threads die when the process exits


# Creates a server for a service.
# route(msg_type, payload, req_id) must return (status, data).
def make_server(addr, name, route):
    srv = _Server(addr, _Handler)
    srv.logger = get_logger(name)
    srv.route = route
    return srv
