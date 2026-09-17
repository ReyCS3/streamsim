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

from config import REQUEST_TIMEOUT

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")    # message logs go here
DATA_DIR = os.path.join(BASE_DIR, "data")   # chunks + catalog go here


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
    req_id = req_id or uuid.uuid4().hex[:8]  # new id unless we're forwarding one
    msg = {"type": msg_type, "req_id": req_id, "sender": sender, "payload": payload or {}}
    if not quiet:
        log_msg(logger, "SEND ->", peer_name, req_id, type=msg_type, payload=msg["payload"])
    try:
        with socket.create_connection(addr, timeout=timeout) as sock:
            sock.sendall((json.dumps(msg) + "\n").encode())
            line = sock.makefile("rb").readline()  # wait for the reply line
            if not line:
                raise ConnectionError("connection closed by peer")
            resp = json.loads(line)
    except (OSError, ValueError) as e:  # refused, reset, timeout, or bad JSON
        if not quiet:
            log_msg(logger, "FAIL xx", peer_name, req_id, type=msg_type, error=str(e))
        return None
    if not quiet:
        log_msg(logger, "RECV <-", peer_name, req_id, status=resp["status"], data=resp.get("data"))
    return resp


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
