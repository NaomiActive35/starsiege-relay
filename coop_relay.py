"""Star Siege CO-OP relay (pure standard library).

A tiny "mailbox" between two players.  It does NOT run the game: the host's game simulates the
enemies and the relay only passes small JSON messages between the two phones/PCs.

Endpoints (all JSON, all POST except /ping)
    GET  /api/coop/ping                       -> {"ok":true,"rooms":N}
    POST /api/coop/create  {name,ship,public} -> {"room":"ABCD","pid":"..","role":"host"}
    POST /api/coop/join    {room,name,ship}   -> {"room":"ABCD","pid":"..","role":"guest","peer":{..}}
    POST /api/coop/quick   {name,ship}        -> joins any waiting public room, else makes one
    POST /api/coop/sync    {room,pid,me,state,sseq,evs,ack,have}  -> the other player's state + events
    POST /api/coop/leave   {room,pid}

Three ways to run it
    1. Standalone:        python coop_relay.py            (PORT env var, default 8001)
    2. Inside server.py:  server.py already mounts it automatically when this file sits next to it.
    3. WSGI (PythonAnywhere, gunicorn...):   from coop_relay import application
       Flask app?  Call  coop_relay.handle(method, path, body_dict, ip)  from a catch-all route.

IMPORTANT: rooms live in memory, so run ONE worker/process (threads are fine).
"""
import json, os, random, threading, time

CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ"        # no I / O: easier to read out loud
CODE_LEN = 4
PEER_TIMEOUT = 5.0                             # seconds without a sync = that player is gone
ROOM_TTL = 120.0                               # an abandoned room is deleted after this long
MAX_BODY = 48 * 1024
MAX_EVENTS_QUEUED = 400
MAX_ROOMS = 500
CREATE_LIMIT = (30, 60)                        # create/join/quick calls per IP per 60 s
API_KEY = os.environ.get("API_KEY", "")

_lock = threading.Lock()
_rooms = {}                                    # code -> Room
_hits = {}                                     # ip -> [timestamps]


def clean_name(raw):
    out = "".join(c for c in str(raw) if c.isalnum() or c in " _.!-").strip()[:12]
    return out or "PILOT"


class Player:
    def __init__(self, name, ship):
        self.pid = "%032x" % random.getrandbits(128)
        self.name = clean_name(name)
        self.ship = str(ship)[:12]
        self.seen = time.time()
        self.state = None            # latest state blob (overwritten every sync)
        self.sseq = 0
        self.evlog = []              # events this player sent that the peer has not acked yet
        self.last_in = 0             # highest event id received from this player


class Room:
    def __init__(self, code, public):
        self.code = code
        self.public = public
        self.created = time.time()
        self.players = []            # [host, guest]
        self.closed = False

    def other(self, p):
        for q in self.players:
            if q is not p:
                return q
        return None

    def alive(self, p, now):
        return now - p.seen < PEER_TIMEOUT


def _rate_ok(ip):
    now = time.time()
    n, window = CREATE_LIMIT
    recent = [t for t in _hits.get(ip, []) if now - t < window]
    ok = len(recent) < n
    if ok:
        recent.append(now)
    _hits[ip] = recent
    return ok


def _sweep(now):
    for code in [c for c, r in _rooms.items()
                 if r.closed or not r.players or all(now - p.seen > ROOM_TTL for p in r.players)]:
        _rooms.pop(code, None)
    if len(_hits) > 2000:
        _hits.clear()


def _new_code():
    for _ in range(200):
        code = "".join(random.choice(CODE_CHARS) for _ in range(CODE_LEN))
        if code not in _rooms:
            return code
    return None


def _peer_info(room, me, now):
    other = room.other(me)
    if other is None:
        return {"present": False}
    return {"present": room.alive(other, now), "name": other.name, "ship": other.ship}


def _create(body, public=None):
    if len(_rooms) >= MAX_ROOMS:
        return 503, {"error": "server busy"}
    code = _new_code()
    if not code:
        return 503, {"error": "server busy"}
    room = Room(code, bool(body.get("public") if public is None else public))
    me = Player(body.get("name"), body.get("ship", ""))
    room.players.append(me)
    _rooms[code] = room
    return 200, {"room": code, "pid": me.pid, "role": "host", "peer": {"present": False}}


def _join_room(room, body):
    now = time.time()
    if room.closed:
        return 404, {"error": "no such room"}
    live = [p for p in room.players if room.alive(p, now)]
    if len(room.players) >= 2 and len(live) >= 2:
        return 409, {"error": "room is full"}
    if len(room.players) >= 2:                       # a dead guest slot: reuse it
        room.players = [room.players[0]]
    me = Player(body.get("name"), body.get("ship", ""))
    room.players.append(me)
    return 200, {"room": room.code, "pid": me.pid, "role": "guest", "peer": _peer_info(room, me, now)}


def _find(body):
    room = _rooms.get(str(body.get("room", "")).upper())
    if not room:
        return None, None
    pid = str(body.get("pid", ""))
    for p in room.players:
        if p.pid == pid:
            return room, p
    return room, None


def _sync(body):
    room, me = _find(body)
    if room is None or room.closed:
        return 404, {"error": "room closed"}
    if me is None:
        return 403, {"error": "not in this room"}
    now = time.time()
    me.seen = now
    if isinstance(body.get("me"), dict):
        me.name = clean_name(body["me"].get("name", me.name))
        me.ship = str(body["me"].get("ship", me.ship))[:12]
    st = body.get("state")
    try:
        sseq = int(body.get("sseq", 0))
    except (TypeError, ValueError):
        sseq = 0
    if isinstance(st, dict) and sseq >= me.sseq:
        me.state, me.sseq = st, sseq
    evs = body.get("evs")
    if isinstance(evs, list):
        for ev in evs:
            if isinstance(ev, dict) and isinstance(ev.get("i"), int) and ev["i"] > me.last_in:
                if len(me.evlog) < MAX_EVENTS_QUEUED:
                    me.evlog.append(ev)
                me.last_in = ev["i"]
    other = room.other(me)
    out = {"peer": _peer_info(room, me, now), "ack_in": me.last_in, "evs": [], "state": None, "sseq": 0}
    if other is not None:
        try:
            ack = int(body.get("ack", 0))
        except (TypeError, ValueError):
            ack = 0
        other.evlog = [e for e in other.evlog if e["i"] > ack]    # the peer confirmed these
        out["evs"] = other.evlog[:80]
        try:
            have = int(body.get("have", -1))
        except (TypeError, ValueError):
            have = -1
        if other.state is not None and other.sseq != have:
            out["state"], out["sseq"] = other.state, other.sseq
    out["t"] = round(now, 3)
    return 200, out


def _leave(body):
    room, me = _find(body)
    if room and me:
        if room.players and room.players[0] is me:
            room.closed = True                        # host left: the room is over
        else:
            room.players = [p for p in room.players if p is not me]
    return 200, {"ok": True}


def handle(method, path, body, ip="?", key=""):
    """-> (status, dict) for /api/coop/*, or None when the path is not ours."""
    if not path.startswith("/api/coop/"):
        return None
    if API_KEY and key != API_KEY:
        return 401, {"error": "bad key"}
    route = path[len("/api/coop/"):].strip("/")
    body = body if isinstance(body, dict) else {}
    with _lock:
        _sweep(time.time())
        if route == "ping":
            return 200, {"ok": True, "rooms": len(_rooms)}
        if method != "POST":
            return 405, {"error": "POST only"}
        if route == "sync":
            return _sync(body)
        if route == "leave":
            return _leave(body)
        if route in ("create", "join", "quick"):
            if not _rate_ok(ip):
                return 429, {"error": "slow down"}
            if route == "create":
                return _create(body)
            if route == "join":
                room = _rooms.get(str(body.get("room", "")).strip().upper())
                if not room:
                    return 404, {"error": "no such room"}
                return _join_room(room, body)
            now = time.time()                                       # quick match
            for room in _rooms.values():
                if (room.public and not room.closed and len(room.players) == 1
                        and room.alive(room.players[0], now)):
                    return _join_room(room, body)
            return _create(body, public=True)
    return 404, {"error": "not found"}


# ------------------------------------------------------------------------------ WSGI
def application(environ, start_response):
    method = environ.get("REQUEST_METHOD", "GET")
    path = environ.get("PATH_INFO", "")
    ip = (environ.get("HTTP_X_FORWARDED_FOR") or environ.get("REMOTE_ADDR") or "?").split(",")[0].strip()
    body = {}
    if method == "POST":
        try:
            size = int(environ.get("CONTENT_LENGTH") or 0)
            if size > MAX_BODY:
                raise ValueError
            body = json.loads(environ["wsgi.input"].read(size) or b"{}")
        except (ValueError, KeyError):
            return _wsgi(start_response, 400, {"error": "bad json"})
    if method == "OPTIONS":
        return _wsgi(start_response, 204, {})
    res = handle(method, path, body, ip, environ.get("HTTP_X_GAME_KEY", ""))
    return _wsgi(start_response, *(res or (404, {"error": "not found"})))


def _wsgi(start_response, code, obj):
    data = json.dumps(obj).encode()
    start_response("%d %s" % (code, "OK" if code < 400 else "ERR"),
                   [("Content-Type", "application/json"), ("Content-Length", str(len(data))),
                    ("Access-Control-Allow-Origin", "*"),
                    ("Access-Control-Allow-Headers", "Content-Type, X-Game-Key"),
                    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS")])
    return [data]


# ------------------------------------------------------------------------------ standalone
if __name__ == "__main__":
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"                 # keep-alive: one TCP/TLS connection for a whole match
        disable_nagle_algorithm = True                # tiny messages: send at once

        def _go(self, method):
            path = self.path.split("?")[0]
            body = {}
            if method == "POST":
                try:
                    size = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(size) or b"{}") if 0 <= size <= MAX_BODY else None
                except (ValueError, UnicodeDecodeError):
                    body = None
                if body is None:
                    return self._send(400, {"error": "bad json"})
            ip = (self.headers.get("X-Forwarded-For") or self.client_address[0]).split(",")[0].strip()
            res = handle(method, path, body, ip, self.headers.get("X-Game-Key", ""))
            self._send(*(res or (404, {"error": "not found"})))

        def _send(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Game-Key")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._go("GET")

        def do_POST(self):
            self._go("POST")

        def do_OPTIONS(self):
            self._send(204, {})

        def log_message(self, fmt, *args):
            pass

    port = int(os.environ.get("PORT", 8001))
    print(f"Star Siege co-op relay on :{port}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
