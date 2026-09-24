"""Humsync - self-hosted multi-device music sync server.

Flow:
  1. A device creates a room and becomes the controller (admin).
  2. Other devices join the room with its code.
  3. The controller searches YouTube / pastes a YouTube URL / uploads a local
     file. Tracks are downloaded once on the server and streamed to everyone.
  4. Every action (play / pause / seek / volume) is scheduled at a common
     future "server time" and executed by every client simultaneously.

Clock model (mirrors the reference beatsync architecture):
  - The server owns the source of truth for playback:
        playback = {type, sourceId, position (s), at (server clock ms)}
    While playing: currentPosition = position + (now - at)/1000.
  - Clients measure their clock offset to the server via an NTP handshake and
    estimate offsets using min-RTT selection.
  - The server waits for all clients to finish buffering a new source before
    broadcasting a play action (with a timeout fallback), and computes a
    dynamic schedule delay from the max reported client RTT.
"""

import asyncio
import base64
import io
import json
import os
import secrets
import time
from pathlib import Path

import httpx

import uvicorn
from fastapi import (
    FastAPI,
    File,
    HTTPException,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from providers import (
    CACHE_DIR,
    download_youtube,
    guess_mime,
    is_youtube_url,
    related_youtube,
    resolve_youtube_url,
    search_youtube_cached,
)

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
UPLOAD_DIR = BASE_DIR / "uploads"

HOST_ROOM_TTL_MS = 60_000        # host drops -> autofailover after this long
MAX_CLIENTS_PER_ROOM = 12
WS_BROADCAST_QUIET_TTL_MS = 400  # coalesce identical progress echoes
RESCHEDULE_EPSILON_S = 0.05
RESET_POSITION_EPSILON_S = 0.08

# ---------------------------------------------------------------------------
# Time utils (server clock)
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def clamp_pos(pos: float, dur: float) -> float:
    return min(max(0.0, pos), max(0.0, dur))


# ---------------------------------------------------------------------------
# Room model
# ---------------------------------------------------------------------------

class Room:
    """A live room: clients, queue, playback state, and network-clock sync."""

    def __init__(self, room_id: str):
        self.id = room_id
        self.clients: dict[str, dict] = {}       # client_id -> client info
        self.queue: list[str] = []               # source ids in play order
        self.sources: dict[str, dict] = {}      # source_id -> metadata
        self.playback = {
            "type": None,         # "none" | "youtube" | "local"
            "sourceId": None,
            "position": 0.0,
            "at": 0,
            "playing": False,
        }
        self._lock = asyncio.Lock()
        self._pending_play = None
        self._host_id = None
        self._host_seen_ms = now_ms()
        self.sockets: dict = {}            # client_id -> WebSocket (live conns)
        self.global_volume: float = 1.0

    # -- admin / host ------------------------------------------------

    @property
    def host_id(self) -> str | None:
        return self._host_id

    def set_host(self, client_id: str | None) -> None:
        self._host_id = client_id
        self._host_seen_ms = now_ms() if client_id else now_ms()

    def touch_host(self) -> None:
        if self._host_id:
            self._host_seen_ms = now_ms()

    def host_stale(self) -> bool:
        return bool(self._host_id) and (now_ms() - self._host_seen_ms) > HOST_ROOM_TTL_MS

    # -- clients -----------------------------------------------------

    def add_client(self, client_id: str, name: str, is_admin: bool) -> dict:
        info = {
            "client_id": client_id,
            "name": name,
            "is_admin": is_admin,
            "last_seen": now_ms(),
            "rtt_ms": 0,
            "clock_offset_ms": 0,
            "clock_samples": [],
        }
        self.clients[client_id] = info
        if is_admin and self._host_id is None:
            self.set_host(client_id)
        return info

    def remove_client(self, client_id: str) -> dict | None:
        info = self.clients.pop(client_id, None)
        if self._host_id == client_id:
            # failover: promote the most recently seen client
            if self.clients:
                newest = max(self.clients.values(), key=lambda c: c["last_seen"])
                newest["is_admin"] = True
                self.set_host(newest["client_id"])
            else:
                self.set_host(None)
        return info

    def get_client(self, client_id: str) -> dict | None:
        return self.clients.get(client_id)

    # -- sources / queue ---------------------------------------------

    def add_source(self, source) -> None:
        sid = source.get("id") or source.get("source_id")
        if not sid:
            return
        if sid in self.sources:
            return
        self.sources[sid] = source
        if sid not in self.queue:
            self.queue.append(sid)

    def get_source(self, source_id: str) -> dict | None:
        return self.sources.get(source_id)

    def remove_source(self, source_id: str) -> None:
        self.sources.pop(source_id, None)
        if source_id in self.queue:
            self.queue.remove(source_id)
        if self.playback.get("sourceId") == source_id:
            self.playback.update({"type": None, "sourceId": None, "playing": False})

    def public_source(self, source) -> dict:
        s = dict(source)
        s.setdefault("id", s.get("source_id", ""))
        s.setdefault("title", s.get("name", ""))
        return s

    # -- state broadcast ---------------------------------------------

    def public_state(self) -> dict:
        return {
            "roomId": self.id,
            "clients": [
                {"id": c["client_id"], "name": c["name"], "isAdmin": c["is_admin"]}
                for c in self.clients.values()
            ],
            "queue": list(self.queue),
            "sources": {sid: self.public_source(s) for sid, s in self.sources.items()},
            "playback": dict(self.playback),
            "globalVolume": self.global_volume,
        }

    def public_sources_list(self) -> list[dict]:
        return [self.public_source(s) for s in self.sources.values()]

    # -- send helpers (no asyncio send: simple direct websocket list) --

    def send_to(self, client_id: str, payload: dict) -> None:
        pass  # real send implemented by the room connection mgr

    def send_to_all(self, payload: dict) -> None:
        pass


# ---------------------------------------------------------------------------
# Request models (kept tiny; mostly plain dicts over WS)
# ---------------------------------------------------------------------------

class CreateRoomRequest:
    pass


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Humsync")

ROOMS: dict[str, Room] = {}


def _create_room() -> Room:
    # Uppercase-only unambiguous alphabet (no 0/O/1/I): matches the
    # index.html join box (maxlength=6, uppercases input) so typed codes
    # always match. 6 chars.
    alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
    while True:
        rid = "".join(secrets.choice(alphabet) for _ in range(6))
        if rid not in ROOMS:
            break
    room = Room(rid)
    ROOMS[rid] = room
    return room


def _cleanup_empty():
    for rid in [r for r, room in ROOMS.items() if not room.clients]:
        ROOMS.pop(rid, None)


def _room_or_404(room_id: str) -> Room:
    room = ROOMS.get(room_id)
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    return room


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------

@app.get("/")
async def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/room")
async def room_page_query():
    # phone app.js `location.href = "/room?roomId=XYZ"` ke liye — query-string
    # path `/room` ko bhi room.html serve karo (roomId query se app.js leta hai)
    return FileResponse(STATIC_DIR / "room.html")


@app.get("/room/{room_id}")
async def room_page(room_id: str):
    return FileResponse(STATIC_DIR / "room.html")


@app.post("/api/rooms")
async def create_room():
    room = _create_room()
    return {"roomId": room.id}


@app.post("/api/rooms/join")
async def join_room(payload: dict):
    room_id = payload.get("roomId", "")
    room = _room_or_404(room_id)
    return {"roomId": room.id, "queue": room.queue, "playback": room.playback}


@app.post("/api/upload")
async def upload(room_id: str = "", file: UploadFile = File(...)):
    room = _room_or_404(room_id)
    data = await file.read()
    ext = os.path.splitext(file.filename or "audio")[1] or ".mp3"
    src_id = "local-" + secrets.token_hex(4)
    UPLOAD_DIR.mkdir(exist_ok=True)
    path = UPLOAD_DIR / (src_id + ext)
    path.write_bytes(data)
    room.add_source(
        {
            "id": src_id,
            "type": "local",
            "title": file.filename or src_id,
            "uploader": "upload",
            "url": f"/uploads/{path.name}",
            "duration": 0,
            "mime": guess_mime(ext) or "audio/mpeg",
        }
    )
    await _broadcast_state(room)
    return {"sourceId": src_id}


@app.get("/api/stream/{source_id}")
async def stream_source(source_id: str):
    """Serve audio bytes for a queued source (Range-capable via FileResponse).

    Local uploads and already-downloaded YouTube tracks stream instantly.
    A YouTube id that was queued but never downloaded is fetched on demand
    (first play takes a few seconds while yt-dlp downloads).
    """
    safe = "".join(c for c in source_id if c.isalnum() or c in ("-", "_")).strip()
    if not safe:
        raise HTTPException(status_code=404, detail="Audio not found")
    for path in sorted(CACHE_DIR.glob(f"{safe}.*")):
        if not path.is_file():
            continue
        if path.suffix.lower() in (".part", ".ytdl"):
            continue
        if path.stat().st_size == 0:
            continue
        return FileResponse(path, media_type=guess_mime(path.suffix))
    # not cached: only YouTube ids can be materialized on demand
    if safe.startswith("local-"):
        raise HTTPException(status_code=404, detail="Audio not found")
    loop = asyncio.get_running_loop()
    try:
        info = await loop.run_in_executor(None, download_youtube, safe)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Download failed: {exc}")
    return FileResponse(info["path"], media_type=guess_mime(info.get("ext", "")))


_LYRICS_CACHE: dict[str, dict] = {}


@app.get("/api/lyrics")
async def get_lyrics(title: str = "", artist: str = ""):
    """Lyrics for the now-playing track via lrclib (free, no key).

    Returns {found, lyrics (plain), synced (LRC or null), title, artist}.
    Results are cached in-memory per title+artist.
    """
    title, artist = title.strip(), artist.strip()
    if not title:
        return {"found": False, "lyrics": "", "synced": None}
    key = f"{artist}\n{title}".lower()
    if key in _LYRICS_CACHE:
        return _LYRICS_CACHE[key]
    out: dict = {"found": False, "lyrics": "", "synced": None,
                 "title": title, "artist": artist}
    try:
        async with httpx.AsyncClient(
            timeout=12.0, headers={"User-Agent": "Humsync/1.0"}
        ) as client:
            data = None
            r = await client.get(
                "https://lrclib.net/api/get",
                params={"artist_name": artist, "track_name": title},
            )
            if r.status_code == 200:
                data = r.json()
            else:
                s = await client.get(
                    "https://lrclib.net/api/search",
                    params={"q": f"{title} {artist}".strip()},
                )
                if s.status_code == 200:
                    items = s.json()
                    data = items[0] if items else None
            if data:
                plain = data.get("plainLyrics") or ""
                synced = data.get("syncedLyrics") or None
                if plain or synced:
                    out = {
                        "found": True,
                        "lyrics": plain,
                        "synced": synced,
                        "title": data.get("trackName") or title,
                        "artist": data.get("artistName") or artist,
                    }
    except Exception:
        pass
    _LYRICS_CACHE[key] = out
    return out


# materialize a single source descriptor for the search/queue UI
def _with_url(source: dict) -> dict:
    s = dict(source)
    return s


async def _add_youtube(meta: dict) -> None:
    # meta from providers.search / resolve -> room.add_source maintained by ws mgr
    pass


# ---------------------------------------------------------------------------
# WebSocket room
# ---------------------------------------------------------------------------

def _public_client(cli: dict) -> dict:
    return {"id": cli["client_id"], "name": cli["name"], "isAdmin": cli["is_admin"]}


async def _broadcast_state(room: Room) -> None:
    """Send full public state to every connected socket in the room."""
    if not room.sockets:
        return
    payload = json.dumps({"type": "state", "room": room.public_state()})
    dead = []
    for cid, ws in list(room.sockets.items()):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(cid)
    for cid in dead:
        room.sockets.pop(cid, None)


_DOWNLOADING: set[str] = set()

async def _ensure_downloaded(source_id: str) -> None:
    """Download a YouTube track in background right at queue-add time.

    First play then hits the local cache instead of racing a ~10s download
    (the race is why users had to mash play/pause). Broadcasts state when
    done so clients refresh instantly.
    """
    if not source_id or source_id.startswith("local-"):
        return
    for path in sorted(CACHE_DIR.glob(f"{source_id}.*")):
        if not path.is_file():
            continue
        if path.suffix.lower() in (".part", ".ytdl"):
            continue
        if path.stat().st_size == 0:
            continue
        return
    if source_id in _DOWNLOADING:
        return
    _DOWNLOADING.add(source_id)
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, download_youtube, source_id)
    except Exception:
        pass
    finally:
        _DOWNLOADING.discard(source_id)
    for room in list(ROOMS.values()):
        if source_id in room.sources:
            await _broadcast_state(room)


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await websocket.accept()
    room = ROOMS.setdefault(room_id, Room(room_id))
    client_id = secrets.token_hex(8)
    body = {}
    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            mtype = msg.get("type", "")
            print(f"[WS:{room_id}:{client_id[:6]}] {mtype}", flush=True)

            if mtype == "join":
                body = {
                    "client_id": client_id,
                    "name": msg.get("name") or msg.get("username", "Guest"),
                    "is_admin": bool(msg.get("isAdmin", False)) or (room and room.host_id is None),
                }
                room.add_client(client_id, body["name"], body["is_admin"])
                room.sockets[client_id] = websocket
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "welcome",
                            "yourId": client_id,
                            "clientId": client_id,
                            "roomId": room.id,
                            "isAdmin": body["is_admin"],
                            "queue": room.queue,
                            "playback": room.playback,
                            "serverTime": now_ms(),
                            "hostId": room.host_id,
                        }
                    )
                )
                await _broadcast_state(room)
                continue

            if mtype == "search":
                peer = room.clients.get(client_id)
                if not peer or not peer.get("is_admin"):
                    continue
                query = msg.get("query", "")
                if not query.strip():
                    continue
                loop = asyncio.get_running_loop()
                try:
                    results = await asyncio.wait_for(
                        loop.run_in_executor(None, search_youtube_cached, query),
                        timeout=30,
                    )
                except asyncio.TimeoutError:
                    await websocket.send_text(
                        json.dumps({"type": "search-results", "results": [], "timedOut": True})
                    )
                    continue
                except Exception as exc:  # noqa: BLE001
                    await websocket.send_text(
                        json.dumps({"type": "search-results", "results": [], "error": str(exc)[:120]})
                    )
                    continue
                await websocket.send_text(
                    json.dumps({"type": "search-results", "results": results})
                )
                continue

            if mtype == "related":
                if not body.get("is_admin"):
                    continue
                source_id = msg.get("sourceId", "")
                if not source_id:
                    continue
                loop = asyncio.get_running_loop()
                try:
                    results = await loop.run_in_executor(None, related_youtube, source_id)
                except Exception as exc:  # noqa: BLE001
                    await websocket.send_text(
                        json.dumps({"type": "error", "message": f"Related failed: {exc}"})
                    )
                    continue
                await websocket.send_text(
                    json.dumps({"type": "search-results", "results": results})
                )
                continue

            if mtype == "use-result":
                if not body.get("is_admin"):
                    continue
                meta = msg.get("source", {})
                src_id = meta.get("id") or meta.get("source_id")
                if src_id:
                    room.add_source(dict(meta))
                    await _broadcast_state(room)
                    asyncio.create_task(_ensure_downloaded(src_id))
                continue

            if mtype == "add-source":
                if not body.get("is_admin"):
                    continue
                url = msg.get("url", "")
                if not is_youtube_url(url):
                    continue
                loop = asyncio.get_running_loop()
                try:
                    meta = await loop.run_in_executor(None, resolve_youtube_url, url)
                except Exception as exc:  # noqa: BLE001
                    await websocket.send_text(
                        json.dumps({"type": "error", "message": f"Resolve failed: {exc}"})
                    )
                    continue
                room.add_source(meta)
                await _broadcast_state(room)
                _sid = meta.get("id") or meta.get("source_id") or ""
                if _sid:
                    asyncio.create_task(_ensure_downloaded(_sid))
                continue

            if mtype == "remove-source":
                if not body.get("is_admin"):
                    continue
                room.remove_source(msg.get("sourceId", ""))
                await _broadcast_state(room)
                continue

            if mtype == "play":
                if not body.get("is_admin"):
                    continue
                source_id = msg.get("sourceId", "")
                position = float(msg.get("position", 0))
                if room.get_source(source_id):
                    room.playback.update(
                        {
                            "type": "youtube",
                            "sourceId": source_id,
                            "position": position,
                            "at": now_ms(),
                            "playing": True,
                        }
                    )
                    await _broadcast_state(room)
                continue

            if mtype == "pause":
                if not body.get("is_admin"):
                    continue
                room.playback["playing"] = False
                await _broadcast_state(room)
                continue

            if mtype == "seek":
                if not body.get("is_admin"):
                    continue
                pos = float(msg.get("position", 0))
                if room.playback.get("sourceId"):
                    room.playback["position"] = clamp_pos(pos, 10**9)
                    room.playback["at"] = now_ms()
                    await _broadcast_state(room)
                continue

            if mtype == "sync-echo":
                # clock estimation round-trip: client sends t1,t2,t3,t4
                t1 = msg.get("t1", 0)
                t2 = msg.get("t2", 0)
                t3 = msg.get("t3", 0)
                t4 = now_ms()  # server receives now
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "sync-echo",
                            "t1": t1,
                            "t2": t2,
                            "t3": t3,
                            "t4": t4,
                        }
                    )
                )
                continue

            if mtype == "ping":
                await websocket.send_text(
                    json.dumps({"type": "pong", "serverTime": now_ms()})
                )
                continue

            if mtype == "ntp":
                # client clock-calibration round-trip (app.js sendPing)
                t0 = msg.get("t0", 0)
                t1 = now_ms()
                await websocket.send_text(
                    json.dumps({"type": "ntp", "t0": t0, "t1": t1, "t2": now_ms()})
                )
                continue

            if mtype == "prev" or mtype == "next":
                peer = room.clients.get(client_id)
                if not peer or not peer.get("is_admin"):
                    continue
                q = room.queue
                cur = room.playback.get("sourceId")
                try:
                    idx = q.index(cur) if cur in q else (-1 if mtype == "prev" else 0)
                except ValueError:
                    idx = 0
                nxt = idx - 1 if mtype == "prev" else idx + 1
                nxt = max(0, min(nxt, len(q) - 1))
                if q and room.get_source(q[nxt]):
                    room.playback.update(
                        {
                            "type": "youtube",
                            "sourceId": q[nxt],
                            "position": 0.0,
                            "at": now_ms(),
                            "playing": True,
                        }
                    )
                    await _broadcast_state(room)
                continue

            if mtype == "global-volume":
                peer = room.clients.get(client_id)
                if not peer or not peer.get("is_admin"):
                    continue
                try:
                    room.global_volume = max(0.0, min(1.0, float(msg.get("volume", 1.0))))
                except (TypeError, ValueError):
                    continue
                await _broadcast_state(room)
                continue

    except WebSocketDisconnect:
        pass
    finally:
        room.sockets.pop(client_id, None)
        room.remove_client(client_id)
        if not room.clients:
            _cleanup_empty()
        else:
            await _broadcast_state(room)


# ---------------------------------------------------------------------------
# Static mounts
# ---------------------------------------------------------------------------
# NOTE: mounts must come AFTER all API routes. A "/" mount shadows anything
# registered after it (POST /api/... would 405). Keep this here, never above
# the route definitions.
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    # warm the youtube search cache so the very first phone search is ~0ms
    # (network cold start takes ~15s and would exceed the ws client timeout)
    def _warm_search_cache() -> None:
        for q in ("lofi hip hop", "chill beats", "study music"):
            try:
                search_youtube_cached(q, limit=5)
            except Exception:
                pass

    import threading as _threading

    _threading.Thread(target=_warm_search_cache, daemon=True).start()

    uvicorn.run("server:app", host="0.0.0.0", port=port, log_level="info")
