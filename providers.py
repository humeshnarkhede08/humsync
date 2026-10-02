"""Music providers for Humsync.

Supports two kinds of sources:
  - youtube : found via search or pasted URL, downloaded with yt-dlp
  - local   : files uploaded from any device

Every source is downloaded/serialized once on the server and served back
through a single /api/stream endpoint so that every device sees the exact
same bytes (important for identical buffering on every client).
"""
import mimetypes
import os
import re
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "uploads"

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()

def _load_api_key() -> str:
    """Return the YouTube Data API v3 key, falling back to the .env file."""
    global YOUTUBE_API_KEY
    if YOUTUBE_API_KEY:
        return YOUTUBE_API_KEY
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == "YOUTUBE_API_KEY":
                YOUTUBE_API_KEY = v.strip().strip('"').strip("'")
                break
    return YOUTUBE_API_KEY


class YoutubeDataAPIError(RuntimeError):
    pass


def search_youtube_api(query: str, key: str | None = None, limit: int = 8) -> list[dict]:
    """Search YouTube with the official Data API v3 (reliable, no bot-blocking).

    Returns lightweight track descriptors identical in shape to yt-dlp search,
    so callers can reuse the same code path either way. Requires an API key.
    """
    import httpx

    key = key or _load_api_key()
    if not key:
        raise RuntimeError("YouTube API key is not configured (set YOUTUBE_API_KEY or .env)")

    results: list[dict] = []
    with httpx.Client(timeout=15.0) as client:
        resp = client.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": min(limit, 25),
                "key": key,
            },
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"YouTube API search failed (HTTP {resp.status_code}): {resp.text[:200]}"
            )
        data = resp.json()
        for item in data.get("items", []):
            vid_id = (item.get("id") or {}).get("videoId")
            if not vid_id:
                continue
            snippet = item.get("snippet", {})
            results.append(
                {
                    "provider": "youtube",
                    "source_id": vid_id,
                    "title": snippet.get("title") or "Unknown title",
                    "uploader": snippet.get("channelTitle") or "Unknown artist",
                    "duration": 0,  # filled in by resolve step if needed
                    "thumbnail": (snippet.get("thumbnails") or {}).get("high", {}).get("url", ""),
                }
            )
    return results


YTDLP_OPTS = {
    # prefer m4a (AAC): plays on every browser incl. iOS Safari, no ffmpeg needed
    "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "outtmpl": str(CACHE_DIR / "%(id)s.%(ext)s"),
    "restrictfilenames": True,
    # robustness for flaky networks / long tracks
    "retries": 10,
    "fragment_retries": 10,
    "socket_timeout": 30,
    "continuedl": True,
    "concurrent_fragment_downloads": 4,
    # android first: mobile player API extracts even on datacenter IPs where
    # web/tv clients get "Failed to extract any player response". Yields a
    # single 360p progressive mp4 (plays everywhere as audio); tv_embedded
    # fallback gives full audio-only formats where it extracts.
    "extractor_args": {"youtube": {"player_client": ["android", "tv_embedded", "web"]}},
    "http_headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    },
}

_URL_RE = re.compile(r"^(https?://)?(www\.|music\.)?youtu(\.be/|be\.com/watch)")


def is_youtube_url(value: str) -> bool:
    return bool(_URL_RE.match(value.strip()))


def search_youtube(query: str, limit: int = 8):
    """Search YouTube and return lightweight track descriptors (no download)."""
    import yt_dlp

    params = dict(YTDLP_OPTS)
    params.update(
        {
            "skip_download": True,
            "quiet": True,
            "noplaylist": True,
        }
    )
    results = []
    with yt_dlp.YoutubeDL(params) as ydl:
        try:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
            entries = info.get("entries") or []
            for entry in entries:
                if not entry or not entry.get("id"):
                    continue
                results.append(
                    {
                        "provider": "youtube",
                        "source_id": entry["id"],
                        "title": entry.get("title") or "Unknown title",
                        "uploader": entry.get("uploader") or "Unknown artist",
                        "duration": entry.get("duration") or 0,
                        "thumbnail": entry.get("thumbnail") or "",
                    }
                )
        except Exception as exc:  # yt-dlp raises many varied errors
            raise RuntimeError(f"Search failed: {exc}") from exc
    return results


def resolve_youtube_url(url: str):
    """Extract metadata + video id for a pasted YouTube URL (no download)."""
    import yt_dlp

    params = dict(YTDLP_OPTS)
    params.update({"skip_download": True, "quiet": True, "noplaylist": True})
    with yt_dlp.YoutubeDL(params) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Could not resolve URL: {exc}") from exc
    return {
        "provider": "youtube",
        "source_id": info["id"],
        "title": info.get("title") or "Unknown title",
        "uploader": info.get("uploader") or "Unknown artist",
        "duration": info.get("duration") or 0,
        "thumbnail": info.get("thumbnail") or "",
    }


def download_youtube(source_id: str) -> dict:
    """Download a YouTube video's audio. Returns {path, ext, size}."""
    import yt_dlp

    params = dict(YTDLP_OPTS)
    params["outtmpl"] = str(CACHE_DIR / f"{source_id}.%(ext)s")
    with yt_dlp.YoutubeDL(params) as ydl:
        try:
            ydl.download([f"https://www.youtube.com/watch?v={source_id}"])
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Download failed: {exc}") from exc

    # yt-dlp leaves the file with the extension of the chosen format
    matches = sorted(CACHE_DIR.glob(f"{source_id}.*"))
    for path in matches:
        if path.suffix.lower() in (".part", ".ytdl"):
            continue
        size = path.stat().st_size
        if size > 0:
            return {"path": str(path), "ext": path.suffix.lstrip("."), "size": size}
    raise RuntimeError("Download completed but no audio file was found")


def guess_mime(ext: str) -> str:
    if not ext:
        return "application/octet-stream"
    mapped = {
        "m4a": "audio/mp4",
        "mp4": "audio/mp4",
        "mp3": "audio/mpeg",
        "ogg": "audio/ogg",
        "oga": "audio/ogg",
        "opus": "audio/ogg",
        "wav": "audio/wav",
        "webm": "audio/webm",
        "aac": "audio/aac",
        "flac": "audio/flac",
    }
    return mapped.get(ext.lower(), mimetypes.guess_type(f"f.{ext}")[0] or "application/octet-stream")


# --------------------------------------------------------------------------
# Fast path: in-memory TTL cache (avoids the slow yt-dlp network call on
# every keystroke). 10s TTL is enough to make type-ahead feel instant while
# still returning fresh results after a pause.
# --------------------------------------------------------------------------
_SEARCH_CACHE: dict[str, tuple[float, list[dict]]] = {}
_SEARCH_TTL_MS = 10_000

def clear_search_cache() -> None:
    _SEARCH_CACHE.clear()


def search_youtube_cached(query: str, limit: int = 8) -> list[dict]:
    """search_youtube but with a short TTL cache so type-ahead is snappy."""
    import time as _time

    now = _time.monotonic()
    hit = _SEARCH_CACHE.get(query)
    if hit and now - hit[0] < _SEARCH_TTL_MS:
        return hit[1]
    # Prefer the official Data API (fast, no bot-blocking); yt-dlp fallback.
    try:
        results = search_youtube_api(query, limit=limit)
        if not results:
            results = search_youtube(query, limit=limit)
    except Exception:
        results = search_youtube(query, limit=limit)
    _SEARCH_CACHE[query] = (now, results)
    if len(_SEARCH_CACHE) > 200:
        for k in list(_SEARCH_CACHE):
            if now - _SEARCH_CACHE[k][0] > _SEARCH_TTL_MS * 3:
                del _SEARCH_CACHE[k]
    return results


def related_youtube(source_id: str, limit: int = 8) -> list[dict]:
    """Find similar/recommended tracks for a YouTube source id.

    Reuses the same search flow but scopes the query to the source's title
    artist so 'More like this' returns genuinely similar stuff without an
    extra provider dependency. Returned descriptors match search results.
    """
    from .providers_meta import source_by_id  # local cache of known sources

    meta = source_by_id(source_id) or {"title": "", "uploader": ""}
    # build a focused query: prefer the uploader's artist channel, else title
    parts = []
    if meta.get("uploader"):
        parts.append(meta["uploader"])
    qt = meta.get("title") or ""
    # strip common "song YT audio" noise to keep the query clean
    for noise in ("Official Audio", "Official Video", "LYRICS", "lyrics",
                  "Audio", "Video", "(Official)"):
        qt = qt.replace(noise, " ").strip()
    if qt and qt not in parts:
        parts.append(qt)
    query = " ".join(parts).strip() or meta.get("title") or source_id
    return search_youtube(query, limit=limit)