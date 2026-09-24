# Humsync — Summary

Self-hosted multi-device music sync app (Python FastAPI + vanilla JS).
Ek device room banata hai (controller), baaki join karte hain — sab par
gaane sync mein bajte hain, queue + synced lyrics ke saath.

## Run

```bat
start.bat
```
ya: `python server.py` (env `PORT`, default 8000). Phone (same Wi-Fi):
`http://<PC-IP>:8000`. PC ki IP badle to `ipconfig` se dekho; permanent
fix ke liye static IP / DHCP reservation lagao.

Setup: `pip install -r requirements.txt`. `YOUTUBE_API_KEY` `.env` mein
rakho (`YOUTUBE_API_KEY=...`) — fast search ke liye (bina key yt-dlp
fallback chalta hai).

## Features

- Room create/join (6-char uppercase codes), auto-admin first joiner
- YouTube search (Data API first, yt-dlp fallback, TTL cache)
- Queue add/remove, synced play/pause/seek/prev/next, global volume
- Upload local files, paste YouTube link
- Synced lyrics (lrclib) with active-line highlight
- NTP clock sync + drift correction across devices
- Live state broadcast: queue, devices, playback sab real-time

## Repo layout

- `server.py` — FastAPI app: rooms, WS sync (`/ws/{room_id}`), stream,
  upload, lyrics, search
- `providers.py` — YouTube search/download/stream, lyrics via lrclib
- `static/` — `index.html`, `room.html`, `app.js`, `style.css`
- `requirements.txt`, `start.bat`

Debug/probe scripts, logs, uploads aur `.env` git mein nahi aate
(`.gitignore` dekho).
