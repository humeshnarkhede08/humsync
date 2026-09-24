# Humsync

Self-hosted multi-device music sync — ek gaana, har device pe perfectly sync.
Inspired by the original [beatsync](https://github.com/freeman-jiang/beatsync) architecture, rebuilt in Python + Web.

## How it works

- Ek device `Create room` karta hai → wahi **controller (host)** ban jata hai.
- Baaki devices room code se `Join` karte hain (mobile/unable browser sab chalega).
- Controller kaam karta hai: YouTube search / YouTube link / **local file upload** (apne phone ke gaane bhi).
- Har track server pe download/save hota hai, aur ek hi `/api/stream` se sab clients ko milta hai.
- Playback state server ka ek source-of-truth hota hai:
  `{type, sourceId, position, at}` — jab playing ho, position = position + (now - at).
- Har action (`play` / `pause` / `seek` / `volume`) ek **common future server time** pe schedule hota hai,
  aur har client usi moment pe execute karta hai.
- Clients apne clock ko NTP handshake se server clock se align karte hain
  (min-RTT selection, reference architecture jaisa).
- Server naya source pehle **sabhi clients buffer hone ka wait** karta hai,
  phir play broadcast karta hai (timeout fallback ke saath).
- Jo device thoda late join hota hai, wo `resync` ho jata hai (correct position se shuru).
- Controller chala jaye → sabse recently-active client controller promote ho jata hai.

## Run

```bash
pip install -r requirements.txt
python server.py
```

Phir kholo: `http://<yourserver-ip>:8000`

- Server LAN pe `0.0.0.0:8000` listen karta hai.
- E.g. `http://192.168.1.5:8000` — saare devices isi URL pe kholein.

## Notes

- YouTube audio `yt-dlp` se download hota hai (pehli baar add karne par thoda wait hoga).
- Local files `uploads/` folder mein save hoti hain.
- `ffmpeg` ho toh YouTube `.m4a` me convert hoga (best browser support); warna raw format.
- Server chale tab room active rahega; empty rooms 10 min baad clean.