/* Humsync - browser sync engine for room.html */

/* ------------------------------------------------------------------ */
/* Clock                                                              */
/* ------------------------------------------------------------------ */
// client clock is performance.now() (monotonic ms).  The server uses a
// monotonic clock too.  NTP handshakes give us clockOffsetMs so that
// `serverNow()` approximates the server clock.

let clockOffsetMs = 0;
let lastRttMs = 0;
const NTP_RING = [];
const MAX_NTP = 12;

const clientNow = () => performance.now();
const serverNow = () => performance.now() + clockOffsetMs;

function sendPing(ws) {
  const t0 = clientNow();
  ws.send(JSON.stringify({ type: "ntp", t0, rtt: lastRttMs }));
  return new Promise((resolve, reject) => {
    const on = (ev) => {
      const d = JSON.parse(ev.data);
      if (d.type !== "ntp" || d.t0 !== t0) return;
      ws.removeEventListener("message", on);
      const t3 = clientNow();
      const offset = (d.t1 - t0 + (d.t2 - t3)) / 2;
      const rtt = t3 - t0 - (d.t2 - d.t1);
      NTP_RING.push({ offset, rtt });
      if (NTP_RING.length > MAX_NTP) NTP_RING.shift();
      let best = NTP_RING[0];
      for (const m of NTP_RING) if (m.rtt < best.rtt) best = m;
      // keep playback elapsed continuous across calibration jumps:
      // serverNow() = perf.now() + offset, so shift exec anchor by same delta
      const delta = best.offset - clockOffsetMs;
      clockOffsetMs = best.offset;
      if (sync.execTimeMs) sync.execTimeMs += delta;
      lastRttMs = best.rtt;
      resolve();
    };
    const off = () => {
      ws.removeEventListener("message", on);
      reject(new Error("ws closed"));
    };
    ws.addEventListener("message", on);
    ws.addEventListener("close", off, { once: true });
  });
}

/* ------------------------------------------------------------------ */
/* Audio engine                                                       */
/* ------------------------------------------------------------------ */
const audio = new Audio();
audio.preload = "auto";
audio.crossOrigin = "anonymous";
let localVolume = 1.0;

const sync = {
  playing: false,
  sourceId: null,
  position: 0,      // position (s) at execTimeMs on the server clock
  execTimeMs: 0,
  pendingTimer: null,
  driftTimer: null,
};

const streamUrl = (id) => `/api/stream/${encodeURIComponent(id)}`;

function setTrack(sourceId) {
  if (audio.getAttribute("data-src") === sourceId) return;
  if (sync.pendingTimer) {
    clearTimeout(sync.pendingTimer);
    sync.pendingTimer = null;
  }
  audio.src = streamUrl(sourceId);
  audio.setAttribute("data-src", sourceId);
}

function loadTrack(sourceId) {
  return new Promise((resolve) => {
    setTrack(sourceId);
    if (audio.readyState >= 3) return resolve();
    const done = () => {
      audio.removeEventListener("canplay", done);
      audio.removeEventListener("error", err);
      resolve();
    };
    const err = () => {
      audio.removeEventListener("canplay", done);
      resolve();
    };
    audio.addEventListener("canplay", done);
    audio.addEventListener("error", err);
    // long window: first play may wait for the server's background download
    setTimeout(done, 15000);
  });
}

// Position we *should* be at right now (server clock reference).
// While playing = position + elapsed; paused = anchor position.
function expectedCurrentTime() {
  const pb = sync;
  return pb.playing
    ? pb.position + (serverNow() - pb.execTimeMs) / 1000
    : pb.position;
}

function clampPos(seconds, fallback) {
  if (isNaN(seconds) || seconds < 0) seconds = fallback || 0;
  const max = isFinite(audio.duration) ? audio.duration : Infinity;
  return Math.min(seconds, max);
}

function scheduleAction(msg) {
  // msg: {serverTimeToExecute, action}
  const when = Math.max(0, msg.serverTimeToExecute - serverNow());
  if (sync.pendingTimer) {
    clearTimeout(sync.pendingTimer);
    sync.pendingTimer = null;
  }
  sync.pendingTimer = setTimeout(applyAction, when, msg.action);
}

async function applyAction(action) {
  switch (action.type) {
    case "play": {
      const t0 = serverNow();
      await loadTrack(action.sourceId);
      sync.playing = true;
      sync.sourceId = action.sourceId;
      sync.position = action.position;
      sync.execTimeMs = t0;
      let cur = expectedCurrentTime();
      audio.currentTime = clampPos(cur);
      try {
        await audio.play();
      } catch (e) {
        // autoplay policy: keep state, user must tap
      }
      startDriftLoop();
      break;
    }
    case "pause": {
      sync.playing = false;
      const now = expectedCurrentTime();
      sync.position = now;
      sync.execTimeMs = serverNow();
      audio.pause();
      stopDriftLoop();
      break;
    }
    case "seek": {
      sync.position = action.position;
      sync.execTimeMs = serverNow();
      sync.playing = action.playing;
      audio.pause();
      audio.currentTime = clampPos(action.position);
      if (action.playing) {
        try {
          await audio.play();
        } catch (e) {}
        startDriftLoop();
      }
      break;
    }
    case "volume":
      audio.volume = Math.min(1, Math.max(0, action.volume)) * localVolume;
      break;
  }
}

/* Drift correction: every ~2s compare expected vs actual and re-align. */
function startDriftLoop() {
  stopDriftLoop();
  sync.driftTimer = setInterval(() => {
    if (!sync.playing) return;
    const expected = expectedCurrentTime();
    const actual = audio.currentTime;
    const drift = expected - actual;
    // audio elements refill from network; micro-nudge via playbackRate,
    // only hard-seek when way off (avoids audible glitch on small drift)
    if (Math.abs(drift) > 0.5) {
      audio.currentTime = clampPos(expected);
      audio.playbackRate = 1;
    } else if (Math.abs(drift) > 0.1) {
      // small nudge: speed up/slow down ~4% for ~1s instead of a jump
      audio.playbackRate = drift > 0 ? 1.04 : 0.96;
      setTimeout(() => { audio.playbackRate = 1; }, 1000);
    }
    renderProgress();
  }, 2000);
}

function stopDriftLoop() {
  if (sync.driftTimer) {
    clearInterval(sync.driftTimer);
    sync.driftTimer = null;
  }
}

/* ------------------------------------------------------------------ */
/* WebSocket + room lifecycle                                         */
/* ------------------------------------------------------------------ */
const params = new URLSearchParams(location.search);
const roomId = params.get("roomId") || "";
const myName = localStorage.getItem("beatsync_name") || "Guest";

let ws = null;
let myId = null;
let isAdmin = false;
let room = null;
let reconnectAttempts = 0;
let searchResults = [];
let statusTimer = null;

const $ = (id) => document.getElementById(id);

function wsUrl() {
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  return `${proto}${location.host}/ws/${encodeURIComponent(roomId)}`;
}

function connect() {
  ws = new WebSocket(wsUrl());
  ws.onopen = async () => {
    setStatus("connected");
    ws.send(JSON.stringify({ type: "join", username: myName }));
    await sendPing(ws);
    startNtpLoop();
  };
  ws.onmessage = (ev) => handleMessage(JSON.parse(ev.data));
  ws.onclose = () => {
    setStatus("disconnected");
    stopNtpLoop();
    stopDriftLoop();
    const delay = Math.min(30000, 500 * 2 ** reconnectAttempts);
    reconnectAttempts++;
    setTimeout(connect, delay);
  };
  ws.onerror = () => ws.close();
}

function startNtpLoop() {
  stopNtpLoop();
  statusTimer = setInterval(() => {
    sendPing(ws).catch(() => {});
  }, 5000);
}
function stopNtpLoop() {
  if (statusTimer) clearInterval(statusTimer);
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function handleMessage(msg) {
  switch (msg.type) {
    case "welcome":
      myId = msg.yourId;
      isAdmin = msg.isAdmin;
      renderHeader(msg.roomId);
      break;
    case "state": {
      room = msg.room;
      // server sends sources as {id: meta} map; normalize once to an array
      // (nowPlaying/renderQueue use .find on it)
      if (room && room.sources && !Array.isArray(room.sources)) {
        room.sources = Object.values(room.sources);
      }
      // Reconnect/mismatch safety: align audio element to server truth.
      // Server is source of truth: playback {sourceId, position, at, playing}.
      const pb = msg.room.playback;
      const curSrc = pb.sourceId;
      if (curSrc) {
        const changed = sync.sourceId !== curSrc;
        setTrack(curSrc);
        sync.sourceId = curSrc;
        if (pb.playing) {
          // anchor position: server position + elapsed since `at` (epoch ms)
          const elapsed = Math.max(0, (Date.now() - (pb.at || Date.now())) / 1000);
          sync.position = (pb.position || 0) + elapsed;
          sync.execTimeMs = serverNow();
          if (!sync.playing || changed) {
            sync.playing = true;
            loadTrack(curSrc).then(() => {
              audio.currentTime = clampPos(sync.position);
              audio.play().catch(() => flash("Tap ▶ to start audio"));
              startDriftLoop();
            });
          }
        } else if (sync.playing) {
          sync.playing = false;
          sync.position = pb.position || audio.currentTime || 0;
          sync.execTimeMs = serverNow();
          audio.pause();
          stopDriftLoop();
        }
      } else if (sync.playing) {
        sync.playing = false;
        audio.pause();
        stopDriftLoop();
      }
      render();
      break;
    }
    case "load-source": {
      // server asks all clients to buffer this source before a play
      const src = msg.source;
      loadTrack(src.id).then(() => send({ type: "source-loaded" }));
      break;
    }
    case "scheduled-action":
      scheduleAction(msg);
      break;
    case "search-results":
      searchResults = msg.results || [];
      renderSearch();
      break;
    case "promoted":
      isAdmin = true;
      flash("You are now the controller");
      render();
      break;
    case "error":
      flash(msg.message);
      break;
    case "peer-joined":
    case "peer-left":
      flash(`${msg.username || "a device"} ${msg.type === "peer-joined" ? "joined" : "left"}`);
      break;
    default:
      if (msg.type === "ntp") break; // handled by ping
  }
}

/* ------------------------------------------------------------------ */
/* Lyrics (lrclib: plain + synced LRC, active line follows playback)    */
/* ------------------------------------------------------------------ */
let lyricsState = { sourceId: null, lines: [], plain: "", status: "" };

function parseLrc(lrc) {
  const out = [];
  for (const line of String(lrc || "").split("\n")) {
    const m = line.match(/\[(\d+):(\d+(?:\.\d+)?)\]\s*(.*)/);
    if (!m) continue;
    const text = (m[3] || "").trim();
    if (!text) continue;
    out.push({ t: parseInt(m[1], 10) * 60 + parseFloat(m[2]), text });
  }
  out.sort((a, b) => a.t - b.t);
  return out;
}

async function loadLyrics(track) {
  const sid = track ? track.id || track.source_id : null;
  if (!sid || sid === lyricsState.sourceId) return;
  lyricsState = { sourceId: sid, lines: [], plain: "", status: "loading" };
  renderLyrics(-1);
  try {
    const r = await fetch(
      `/api/lyrics?title=${encodeURIComponent(track.title || "")}` +
      `&artist=${encodeURIComponent(track.uploader || "")}`
    );
    const d = await r.json();
    if (lyricsState.sourceId !== sid) return; // track changed meanwhile
    if (d.synced) lyricsState.lines = parseLrc(d.synced);
    lyricsState.plain = d.lyrics || "";
    lyricsState.status = d.found ? "ok" : "missing";
  } catch (e) {
    lyricsState.status = "error";
  }
  updateLyricHighlight._last = undefined;
  renderLyrics(-1);
}

function renderLyrics(activeIdx) {
  const box = $("lyrics");
  if (!box) return;
  if (lyricsState.status === "loading") {
    box.innerHTML = `<div class="empty">Loading lyrics…</div>`;
    return;
  }
  if (!lyricsState.lines.length) {
    box.innerHTML = lyricsState.plain
      ? `<div class="plain">${escapeHtml(lyricsState.plain)}</div>`
      : `<div class="empty">No lyrics found</div>`;
    return;
  }
  box.innerHTML = lyricsState.lines
    .map((l, i) => `<div class="lyric${i === activeIdx ? " active" : ""}">${escapeHtml(l.text)}</div>`)
    .join("");
  const el = box.querySelector(".lyric.active");
  if (el) el.scrollIntoView({ block: "nearest" });
}

function updateLyricHighlight() {
  if (!lyricsState.lines.length || !sync.playing) return;
  const cur = expectedCurrentTime();
  let idx = -1;
  for (let i = 0; i < lyricsState.lines.length; i++) {
    if (lyricsState.lines[i].t <= cur) idx = i;
    else break;
  }
  if (idx !== updateLyricHighlight._last) {
    updateLyricHighlight._last = idx;
    renderLyrics(idx);
  }
}

/* ------------------------------------------------------------------ */
/* UI                                                                 */
/* ------------------------------------------------------------------ */
function flash(text) {
  let el = $("toast");
  if (!el) {
    el = document.createElement("div");
    el.id = "toast";
    document.body.appendChild(el);
  }
  el.textContent = text;
  el.classList.add("show");
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove("show"), 3000);
}

function setStatus(s) {
  const el = $("conn");
  if (el) {
    el.textContent = s;
    el.className = `conn ${s}`;
  }
}

function renderHeader(rid) {
  const el = $("room-code");
  if (el) el.textContent = rid || roomId;
}

function fmtTime(sec) {
  if (!isFinite(sec) || sec < 0) sec = 0;
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return `${m}:${String(s).padStart(2, "0")}`;
}

function nowPlaying() {
  if (!room) return null;
  const sid = room.playback.sourceId;
  return room.sources.find((s) => s.id === sid) || null;
}

function render() {
  const track = nowPlaying();
  loadLyrics(track);
  const npTitle = $("np-title");
  const npSub = $("np-sub");
  const npArt = $("np-art");
  const playBtn = $("btn-play");
  const mainCtrl = $("main-controls");

  if (track) {
    npTitle.textContent = track.title;
    npSub.textContent = track.uploader || track.provider;
    npArt.src = track.thumbnail;
    playBtn.textContent = room.playback.playing ? "Pause" : "Play";
  } else {
    npTitle.textContent = "Nothing playing";
    npSub.textContent = "Add a track below";
    npArt.removeAttribute("src");
    playBtn.textContent = "Play";
  }

  mainCtrl.style.display = isAdmin ? "" : "none";
  const gvol = $("gvolwrap");
  if (gvol) gvol.style.display = isAdmin ? "" : "none";
  renderQueue();
  renderClients();
  renderSyncStats();
  renderSearch();
}

function renderProgress() {
  const track = nowPlaying();
  const cur = expectedCurrentTime();
  const dur = audio.duration;
  const bar = $("progress");
  const tCur = $("t-cur");
  const tDur = $("t-dur");
  if (bar) {
    if (!track || isNaN(dur) || dur <= 0) bar.value = 0;
    else bar.value = (cur / dur) * 100;
  }
  if (tCur) tCur.textContent = fmtTime(cur);
  if (tDur && isFinite(dur)) tDur.textContent = fmtTime(dur);
  updateLyricHighlight();
}

function renderSearch() {
  const box = $("search-results");
  if (!box) return;
  if (searchResults.length === 0) {
    box.innerHTML = "";
    return;
  }
  box.innerHTML = searchResults
    .map(
      (r, i) => `
    <div class="result">
      <img src="${r.thumbnail}" alt="" onerror="this.style.display='none'" />
      <div class="meta">
        <div class="rt">${escapeHtml(r.title)}</div>
        <div class="rs">${escapeHtml(r.uploader || "")} · ${fmtTime(r.duration || 0)}</div>
      </div>
      <button class="mini" data-add="${i}">Add</button>
      <button class="mini" data-like="${i}" title="More like this">↻ Similar</button>
    </div>`
    )
    .join("");
}

function renderQueue() {
  const box = $("queue");
  if (!box || !room) return;
  if (room.queue.length === 0) {
    box.innerHTML = `<div class="empty">No tracks yet</div>`;
    return;
  }
  const curId = room.playback.sourceId;
  box.innerHTML = room.queue
    .map((sid, idx) => {
      const s = room.sources.find((x) => x.id === sid);
      if (!s) return "";
      const active = sid === curId;
      const status =
        s.status === "pending"
          ? `<span class="badge pending">downloading…</span>`
          : s.status === "error"
          ? `<span class="badge err">failed</span>`
          : "";
      return `
      <div class="qitem ${active ? "active" : ""}">
        <button class="mini" data-qplay="${idx}" title="Play">▶</button>
        <div class="meta">
          <div class="qt">${escapeHtml(s.title)}</div>
          <div class="qs">${escapeHtml(s.uploader || s.provider)} ${status}</div>
        </div>
        <button class="mini danger" data-qdel="${idx}">✕</button>
      </div>`;
    })
    .join("");
}

function renderClients() {
  const box = $("devices");
  if (!box || !room) return;
  box.innerHTML = room.clients
    .map(
      (c) => `
    <div class="device">
      <span class="dot ${c.isAdmin ? "host" : ""}"></span>
      <span>${escapeHtml(c.name || "Guest")}</span>
      <span class="rt">${c.isAdmin ? "controller" : "listener"}</span>
    </div>`
    )
    .join("");
}

function renderSyncStats() {
  const el = $("sync-stats");
  if (el) {
    const off = clockOffsetMs.toFixed(0);
    el.textContent = `offset ${off >= 0 ? "+" : ""}${off}ms · rtt ${lastRttMs.toFixed(0)}ms`;
  }
}

/* ------------------------------------------------------------------ */
/* Interaction                                                        */
/* ------------------------------------------------------------------ */
function bindUI() {
  // transport
  $("btn-play").onclick = () => {
    if (!isAdmin || !room) return;
    const track = nowPlaying();
    if (!track) return;
    if (room.playback.playing) {
      const pos = expectedCurrentTime();
      send({ type: "pause", sourceId: track.id, position: pos });
    } else {
      send({ type: "play", sourceId: track.id, position: sync.position });
    }
  };
  $("btn-prev").onclick = () => isAdmin && send({ type: "prev" });
  $("btn-next").onclick = () => isAdmin && send({ type: "next" });

  // progress seek (controller only)
  const prog = $("progress");
  prog.addEventListener("input", () => {
    if (!isAdmin || !room) return;
    const dur = audio.duration;
    if (!isFinite(dur) || dur <= 0) return;
    const pos = (prog.value / 100) * dur;
    send({ type: "seek", position: pos });
  });

  // local volume
  $("vol").addEventListener("input", () => {
    localVolume = parseInt($("vol").value, 10) / 100;
    audio.volume = localVolume * (room ? room.globalVolume : 1);
  });

  // global volume (controller)
  $("gvol").addEventListener("input", () => {
    if (!isAdmin) return;
    const v = parseInt($("gvol").value, 10) / 100;
    send({ type: "global-volume", volume: v });
  });

  // search
  let searchT;
  $("btn-search").onclick = () => {
    if (!isAdmin) return;
    const q = $("q").value.trim();
    if (q) send({ type: "search", query: q });
  };
  $("q").addEventListener("input", () => {
    if (!isAdmin) return;
    clearTimeout(searchT);
    const q = $("q").value.trim();
    if (!q) { searchResults = []; renderSearch(); return; }
    searchT = setTimeout(() => send({ type: "search", query: q }), 250);
  });
  $("q").addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("btn-search").click();
  });

  // add youtube url
  $("btn-addurl").onclick = () => {
    if (!isAdmin) return;
    const u = $("url").value.trim();
    if (u) send({ type: "add-source", url: u });
  };
  $("url").addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("btn-addurl").click();
  });

  // upload local file
  $("file").addEventListener("change", async () => {
    const file = $("file").files[0];
    if (!file) return;
    const fd = new FormData();
    fd.append("file", file);
    try {
      await fetch(`/api/upload?roomId=${encodeURIComponent(roomId)}`, { method: "POST", body: fd });
      flash("Uploaded");
    } catch (e) {
      flash("Upload failed");
    }
  });

  // search result / queue delegation
  document.addEventListener("click", (e) => {
    const like = e.target.dataset.like;
    if (like !== undefined) {
      const r = searchResults[Number(like)];
      if (r && r.id) send({ type: "related", sourceId: r.id });
      return;
    }
    const add = e.target.dataset.add;
    if (add !== undefined) {
      const r = searchResults[Number(add)];
      if (r) send({ type: "use-result", source: r });
      return;
    }
    const qplay = e.target.dataset.qplay;
    if (qplay !== undefined && room) {
      const sid = room.queue[Number(qplay)];
      if (sid) send({ type: "play", sourceId: sid, position: 0 });
      return;
    }
    const qdel = e.target.dataset.qdel;
    if (qdel !== undefined && room) {
      const sid = room.queue[Number(qdel)];
      if (sid) send({ type: "remove-source", sourceId: sid });
      return;
    }
  });

  setInterval(renderProgress, 250);
  setInterval(renderSyncStats, 1000);

  // Audio unlock: a remote device pressing play triggers audio.play() here
  // with no user gesture, which mobile/desktop browsers block. Any tap on
  // the page retries playback when we should be playing but are paused.
  document.addEventListener("pointerdown", () => {
    if (sync.playing && audio.paused && sync.sourceId) {
      audio.play().catch(() => {});
    }
  });
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

/* ------------------------------------------------------------------ */
/* Boot                                                               */
/* ------------------------------------------------------------------ */
if (!roomId) location.href = "/";
bindUI();
connect();
renderHeader(roomId);
