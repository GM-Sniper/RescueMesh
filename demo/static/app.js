/* RescueMesh Mission Control: the browser side of demo/server.py.
   One snapshot per second arrives over /api/stream (Server-Sent Events); everything on the
   page is drawn from it. Buttons POST /api/action {name, params}; the server runs the
   commands over SSH and reports progress in the next snapshots. */
"use strict";

const NODE_COL = { 1: "#22d3ee", 2: "#a78bfa", 3: "#fbbf24" };
const OK = "#34d399", WARN = "#fb923c", BAD = "#f87171", INFO = "#60a5fa", BID = "#f0abfc";
window.addEventListener("error", e => { try { toast("Page error", e.message, "fail"); } catch (_) { /* toast not ready */ } });
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
const S = { snap: null, settings: null, view: "live", mine: {}, seenStatus: {}, sticky: {}, logKey: null,
            openActions: new Set(), loadFormInit: false, rampStarted: false };

function esc(x) { return String(x == null ? "" : x).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }
function fmtInt(n) { return n == null ? "-" : Math.round(n).toLocaleString("en-US"); }
function fmtMs(v, d = 1) { if (v == null) return "-"; return v >= 1000 ? (v / 1000).toFixed(2) + " s" : v.toFixed(v < 10 ? Math.max(d, 1) : d) + " ms"; }
function fmtS(ms) { return ms == null ? "-" : (ms / 1000).toFixed(2) + " s"; }
function fmtBytes(b) { if (b == null) return "-"; if (b > 1e6) return (b / 1e6).toFixed(1) + " MB/s"; if (b > 1e3) return (b / 1e3).toFixed(0) + " KB/s"; return b + " B/s"; }
function hhmmss(t) { const d = new Date(t); return d.toTimeString().slice(0, 8); }
function sum(a) { let s = 0; for (const x of a) if (x) s += x; return s; }
function last(a, n) { return a.slice(Math.max(0, a.length - n)); }
function nodes(snap) { return Object.values(snap.machines).filter(m => m.role === "node").sort((a, b) => a.id - b.id); }
function clientsOf(snap) { return Object.values(snap.machines).filter(m => m.role === "client").sort((a, b) => a.name.localeCompare(b.name)); }
const ELIGIBLE = new Set(["online", "stressed", "slowed"]);

async function api(path, body) {
  const r = await fetch(path, body === undefined ? {} : { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.ok === false) throw new Error(j.error || ("HTTP " + r.status));
  return j;
}

function toast(title, text, kind) {
  const t = document.createElement("div");
  t.className = "toast " + (kind || "");
  t.innerHTML = `<b>${esc(title)}</b>${esc(text || "")}`;
  $("#toasts").appendChild(t);
  setTimeout(() => { t.style.transition = "opacity .4s"; t.style.opacity = "0"; setTimeout(() => t.remove(), 400); }, kind === "fail" ? 9000 : 5000);
}

/* ======================================================================= tabs */
$$("#tabs button").forEach(b => b.addEventListener("click", () => showView(b.dataset.view)));
function showView(v) {
  S.view = v;
  $$("#tabs button").forEach(b => b.classList.toggle("active", b.dataset.view === v));
  $$(".view").forEach(x => x.classList.toggle("active", x.id === "v-" + v));
  if (S.snap) render(S.snap);
  if (v === "run") renderRun(S.snap);
  window.dispatchEvent(new Event("resize"));
}

/* ======================================================================= actions */
const CONFIRM = {
  full_reset: "Full reset: stop load and nodes, DELETE all shared storage, then start everything fresh. Continue?",
  weed_wipe: "Delete all SeaweedFS data on the 3 node PCs?",
  stop_all: "Stop load, CPU stress and the 3 nodes?",
  nodes_stop: "Stop the 3 nodes?",
  apply_faults: "This restarts the 3 nodes (a few seconds of downtime) with the new setting. Continue?",
  node_restart: "This restarts that node. Continue?",
  power_cut: "POWER CUT: this kill -9s the node AND its SeaweedFS storage on that PC. Continue?",
};

function loadParams(btn) {
  const mode = (btn && btn.dataset.mode) || $("#ld-mode").value;
  const pcs = $$(".cl-pick").filter(c => c.checked).map(c => c.value);
  const p = { pcs, mode };
  if (mode === "ramp") { p.stages = $("#ld-stages").value; p.stage_secs = +$("#ld-ss").value; }
  else { p.rate = +$("#ld-rate").value; p.clients = +$("#ld-clients").value; p.requests = +$("#ld-req").value; }
  return p;
}

const PARAMS = {
  load_start: loadParams,
  load_stop: () => ({ pcs: $$(".cl-pick").filter(c => c.checked).map(c => c.value) }),
  mute: btn => ({ node: +btn.dataset.node, secs: +$("#mute-secs").value }),
  mute_leader: () => ({ secs: +$("#mute-secs").value }),
  bringup: () => ({ wipe: $("#bring-wipe").checked }),
  stop_all: () => ({ weed: $("#stop-weed").checked }),
  dup_test: () => ({ client: ($$(".cl-pick").find(c => c.checked) || { value: "A" }).value }),
};

async function runAction(name, params, btn) {
  if (CONFIRM[name] && !confirm(CONFIRM[name])) return;
  if (name === "bringup" && params.wipe && !confirm("One-click bring-up with a WIPE of the shared storage. Continue?")) return;
  try {
    if (btn) btn.classList.add("busy");
    const r = await api("/api/action", { name, params });
    S.mine[r.id] = btn || true;
    if (name === "load_start" && params.mode === "ramp") S.rampStarted = true;
  } catch (e) {
    if (btn) btn.classList.remove("busy");
    toast("Could not start", e.message, "fail");
  }
}

document.addEventListener("click", e => {
  const b = e.target.closest("[data-action]");
  if (b) {
    const name = b.dataset.action;
    const params = PARAMS[name] ? PARAMS[name](b) : (b.dataset.node ? { node: +b.dataset.node } : {});
    runAction(name, params, b);
    return;
  }
  const h = e.target.closest(".hog");
  if (h) {
    const n = +h.dataset.node;
    const m = S.snap && nodes(S.snap).find(x => x.id === n);
    const on = m && m.proc && m.proc.hogs > 0;
    runAction(on ? "hog_off" : "hog_on", { node: n, cores: +$("#hog-cores").value || 0 }, h);
    return;
  }
  const v = e.target.closest("[data-goto]");
  if (v) showView(v.dataset.goto);
});

$("#auto-toggle").addEventListener("click", e => runAction("apply_faults", { enabled: !(S.snap && S.snap.faults_enabled) }, e.target));
$("#slow-go").addEventListener("click", e => runAction("node_restart", { node: +$("#slow-node").value, iterations: +$("#slow-it").value }, e.target));
$("#slow-undo").addEventListener("click", e => runAction("node_restart", { node: +$("#slow-node").value, iterations: 0 }, e.target));
$("#res-apply").addEventListener("click", async e => {
  const n = Math.max(1, +$("#res-count").value || 0);
  if (!confirm("Start with " + n + " resources? This DELETES all shared storage and restarts the 3 nodes.")) return;
  try {
    const s = JSON.parse(JSON.stringify(S.settings)); s.cluster.resource_count = n;
    await api("/api/settings", s); S.settings = s;
    runAction("full_reset", {}, e.target);
  } catch (err) { toast("Could not apply", err.message, "fail"); }
});
$("#mute-secs").addEventListener("input", e => { $("#mute-secs-v").textContent = e.target.value; });
$("#ld-mode").addEventListener("change", () => {
  const ramp = $("#ld-mode").value === "ramp";
  $("#ld-fixed").classList.toggle("hidden", ramp);
  $("#ld-ramp").classList.toggle("hidden", !ramp);
});

function trackActions(snap) {
  for (const a of snap.actions) {
    const prev = S.seenStatus[a.id];
    if (a.status !== "running" && prev !== a.status) {
      const mine = S.mine[a.id];
      if (prev === "running" || mine) {
        toast(a.title + (a.status === "ok" ? " ✔" : " ✘"), a.summary || (a.status === "ok" ? "done" : "failed: see Consoles"), a.status === "ok" ? "ok" : "fail");
        if (mine && mine !== true) mine.classList.remove("busy");
        delete S.mine[a.id];
      }
    }
    S.seenStatus[a.id] = a.status;
  }
  const la = snap.actions[snap.actions.length - 1];
  if (la) {
    const chip = la.status === "running" ? "run" : la.status;
    $("#last-action").innerHTML = `<span class="chip ${chip}">${esc(la.status)}</span> <b>${esc(la.title)}</b> <span>${esc(la.summary || "")}</span>`;
  }
}

/* ======================================================================= charts */
function niceMax(v) {
  if (!(v > 0)) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}
function fmtAxis(v, unit) {
  if (unit === "ms") return v >= 1000 ? (v / 1000).toFixed(v >= 10000 ? 0 : 1) + "s" : (v < 1 ? v.toFixed(1) : Math.round(v)) + "";
  if (unit === "%") return Math.round(v) + "%";
  if (v >= 1000) return (v / 1000).toFixed(1) + "k";
  return v < 1 && v > 0 ? v.toFixed(2) : Math.round(v * 10) / 10 + "";
}

class Chart {
  constructor(canvas, opts) {
    this.c = canvas; this.o = opts || {}; this.t = []; this.series = []; this.hover = null;
    canvas.addEventListener("mousemove", e => { const r = canvas.getBoundingClientRect(); this.hx = e.clientX - r.left; this.draw(); this.tip(e); });
    canvas.addEventListener("mouseleave", () => { this.hx = null; this.draw(); $("#tip").style.display = "none"; });
  }
  set(t, series) { this.t = t; this.series = series; this.draw(); }
  geom() { const w = this.c.clientWidth, h = this.c.clientHeight; const pad = { l: 44, r: 22, t: 10, b: 22 }; return { w, h, pad, W: w - pad.l - pad.r, H: h - pad.t - pad.b }; }
  draw() {
    const c = this.c, dpr = window.devicePixelRatio || 1, { w, h, pad, W, H } = this.geom();
    if (!w || !h) return;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) { c.width = Math.round(w * dpr); c.height = Math.round(h * dpr); }
    const g = c.getContext("2d"); g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, h);
    const n = this.t.length; if (n < 2) return;
    const o = this.o, stack = !!o.stack;
    let cum = new Array(n).fill(0); const drawn = []; let ymax = 0, ymin = Infinity;
    for (const s of this.series) {
      const top = s.data.map((v, i) => stack ? cum[i] + (v || 0) : v);
      drawn.push({ s, base: stack ? cum.slice() : null, top });
      if (stack) cum = top;
      for (const v of top) if (v != null) { if (v > ymax) ymax = v; if (v > 0 && v < ymin) ymin = v; }
    }
    if (o.yMax) ymax = o.yMax; else ymax = niceMax(Math.max(ymax, o.yMin || 0) * 1.08);
    const log = !!o.log;
    const lmin = log ? Math.log10(o.logMin || 1) : 0, lmax = log ? Math.log10(Math.max(ymax, (o.logMin || 1) * 10)) : 0;
    const X = i => pad.l + W * i / (n - 1);
    const Y = v => log ? pad.t + H - H * (Math.log10(Math.max(v, o.logMin || 1)) - lmin) / (lmax - lmin) : pad.t + H - H * Math.min(v, ymax) / ymax;
    g.font = "11px ui-monospace, Consolas, monospace"; g.lineWidth = 1;
    g.strokeStyle = "rgba(120,150,210,0.12)"; g.fillStyle = "#5b6b88"; g.textAlign = "right"; g.textBaseline = "middle";
    const ticks = log ? (() => { const t = []; for (let e = Math.ceil(lmin); e <= Math.floor(lmax); e++) t.push(Math.pow(10, e)); return t; })() : [0, 1, 2, 3, 4].map(k => ymax * k / 4);
    for (const v of ticks) { const y = Y(v); g.beginPath(); g.moveTo(pad.l, y); g.lineTo(pad.l + W, y); g.stroke(); g.fillText(fmtAxis(v, o.unit), pad.l - 6, y); }
    g.textAlign = "center"; g.textBaseline = "top";
    if (o.labels) { o.labels.forEach((l, i) => g.fillText(l, X(i), pad.t + H + 6)); }
    else { const span = (this.t[n - 1] - this.t[0]) / 1000; for (const sec of [180, 150, 120, 90, 60, 30, 0]) { if (sec > span) continue; g.fillText(sec ? `-${sec}s` : "now", pad.l + W * (1 - sec / span), pad.t + H + 6); } }
    for (const d of drawn) {
      const col = d.s.color;
      if (stack) {
        g.beginPath(); let started = false;
        for (let i = 0; i < n; i++) { const x = X(i), y = Y(d.top[i] || 0); if (!started) { g.moveTo(x, y); started = true; } else g.lineTo(x, y); }
        for (let i = n - 1; i >= 0; i--) g.lineTo(X(i), Y(d.base[i] || 0));
        g.closePath(); g.globalAlpha = 0.55; g.fillStyle = col; g.fill(); g.globalAlpha = 1;
        g.beginPath(); for (let i = 0; i < n; i++) { const x = X(i), y = Y(d.top[i] || 0); i ? g.lineTo(x, y) : g.moveTo(x, y); }
        g.strokeStyle = col; g.lineWidth = 1.5; g.stroke();
      } else {
        g.beginPath(); let pen = false;
        for (let i = 0; i < n; i++) { const v = d.top[i]; if (v == null) { pen = false; continue; } const x = X(i), y = Y(v); if (!pen) { g.moveTo(x, y); pen = true; } else g.lineTo(x, y); }
        g.strokeStyle = col; g.lineWidth = d.s.width || 2; g.setLineDash(d.s.dash || []); g.stroke(); g.setLineDash([]);
        if (d.s.fill) {
          const gr = g.createLinearGradient(0, pad.t, 0, pad.t + H); gr.addColorStop(0, col + "55"); gr.addColorStop(1, col + "00");
          g.lineTo(X(n - 1), pad.t + H); g.lineTo(X(0), pad.t + H); g.closePath(); g.fillStyle = gr; g.fill();
        }
        if (o.points) for (let i = 0; i < n; i++) { const v = d.top[i]; if (v == null) continue; g.beginPath(); g.arc(X(i), Y(v), 3.5, 0, 7); g.fillStyle = col; g.fill(); }
      }
    }
    if (o.marks) for (const m of o.marks) { if (m.i < 0 || m.i >= n) continue; const x = X(m.i); g.strokeStyle = m.color; g.setLineDash([3, 3]); g.beginPath(); g.moveTo(x, pad.t); g.lineTo(x, pad.t + H); g.stroke(); g.setLineDash([]); }
    if (this.hx != null && this.hx > pad.l) {
      const i = Math.max(0, Math.min(n - 1, Math.round((this.hx - pad.l) / W * (n - 1))));
      this.hi = i; const x = X(i);
      g.strokeStyle = "rgba(230,237,248,0.35)"; g.beginPath(); g.moveTo(x, pad.t); g.lineTo(x, pad.t + H); g.stroke();
    } else this.hi = null;
  }
  tip(e) {
    const tip = $("#tip"); if (this.hi == null) { tip.style.display = "none"; return; }
    const i = this.hi; const head = this.o.labels ? this.o.labels[i] : hhmmss(this.t[i]);
    const lines = [head].concat(this.series.map(s => `${s.label.padEnd(8)} ${s.data[i] == null ? "-" : (Math.round(s.data[i] * 100) / 100) + (this.o.unit === "ms" ? " ms" : this.o.unit === "%" ? " %" : "")}`));
    tip.textContent = lines.join("\n"); tip.style.display = "block";
    tip.style.left = (e.clientX + 14) + "px"; tip.style.top = (e.clientY + 10) + "px";
  }
}

const CH = {
  thr: new Chart($("#c-thr"), { stack: true }),
  lat: new Chart($("#c-lat"), { log: true, logMin: 1, unit: "ms" }),
  share: new Chart($("#c-share"), { stack: true, yMax: 100, unit: "%" }),
  load: new Chart($("#c-load"), { yMin: 0.3 }),
  cpu: new Chart($("#c-cpu"), { yMax: 100, unit: "%" }),
  el: new Chart($("#c-el"), {}),
};
$$(".legend.nodes").forEach(l => l.innerHTML = [1, 2, 3].map(i => `<span><i class="lg" style="background:${NODE_COL[i]}"></i>node ${i}</span>`).join(""));

function rollingShare(w, win) {
  const n = w[1].length, out = { 1: [], 2: [], 3: [] };
  for (let i = 0; i < n; i++) {
    const a = Math.max(0, i - win + 1); let t = 0; const s = {};
    for (const k of [1, 2, 3]) { s[k] = 0; for (let j = a; j <= i; j++) s[k] += w[k][j] || 0; t += s[k]; }
    for (const k of [1, 2, 3]) out[k].push(t ? 100 * s[k] / t : null);
  }
  return out;
}

function renderCharts(snap) {
  const s = snap.series, t = s.t;
  CH.thr.set(t, [1, 2, 3].map(i => ({ label: "node " + i, color: NODE_COL[i], data: s.ex[i] })));
  CH.lat.set(t, [{ label: "p50", color: OK, data: s.p50 }, { label: "p95", color: WARN, data: s.p95 }, { label: "p99", color: BAD, data: s.p99, width: 1.5 }]);
  const sh = rollingShare(s.wins, 10);
  CH.share.set(t, [1, 2, 3].map(i => ({ label: "node " + i, color: NODE_COL[i], data: sh[i] })));
  CH.load.set(t, [1, 2, 3].map(i => ({ label: "node " + i, color: NODE_COL[i], data: s.load[i] })));
  CH.cpu.set(t, [1, 2, 3].map(i => ({ label: "node " + i, color: NODE_COL[i], data: s.cpu[i] })));
  CH.el.set(t, [{ label: "elect/s", color: INFO, data: s.el, fill: true }, { label: "retried", color: WARN, data: s.retried }, { label: "lost", color: BAD, data: s.lost }]);
}

/* ======================================================================= mesh animation */
const Topo = {
  c: $("#topo"), parts: [], acc: {}, lastT: performance.now(), hbT: 0, pos: {}, flashes: [],
  layout() {
    const c = this.c, w = c.clientWidth, h = c.clientHeight, dpr = window.devicePixelRatio || 1;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) { c.width = Math.round(w * dpr); c.height = Math.round(h * dpr); }
    this.w = w; this.h = h; this.dpr = dpr;
    const R = Math.max(34, Math.min(w, h) * 0.085); this.R = R;
    this.pos = {
      A: { x: w * 0.12, y: h * 0.2 }, B: { x: w * 0.12, y: h * 0.5 }, C: { x: w * 0.12, y: h * 0.8 },
      1: { x: w * 0.66, y: h * 0.23 }, 2: { x: w * 0.86, y: h * 0.66 }, 3: { x: w * 0.47, y: h * 0.66 },
    };
  },
  spawn(from, to, col, dur, size, then, fade) {
    const a = this.pos[from], b = this.pos[to]; if (!a || !b) return;
    this.parts.push({ a, b, col, t0: performance.now(), dur, size, then, fade });
  },
  eligible(snap) { return nodes(snap).filter(m => ELIGIBLE.has(m.state)).map(m => m.id); },
  pickWinner(snap) {
    const el = this.eligible(snap); if (!el.length) return null;
    const sh = snap.kpi.share30; let tot = 0; for (const i of el) tot += (sh[i] || 0) + 0.5;
    let r = Math.random() * tot; for (const i of el) { r -= (sh[i] || 0) + 0.5; if (r <= 0) return i; }
    return el[el.length - 1];
  },
  request(snap, cname) {
    const ns = nodes(snap).filter(m => m.state !== "offline" && m.state !== "stopped");
    if (!ns.length) return;
    const winner = this.pickWinner(snap);
    ns.forEach(m => {
      const isWin = m.id === winner;
      this.spawn(cname, m.id, INFO, 700, 3, isWin ? () => {
        const el = this.eligible(snap);
        el.filter(x => x !== winner).forEach(x => { this.spawn(winner, x, BID, 260, 2.2); this.spawn(x, winner, BID, 260, 2.2); });
        this.flashes.push({ id: winner, t0: performance.now() });
        setTimeout(() => this.spawn(winner, cname, OK, 700, 3.4), 280);
      } : null, m.state === "muted");
    });
  },
  tick() {
    const now = performance.now(), dt = Math.min(0.1, (now - this.lastT) / 1000); this.lastT = now;
    const snap = S.snap;
    if (snap && S.view === "live") {
      for (const cl of clientsOf(snap)) {
        const arr = snap.series.cl[cl.name] || [];
        const rate = sum(last(arr, 3)) / 3;
        const vis = cl.state === "running" ? Math.min(9, Math.max(rate > 0 ? 1.2 : 0.6, rate / 6)) : 0;
        this.acc[cl.name] = (this.acc[cl.name] || 0) + vis * dt;
        while (this.acc[cl.name] >= 1) { this.acc[cl.name] -= 1; this.request(snap, cl.name); }
      }
      this.hbT += dt;
      if (this.hbT > 0.5) {
        this.hbT = 0;
        const live = nodes(snap).filter(m => m.state !== "offline" && m.state !== "stopped" && m.state !== "muted").map(m => m.id);
        for (const a of live) for (const b of live) if (a !== b) this.spawn(a, b, "rgba(230,237,248,0.55)", 450, 1.4);
      }
      this.draw(snap, now);
    }
    requestAnimationFrame(() => this.tick());
  },
  draw(snap, now) {
    const g = this.c.getContext("2d"); this.layout();
    const { w, h, R, dpr } = this; g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, h);
    const ns = nodes(snap), byId = {}; ns.forEach(m => byId[m.id] = m);
    const cls = clientsOf(snap);
    // storage band
    g.save(); g.strokeStyle = "rgba(96,165,250,0.18)"; g.setLineDash([2, 6]); g.lineWidth = 1.2;
    g.beginPath(); g.ellipse(w * 0.665, h * 0.5, w * 0.28, h * 0.43, 0, 0, 7); g.stroke(); g.restore();
    g.fillStyle = "rgba(139,155,184,0.55)"; g.font = "600 11px Inter, Segoe UI, sans-serif"; g.textAlign = "center";
    g.textAlign = "right"; g.fillText("CLOUD", w - 12, 16);
    g.textAlign = "left"; g.fillText("CLIENTS", 12, 16);
    // client -> node links
    for (const cl of cls) for (const m of ns) {
      const a = this.pos[cl.name], b = this.pos[m.id]; if (!a || !b) continue;
      g.strokeStyle = cl.state === "running" ? "rgba(96,165,250,0.16)" : "rgba(120,150,210,0.07)"; g.lineWidth = 1;
      g.beginPath(); g.moveTo(a.x + 70, a.y); g.lineTo(b.x, b.y); g.stroke();
    }
    // node <-> node links
    const pairs = [[1, 2], [2, 3], [3, 1]];
    for (const [x, y] of pairs) {
      const a = this.pos[x], b = this.pos[y], mx = byId[x], my = byId[y];
      const down = !mx || !my || !ELIGIBLE.has(mx.state) && mx.state !== "muted" || !ELIGIBLE.has(my.state) && my.state !== "muted";
      const silent = (mx && mx.state === "muted") || (my && my.state === "muted") ||
        (mx && mx.peers_down.includes(y)) || (my && my.peers_down.includes(x));
      g.lineWidth = 2; g.strokeStyle = down ? "rgba(91,107,136,0.4)" : silent ? "rgba(248,113,113,0.7)" : "rgba(167,139,250,0.35)";
      g.setLineDash(down || silent ? [6, 6] : []); g.beginPath(); g.moveTo(a.x, a.y); g.lineTo(b.x, b.y); g.stroke(); g.setLineDash([]);
    }
    // particles
    this.parts = this.parts.filter(p => {
      let k = (now - p.t0) / p.dur; if (k >= 1) { if (p.then) p.then(); return false; }
      if (p.fade && k > 0.78) {
        const x = p.a.x + (p.b.x - p.a.x) * 0.78, y = p.a.y + (p.b.y - p.a.y) * 0.78;
        g.strokeStyle = BAD; g.lineWidth = 2; const s = 5 * (1 + (k - 0.78) * 4);
        g.globalAlpha = Math.max(0, 1 - (k - 0.78) * 4.5);
        g.beginPath(); g.moveTo(x - s, y - s); g.lineTo(x + s, y + s); g.moveTo(x + s, y - s); g.lineTo(x - s, y + s); g.stroke(); g.globalAlpha = 1;
        return true;
      }
      const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
      const x = p.a.x + (p.b.x - p.a.x) * e, y = p.a.y + (p.b.y - p.a.y) * e;
      g.beginPath(); g.arc(x, y, p.size * 2.6, 0, 7); g.fillStyle = p.col.startsWith("#") ? p.col + "33" : "rgba(230,237,248,0.12)"; g.fill();
      g.beginPath(); g.arc(x, y, p.size, 0, 7); g.fillStyle = p.col; g.fill();
      return true;
    });
    if (this.parts.length > 1500) this.parts.splice(0, this.parts.length - 1500);
    this.flashes = this.flashes.filter(f => now - f.t0 < 450);
    // clients
    for (const cl of cls) {
      const p = this.pos[cl.name]; const bw = 140, bh = 62, x = p.x - bw / 2, y = p.y - bh / 2;
      const run = cl.state === "running";
      g.fillStyle = run ? "rgba(96,165,250,0.12)" : "rgba(255,255,255,0.04)";
      g.strokeStyle = cl.state === "offline" ? "rgba(91,107,136,0.5)" : run ? "rgba(96,165,250,0.7)" : "rgba(120,150,210,0.3)";
      g.lineWidth = 1.5; roundRect(g, x, y, bw, bh, 12); g.fill(); g.stroke();
      g.fillStyle = "#e6edf8"; g.font = "700 14px Inter, Segoe UI, sans-serif"; g.textAlign = "left";
      g.fillText("Client " + cl.name, x + 12, y + 21);
      const rate = sum(last(snap.series.cl[cl.name] || [], 5)) / 5;
      g.font = "12px ui-monospace, Consolas, monospace"; g.fillStyle = run ? OK : "#8b9bb8";
      g.fillText(cl.state === "offline" ? "offline" : run ? rate.toFixed(0) + " req/s" : "idle", x + 12, y + 40);
      g.fillStyle = "#5b6b88"; g.font = "11px ui-monospace, Consolas, monospace"; g.fillText(cl.ip || "", x + 12, y + 54);
    }
    // nodes
    for (const m of ns) {
      const p = this.pos[m.id]; if (!p) continue;
      const col = NODE_COL[m.id], st = m.state;
      const share = snap.kpi.share30[m.id];
      const flash = this.flashes.some(f => f.id === m.id);
      // halo
      let halo = null;
      if (st === "muted") halo = BAD; else if (st === "stressed") halo = WARN; else if (st === "slowed") halo = "#fbbf24"; else if (flash) halo = OK;
      if (halo) { const pulse = 0.5 + 0.5 * Math.sin(now / 160); const gr = g.createRadialGradient(p.x, p.y, R * 0.8, p.x, p.y, R * 1.9); gr.addColorStop(0, halo + (flash && !["muted", "stressed"].includes(st) ? "66" : Math.round(60 + pulse * 70).toString(16))); gr.addColorStop(1, halo + "00"); g.fillStyle = gr; g.beginPath(); g.arc(p.x, p.y, R * 1.9, 0, 7); g.fill(); }
      // body
      const dead = st === "offline" || st === "stopped";
      g.beginPath(); g.arc(p.x, p.y, R, 0, 7);
      const body = g.createRadialGradient(p.x - R * 0.3, p.y - R * 0.4, R * 0.1, p.x, p.y, R);
      body.addColorStop(0, dead ? "#2a3347" : "#1c2a48"); body.addColorStop(1, dead ? "#141a26" : "#0d1528");
      g.fillStyle = body; g.fill();
      g.lineWidth = 3; g.strokeStyle = dead ? "#3a465e" : st === "muted" ? BAD : col; g.stroke();
      // share ring
      if (!dead && share != null) {
        g.beginPath(); g.lineWidth = 6; g.strokeStyle = col; g.lineCap = "round";
        g.arc(p.x, p.y, R + 9, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * share / 100); g.stroke(); g.lineCap = "butt";
        g.beginPath(); g.lineWidth = 6; g.strokeStyle = "rgba(255,255,255,0.05)"; g.arc(p.x, p.y, R + 9, -Math.PI / 2 + Math.PI * 2 * share / 100, Math.PI * 1.5); g.stroke();
      }
      // muted countdown
      if (st === "muted" && m.muted) {
        const left = Math.max(0, m.muted.left - (Date.now() - S.snapAt)), frac = m.muted.dur ? left / m.muted.dur : 0;
        g.beginPath(); g.lineWidth = 4; g.strokeStyle = BAD; g.arc(p.x, p.y, R - 6, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * frac); g.stroke();
        g.fillStyle = BAD; g.font = "800 12px Inter, Segoe UI, sans-serif"; g.textAlign = "center";
        g.fillText("SILENT " + (left / 1000).toFixed(1) + " s", p.x, p.y + R * 0.55);
      }
      g.textAlign = "center"; g.fillStyle = dead ? "#5b6b88" : "#fff"; g.font = `800 ${Math.round(R * 0.5)}px Inter, Segoe UI, sans-serif`;
      g.fillText("N" + m.id, p.x, p.y + R * 0.12);
      if (st !== "muted") {
        g.font = "700 11px Inter, Segoe UI, sans-serif";
        g.fillStyle = { online: OK, stressed: WARN, slowed: "#fbbf24", offline: "#5b6b88", stopped: "#8b9bb8" }[st] || "#8b9bb8";
        g.fillText({ online: "ONLINE", stressed: "CPU STRESS", slowed: "SLOWED", offline: "OFFLINE", stopped: "STOPPED" }[st] || st.toUpperCase(), p.x, p.y + R * 0.55);
      }
      // stats under the node
      const mt = m.metrics || {}, pr = m.proc || {};
      const ly = p.y + R + 26;
      g.font = "12px ui-monospace, Consolas, monospace"; g.fillStyle = "#c9d5ea";
      g.fillText(`${share != null ? share.toFixed(0) + "%  ·  " : ""}CPU ${m.sys ? m.sys.cpu.toFixed(0) : "-"}%`, p.x, ly);
      const weedOk = pr.weed && pr.weed.s3 && pr.weed.filer && pr.weed.volume && pr.weed.master;
      g.fillStyle = weedOk ? OK : (pr.weed && (pr.weed.s3 || pr.weed.master)) ? WARN : "#5b6b88";
      g.fillText(`● storage${weedOk ? "" : pr.weed ? " partial" : " -"}`, p.x, ly + 15);
      g.fillStyle = "#5b6b88"; g.font = "11px ui-monospace, Consolas, monospace"; g.fillText(m.ip || "", p.x, p.y - R - 16);
    }
    if (!cls.some(c => c.state === "running") && ns.some(m => ELIGIBLE.has(m.state))) {
      g.fillStyle = "rgba(139,155,184,0.6)"; g.font = "600 13px Inter, Segoe UI, sans-serif"; g.textAlign = "center";
      g.fillText("idle", w * 0.5, h * 0.96);
    }
    if (!ns.length || ns.every(m => m.state === "offline")) {
      g.fillStyle = "rgba(139,155,184,0.8)"; g.font = "600 14px Inter, Segoe UI, sans-serif"; g.textAlign = "center";
      g.fillText("not connected: see Setup", w * 0.5, h * 0.96);
    }
  },
};
function roundRect(g, x, y, w, h, r) { g.beginPath(); g.moveTo(x + r, y); g.arcTo(x + w, y, x + w, y + h, r); g.arcTo(x + w, y + h, x, y + h, r); g.arcTo(x, y + h, x, y, r); g.arcTo(x, y, x + w, y, r); g.closePath(); }
requestAnimationFrame(() => Topo.tick());

/* ======================================================================= live render */
function bump(el, val) { if (el.dataset.v !== String(val)) { el.dataset.v = String(val); el.classList.remove("bump"); void el.offsetWidth; el.classList.add("bump"); } }

function renderHeader(snap) {
  $("#run-id").textContent = snap.run_id;
  const ns = nodes(snap), pill = $("#cluster-pill"), txt = $("#cluster-text");
  const up = ns.filter(m => ELIGIBLE.has(m.state)).length, muted = ns.filter(m => m.state === "muted");
  pill.className = "pill";
  if (ns.every(m => m.state === "offline")) { txt.textContent = "not connected"; }
  else if (muted.length) { pill.classList.add("warn"); txt.textContent = `node ${muted.map(m => m.id).join(", ")} silent · service continues`; }
  else if (up === 3 && ns.some(m => m.weed_down && m.weed_down.length)) {
    // All 3 nodes run, but part of the shared storage on a node PC is down (it is restarted
    // automatically; a node keeps working through another PC's filer meanwhile).
    pill.classList.add("warn");
    txt.textContent = "3/3 peers up · storage: " + ns.filter(m => m.weed_down && m.weed_down.length).map(m => `N${m.id} ${m.weed_down.join("+")} down`).join(", ");
  }
  else if (up === 3) { pill.classList.add("ok"); txt.textContent = "3/3 cloud peers up"; }
  else { pill.classList.add("bad"); txt.textContent = `${up}/3 cloud peers up`; }
}

function renderKpis(snap) {
  const k = snap.kpi;
  const ok = $("#k-ok"); ok.textContent = fmtInt(k.ok); $("#k-ok-sub").textContent = `of ${fmtInt(k.requests)}${k.unassigned ? " · " + fmtInt(k.unassigned) + " no resource" : ""}`;
  $("#k-okps").textContent = k.okps.toFixed(k.okps < 10 ? 1 : 0);
  const pcs = Object.entries(k.per_client || {}).map(([n, c]) => `${n} ${fmtInt(c)}`).join(" · ");
  $("#k-okps-sub").textContent = pcs;
  $("#k-p50").textContent = k.p50 == null ? "-" : k.p50.toFixed(1);
  $("#k-p50-sub").textContent = k.p95 == null ? "" : `p95 ${k.p95.toFixed(0)} ms`;
  $("#k-p99").textContent = k.p99 == null ? "-" : k.p99.toFixed(0);
  $("#k-p99-sub").textContent = "";
  const lost = $("#k-lost"); lost.textContent = fmtInt(k.lost); bump(lost, k.lost);
  $("#kpi-lost").className = "kpi " + (k.requests ? (k.lost ? "some" : "zero") : "");
  $("#k-rel").textContent = k.reliability == null ? "" : `${k.reliability.toFixed(k.reliability === 100 ? 0 : 3)}% ok`;
  $("#k-retried").textContent = fmtInt(k.retried);
  $("#k-dup").textContent = k.dup ? `${fmtInt(k.dup)} duplicate${k.dup === 1 ? "" : "s"}` : "";
  $("#k-el").textContent = fmtInt(k.elections);
  $("#k-el-sub").textContent = k.elps ? `${k.elps}/s` : "";
  const f = $("#k-fail"); f.textContent = fmtInt(k.failures); bump(f, k.failures);
  const lf = snap.failures[snap.failures.length - 1];
  $("#k-fail-sub").textContent = lf ? (lf.detect != null ? `detected in ${fmtS(lf.detect)}` : "detecting…") : "";
}

function parseBids(s) { const o = {}; (s || "").split(";").forEach(x => { const [a, b] = x.split(":"); if (a && b) o[+a] = +b; }); return o; }

function renderElection(snap) {
  const e = snap.last_election, box = $("#el-bids");
  if (!e) { box.innerHTML = [1, 2, 3].map(i => `<div class="bid nobid"><span class="who" style="color:${NODE_COL[i]}">N${i}</span><div class="bar"><i style="width:0"></i></div><span class="score">-</span></div>`).join(""); }
  else {
    const b = parseBids(e.bids), mx = Math.max(0.0001, ...Object.values(b));
    box.innerHTML = [1, 2, 3].map(i => {
      const has = b[i] != null, win = e.winner === i;
      return `<div class="bid ${win ? "win" : ""} ${has ? "" : "nobid"}"><span class="who" style="color:${NODE_COL[i]}">N${i}</span>
        <div class="bar"><i style="width:${has ? Math.max(4, 100 * b[i] / mx) : 0}%;background:${NODE_COL[i]}"></i></div>
        <span class="score">${win ? '<span class="crown">ELECTED </span>' : ""}${has ? b[i].toFixed(4) : "no bid"}</span></div>`;
    }).join("");
    const sc = b[e.winner];
    $("#el-why").innerHTML = `<b style="color:${NODE_COL[e.winner]}">N${e.winner}</b> won (lowest score)` +
      (Object.keys(b).length < 3 ? ` · <span style="color:${BAD}">a silent node did not bid</span>` : "") +
      (e.round > 0 ? ` · <span style="color:${WARN}">re-election ${e.round}</span>` : "");
    $("#el-meta").textContent = `${(e.dur_us / 1000).toFixed(2)} ms`;
  }
  const bar = (el, sh) => {
    el.innerHTML = [1, 2, 3].map(i => { const v = sh[i] || 0; return `<i style="width:${v}%;background:${NODE_COL[i]}">${v >= 8 ? v.toFixed(0) + "%" : ""}</i>`; }).join("");
  };
  bar($("#share30"), snap.kpi.share30); bar($("#shareall"), snap.kpi.share_all);
  const k = snap.kpi, sa = [1, 2, 3].map(i => k.share_all[i] || 0), ss = sum(sa), sq = sa.reduce((a, x) => a + x * x, 0);
  const jain = sq ? (ss * ss) / (3 * sq) : null;
  $("#el-stats").innerHTML = [
    ["election p50", `${k.el_p50 == null ? "-" : k.el_p50.toFixed(2)} ms`],
    ["snapshot / commit", `${k.snap_ms == null ? "-" : k.snap_ms} / ${k.commit_ms == null ? "-" : k.commit_ms} ms`],
    ["fairness", jain == null ? "-" : jain.toFixed(3)],
  ].map(([a, b]) => `<div><span>${a}</span><b>${b}</b></div>`).join("");
}

function renderDock(snap) {
  const cls = clientsOf(snap);
  $("#load-state").innerHTML = cls.map(c => `<span style="color:${c.state === "running" ? OK : c.state === "offline" ? "#5b6b88" : "#8b9bb8"}">● ${c.name}</span>`).join(" ");
  for (const m of nodes(snap)) {
    const on = m.proc && m.proc.hogs > 0;
    $$(`.hog[data-node="${m.id}"]`).forEach(b => { b.classList.toggle("on", !!on); b.textContent = on ? `Stop stress N${m.id}` : `Stress N${m.id}`; });
  }
  const chip = $("#auto-chip"); chip.textContent = snap.faults_enabled ? "ON" : "off"; chip.className = "chip " + (snap.faults_enabled ? "on" : "");
  $("#auto-toggle").textContent = snap.faults_enabled ? "turn off" : "turn on";
}

function renderFailures(snap) {
  const list = $("#fail-list");
  if (!snap.failures.length) return;
  const now = snap.now;
  list.innerHTML = snap.failures.slice().reverse().map(f => {
    const kind = { manual: "mute", auto: "auto", down: "down" }[f.kind] || f.kind;
    const silent = f.end ? f.end - f.start : now - f.start;
    const prog = f.open && f.planned ? Math.min(100, 100 * (now - f.start) / f.planned) : 0;
    const chip = f.open ? `<span class="chip fail">${f.end ? "rejoining" : "silent"}</span>` : `<span class="chip ok">recovered</span>`;
    return `<div class="fcard ${f.open ? "open" : ""}">
      <div class="fh"><b style="color:${NODE_COL[f.node]}">Node ${f.node}</b><span class="muted">${kind} · ${hhmmss(f.start)}</span>${chip}</div>
      <div class="frow"><span>detected</span><b>${f.detect == null ? (f.kind === "down" ? "-" : "…") : fmtS(f.detect)}</b></div>
      <div class="frow"><span>down for</span><b>${fmtS(silent)}</b></div>
      <div class="frow"><span>ready again</span><b>${f.recovery == null ? "…" : "+" + fmtS(f.recovery)}</b></div>
      <div class="frow"><span>ok · lost · retried</span><b style="color:${f.during_lost ? BAD : OK}">${fmtInt(f.during_ok)} · ${fmtInt(f.during_lost)} · ${fmtInt(f.during_retried)}</b></div>
      ${f.open ? `<div class="prog" style="width:${prog}%"></div>` : ""}
    </div>`;
  }).join("");
}

function renderTimeline(snap) {
  const tl = $("#timeline");
  tl.innerHTML = snap.timeline.slice().reverse().map(e => `<div class="ev ${e.kind}"><time>${hhmmss(e.t)}</time><i></i><span>${esc(e.text)}</span></div>`).join("") || '<div class="empty">-</div>';
}

/* ======================================================================= setup */
async function loadSettings() {
  S.settings = await api("/api/settings");
  const tb = $("#mtable tbody");
  tb.innerHTML = S.settings.machines.map((m, i) => `<tr>
    <td>${esc(m.label)}</td>
    <td><input data-m="${i}.ip" value="${esc(m.ip)}" placeholder="10.x.x.x"></td>
    <td><input data-m="${i}.ssh_host" value="${esc(m.ssh_host)}" placeholder="same as IP"></td>
    <td><input data-m="${i}.user" value="${esc(m.user)}" placeholder="default"></td>
    <td><input class="port" data-m="${i}.port" type="number" value="${esc(m.port)}"></td>
    <td class="link" id="link-${m.key}">-</td></tr>`).join("");
  $$("[data-s]").forEach(inp => {
    const v = inp.dataset.s.split(".").reduce((o, k) => o && o[k], S.settings);
    if (inp.type === "checkbox") inp.checked = !!v; else inp.value = v == null ? "" : v;
  });
  if (!S.loadFormInit) {
    const L = S.settings.load;
    $("#ld-rate").value = L.rate; $("#ld-clients").value = L.clients; $("#ld-req").value = L.requests;
    $("#ld-stages").value = L.stages; $("#ld-ss").value = L.stage_secs;
    $("#mute-secs").value = S.settings.demo.mute_secs; $("#mute-secs-v").textContent = S.settings.demo.mute_secs;
    $("#res-count").value = S.settings.cluster.resource_count || 100;
    S.loadFormInit = true;
  }
}

$("#save-settings").addEventListener("click", async () => {
  const s = JSON.parse(JSON.stringify(S.settings));
  $$("[data-m]").forEach(inp => { const [i, f] = inp.dataset.m.split("."); s.machines[+i][f] = f === "port" ? (+inp.value || 22) : inp.value.trim(); });
  $$("[data-s]").forEach(inp => {
    const path = inp.dataset.s.split("."); let o = s; for (const k of path.slice(0, -1)) o = o[k];
    const k = path[path.length - 1];
    o[k] = inp.type === "checkbox" ? inp.checked : inp.type === "number" ? +inp.value : inp.value.trim();
  });
  try {
    await api("/api/settings", s); S.settings = s; $("#save-msg").textContent = "saved · reconnecting to the PCs…";
    toast("Settings saved", "The panel reconnects to the 6 PCs with the new values.", "ok");
    S.loadFormInit = false; loadSettings();
  } catch (e) { toast("Could not save", e.message, "fail"); }
});
$(".cfg").addEventListener("toggle", async e => { if (e.target.open) $("#cfg-preview").textContent = await (await fetch("/api/config")).text(); });

function renderSetup(snap) {
  for (const m of Object.values(snap.machines)) {
    const td = $("#link-" + m.key); if (!td) continue;
    if (m.connected) {
      const extra = m.role === "node" ? `node ${m.proc && m.proc.node ? "running" : "stopped"} · S3 ${m.proc && m.proc.weed && m.proc.weed.s3 ? "up" : "down"}` : `loadgen ${m.state}`;
      td.innerHTML = `<span style="color:${OK}">● live</span> ${esc(m.host || "")}<span class="e" style="color:#8b9bb8">${extra}</span>${m.fatal ? `<span class="e">${esc(m.fatal)}</span>` : ""}`;
    } else if (!m.ip) td.innerHTML = `<span class="muted">no IP</span>`;
    else td.innerHTML = `<span style="color:${BAD}">● no link</span><span class="e">${esc(m.error || "connecting…")}</span>`;
  }
}

/* ======================================================================= consoles */
function renderConsoles(snap) {
  const list = $("#action-list");
  list.innerHTML = snap.actions.slice().reverse().map(a => {
    const chip = a.status === "running" ? "run" : a.status;
    const steps = a.steps.map(st => `<div class="act-step"><div class="st"><span class="chip ${st.status === "running" ? "run" : st.status}">${st.status}</span>${esc(st.title)}</div>
      ${Object.entries(st.hosts).map(([h, r]) => `<div class="act-host"><div class="hn"><span class="chip ${r.status === "running" ? "run" : r.status}">${r.status}</span>${esc(h)}</div>${r.out ? `<pre>${esc(r.out)}</pre>` : ""}</div>`).join("")}</div>`).join("");
    return `<details class="act-item" data-id="${a.id}" ${S.openActions.has(a.id) ? "open" : ""}><summary><b>${esc(a.title)}</b><span class="when">${hhmmss(a.started)}</span><span class="chip ${chip}">${a.status}</span></summary>${a.summary ? `<div class="sum">${esc(a.summary)}</div>` : ""}${steps}</details>`;
  }).join("") || '<div class="empty">No action yet.</div>';
  $$(".act-item", list).forEach(d => d.addEventListener("toggle", () => { d.open ? S.openActions.add(d.dataset.id) : S.openActions.delete(d.dataset.id); }));
  const ms = Object.values(snap.machines);
  if (!S.logKey && ms.length) S.logKey = ms[0].key;
  const pick = $("#log-pick");
  if (pick.childElementCount !== ms.length) {
    pick.innerHTML = ms.map(m => `<button data-k="${m.key}">${esc(m.label)}</button>`).join("");
    $$("button", pick).forEach(b => b.addEventListener("click", () => { S.logKey = b.dataset.k; renderConsoles(S.snap); }));
  }
  $$("button", pick).forEach(b => b.classList.toggle("active", b.dataset.k === S.logKey));
  const m = snap.machines[S.logKey], term = $("#term");
  if (m) {
    const atBottom = term.scrollTop + term.clientHeight >= term.scrollHeight - 20;
    term.innerHTML = (m.log.length ? m.log : ["(no lines yet: node log for a node PC, loadgen output for a client PC)"]).map(l => {
      const c = /\bERROR\b/.test(l) ? "e" : /\bWARN\b/.test(l) ? "w" : "i"; return `<span class="${c}">${esc(l)}</span>`;
    }).join("\n");
    if (atBottom) term.scrollTop = term.scrollHeight;
  }
}

/* ======================================================================= demo script */
function lastAction(snap, name) { const a = snap.actions.filter(x => x.name === name); return a[a.length - 1]; }
function nodeById(snap, id) { return nodes(snap).find(m => m.id === id); }

const ACTS = [
  { title: "The cloud at a glance", goal: "3 cloud peers, 3 client PCs, replicated state.",
    say: "Three lab PCs are the cloud: each runs a RescueMesh node and a SeaweedFS replica. The other three PCs simulate many campus users. Every incident is multicast to all three peers.",
    show: "the Live mesh: blue dots are requests going to all 3 nodes, green dots are the plans coming back.",
    buttons: `<button class="btn go" data-action="load_start">▶ Start steady load</button><button class="btn ghost" data-goto="live">Open Mission Control</button>`,
    checks: [
      ["3 cloud peers online", s => { const n = nodes(s).filter(m => ELIGIBLE.has(m.state)).length; return [n === 3, n + "/3"]; }],
      ["shared storage (S3 gateway) up on all 3 node PCs", s => { const n = nodes(s).filter(m => m.proc && m.proc.weed && m.proc.weed.s3).length; return [n === 3, n + "/3"]; }],
      ["client PCs are sending incidents", s => { const n = clientsOf(s).filter(c => c.state === "running").length; return [n >= 1, n + "/3"]; }],
    ] },
  { title: "Election and transparency", goal: "Peers are elected by load; clients never pick a server.",
    say: "For every request the three peers exchange bids with their load score, and the lowest score does the work. The client just multicasts and takes the first plan: it never knows or cares which server answered.",
    show: "Latest election (the three bids, the winner), the win-share bars, and the latency KPIs.",
    buttons: "",
    checks: [
      ["every node has won elections in the last 30 s", s => { const v = [1, 2, 3].map(i => s.kpi.share30[i] || 0); return [v.every(x => x >= 5), v.map(x => x.toFixed(0) + "%").join(" / ")]; }],
      ["requests answered fast (p50 under 50 ms)", s => [s.kpi.p50 != null && s.kpi.p50 < 50, fmtMs(s.kpi.p50)]],
      ["hundreds of elections decided", s => [s.kpi.elections >= 300, fmtInt(s.kpi.elections)]],
    ] },
  { title: "Load imbalance", goal: "Stress one server: work moves away from it.",
    say: "We now make one server's machine busy. Its CPU goes up, so its load score rises, its bids lose, and the other two peers take its share. Nothing in the clients changes.",
    show: "the Load score and Win share charts: the stressed node's line jumps up and its share drops.",
    buttons: `<button class="btn warn hog" data-node="1">Stress N1</button><button class="btn warn hog" data-node="2">Stress N2</button><button class="btn warn hog" data-node="3">Stress N3</button><button class="btn ghost" data-action="hog_off">Remove stress</button>`,
    checks: [
      ["a node PC is under CPU stress", s => { const m = nodes(s).find(x => x.proc && x.proc.hogs > 0); return [!!m, m ? "node " + m.id : "-"]; }],
      ["its load score is the highest of the three", s => { const m = nodes(s).find(x => x.proc && x.proc.hogs > 0); if (!m || !m.metrics) return [false, "-"]; const mx = Math.max(...nodes(s).map(x => (x.metrics || {}).load || 0)); return [m.metrics.load >= mx, m.metrics.load.toFixed(3)]; }],
      ["its share of the work fell below 15% (last 30 s)", s => { const m = nodes(s).find(x => x.proc && x.proc.hogs > 0); if (!m) return [false, "-"]; const v = s.kpi.share30[m.id]; return [v != null && v < 15, v == null ? "-" : v.toFixed(0) + "%"]; }],
    ] },
  { title: "Server failure", goal: "Silence the elected server under load: service continues.",
    say: "Now the busiest server stops answering anything, like a crash. Its peers notice the missing heartbeats in about a second and a half, requests that were in flight are retried by the client with the same request id, and a live peer answers them.",
    show: "the red node with its countdown, the KPI 'Lost requests' staying at 0, and the failure card filling in.",
    buttons: `<button class="btn danger" data-action="mute_leader">⚡ Mute the busiest node</button><button class="btn danger" data-action="mute" data-node="1">Mute N1</button><button class="btn danger" data-action="mute" data-node="2">Mute N2</button><button class="btn danger" data-action="mute" data-node="3">Mute N3</button>`,
    checks: [
      ["a server was silenced while load was running", s => { const f = s.failures.filter(x => x.kind !== "down"); return [f.length > 0 && s.kpi.requests > 0, f.length + " so far"]; }],
      ["its peers detected it in under 2 s", s => { const f = s.failures.filter(x => x.detect != null && x.kind !== "down").pop(); return [!!f && f.detect < 2000, f ? fmtS(f.detect) : "-"]; }],
      ["0 requests lost while it was silent", s => { const f = s.failures.filter(x => !x.open && x.kind !== "down").pop(); return [!!f && f.during_lost === 0 && f.during_ok > 0, f ? `${fmtInt(f.during_ok)} ok, ${f.during_lost} lost` : "-"]; }],
    ] },
  { title: "Server recovery", goal: "The server catches up before it bids again.",
    say: "The silent node comes back on its own. Before it bids again it checks it is current with the shared state (our rejoin rule), and only then does it start winning elections again.",
    show: "the 'caught up and ready again' line in the failure card, and the node's share coming back in the Win share chart.",
    buttons: "",
    checks: [
      ["it rejoined and caught up (READY)", s => { const f = s.failures.filter(x => x.recovery != null && x.kind !== "down").pop(); return [!!f, f ? "+" + fmtS(f.recovery) : "-"]; }],
      ["it does work again (share above 5%)", s => { const f = s.failures.filter(x => x.recovery != null && x.kind !== "down").pop(); if (!f) return [false, "-"]; const v = s.kpi.share30[f.node]; return [v != null && v > 5, v == null ? "-" : v.toFixed(0) + "%"]; }],
    ] },
  { title: "Automatic fault injection", goal: "The servers silence one of themselves (up to 20 s).",
    say: "Until now we chose the victim. In the real setting the three servers run their own election with random scores, the winner goes silent for a random time up to 20 seconds, then rejoins. We leave it on and the service keeps going.",
    show: "the 'Automatic injector' chip, the failure cards marked 'automatic fault', and the Lost KPI.",
    buttons: `<button class="btn danger" id="act-auto-on">Turn automatic faults ON</button><button class="btn ghost" id="act-auto-off">Turn OFF</button>`,
    checks: [
      ["automatic injector is on", s => [!!s.faults_enabled, s.faults_enabled ? "on" : "off"]],
      ["an automatic fault happened", s => { const n = s.failures.filter(x => x.kind === "auto").length; return [n > 0, n + ""]; }],
      ["service continued with 0 lost requests", s => [s.kpi.requests > 0 && s.kpi.lost === 0 && s.failures.length > 0, fmtInt(s.kpi.lost) + " lost"]],
    ] },
  { title: "Correctness: no double work, one shared truth", goal: "Duplicates are harmless; all peers see the same state.",
    say: "A client resends a request with the same id: the system returns the stored plan instead of planning twice. Then we read the shared state through each node PC's own gateway: all three see exactly the same resources, claims and requests.",
    show: "the toast and the Consoles tab with the output of both checks.",
    buttons: `<button class="btn stop" data-action="load_stop">■ Stop load first</button><button class="btn info" data-action="dup_test">Duplicate test</button><button class="btn info" data-action="state_check">State check</button><button class="btn ghost" data-action="repair_storage">Repair storage</button><button class="btn ghost" data-goto="console">Show output</button>`,
    checks: [
      ["duplicate request answered from the store", s => { const a = lastAction(s, "dup_test"); return [!!a && a.status === "ok", a ? a.status : "-"]; }],
      ["all 3 gateways agree on the state", s => { const a = lastAction(s, "state_check"); return [!!a && a.status === "ok", a ? a.status : "-"]; }],
    ] },
  { title: "Scale", goal: "Throughput and latency as the load grows.",
    say: "Live we can only run a few minutes, so here is a short ramp: the number of clients doubles in stages while we watch throughput and latency.",
    show: "the Throughput and Latency charts during the ramp. The Demo Run tab runs this and every other scenario automatically.",
    buttons: `<button class="btn go" data-action="load_start" data-mode="ramp">▶ Start a ramp</button><button class="btn ghost" data-goto="run">Open Demo Run</button>`,
    checks: [
      ["a client ramp was run", s => [S.rampStarted, S.rampStarted ? "yes" : "-"]],
      ["over 1,000 requests answered live", s => [s.kpi.ok >= 1000, fmtInt(s.kpi.ok)]],
    ] },
];

function buildActs() {
  $("#acts").innerHTML = ACTS.map((a, i) => `<div class="card act" id="act-${i}"><div class="anum">${i + 1}</div><div>
    <h3>${esc(a.title)}</h3><div class="goal">${esc(a.goal)}</div><div class="say">${esc(a.say)}</div>
    <div class="row wrap">${a.buttons}</div><ul class="checks">${a.checks.map((c, j) => `<li id="chk-${i}-${j}">${esc(c[0])}<span class="val"></span></li>`).join("")}</ul></div></div>`).join("");
  $("#act-auto-on").addEventListener("click", e => runAction("apply_faults", { enabled: true }, e.target));
  $("#act-auto-off").addEventListener("click", e => runAction("apply_faults", { enabled: false }, e.target));
  $("#script-total").textContent = ACTS.reduce((n, a) => n + a.checks.length, 0);
}

function renderScript(snap) {
  let done = 0, total = 0;
  ACTS.forEach((a, i) => {
    let all = true;
    a.checks.forEach((c, j) => {
      total++;
      let r; try { r = c[1](snap); } catch (e) { r = [false, "-"]; }
      const id = i + "-" + j;
      if (r[0]) S.sticky[id] = r[1];
      const pass = r[0] || S.sticky[id] !== undefined;
      if (pass) done++; else all = false;
      const li = $("#chk-" + id); if (!li) return;
      li.classList.toggle("pass", pass);
      $(".val", li).textContent = r[0] ? r[1] : (S.sticky[id] !== undefined ? S.sticky[id] : r[1]);
    });
    $("#act-" + i).classList.toggle("done", all);
  });
  $("#script-done").textContent = done;
  const p = total ? done / total : 0;
  $("#script-ring").style.background = `conic-gradient(${OK} ${p * 360}deg, rgba(255,255,255,0.07) 0)`;
  $("#script-ring").innerHTML = `<span style="background:#0b1220;border-radius:50%;width:42px;height:42px;display:grid;place-items:center">${Math.round(p * 100)}%</span>`;
}

/* ======================================================================= demo run
   The server runs the tests (DemoRun in server.py) so a run survives closing the page; this
   only shows snap.demo and sends run / stop. */
const RUNSEL_KEY = "rm-demo-run-selection";
function loadSel() { try { return JSON.parse(localStorage.getItem(RUNSEL_KEY) || "null"); } catch (e) { return null; } }
function saveSel() { try { localStorage.setItem(RUNSEL_KEY, JSON.stringify(selectedTests())); } catch (e) { /* private window */ } }
function selectedTests() { return $$(".rt-pick").filter(c => c.checked).map(c => c.value); }
function fmtDur(s) { s = Math.max(0, Math.round(s)); return s >= 60 ? Math.floor(s / 60) + " min " + String(s % 60).padStart(2, "0") + " s" : s + " s"; }
function runLeft(d) { return d && d.until ? (d.until - S.snap.now - (Date.now() - S.snapAt)) / 1000 : null; }
const RUN_CHIP = { pending: ["", "waiting"], running: ["run", "running"], pass: ["ok", "PASS"], fail: ["fail", "FAIL"], skipped: ["", "skipped"], stopped: ["", "stopped"] };

function buildRun(tests) {
  const box = $("#run-tests");
  if (box.dataset.ready || !tests) return;
  box.dataset.ready = "1";
  const sel = loadSel();
  box.innerHTML = tests.map((t, i) => `<div class="card rtest" id="rt-${t.id}">
    <div class="rt-head"><label class="chk"><input type="checkbox" class="rt-pick" value="${t.id}" ${!sel || sel.includes(t.id) ? "checked" : ""}></label>
      <span class="anum">${i + 1}</span><div class="rt-title"><h3>${esc(t.title)}</h3><div class="goal">${esc(t.goal)}</div></div>
      <span class="muted rt-est">~${fmtDur(t.secs)}</span><span class="chip rt-chip hidden"></span>
      <button class="btn ghost xs rt-one" data-test="${t.id}">Run</button></div>
    <ul class="checks rt-checks"></ul><div class="rt-note"></div></div>`).join("");
  $$(".rt-pick").forEach(c => c.addEventListener("change", () => { saveSel(); runEstimate(); }));
  $$(".rt-one").forEach(b => b.addEventListener("click", () => startRun([b.dataset.test], b)));
  runEstimate();
}
function runEstimate() {
  const tests = (S.snap && S.snap.demo_tests) || [], sel = new Set(selectedTests());
  const secs = tests.filter(t => sel.has(t.id)).reduce((n, t) => n + t.secs, 0);
  $("#run-est").textContent = sel.size ? `~${fmtDur(secs)}` : "";
}
async function startRun(tests, btn) {
  if (!tests.length) { toast("Nothing to run", "Select at least one test.", "fail"); return; }
  const cut = tests.includes("powercut") ? " It also power-cuts one node PC and brings it back." : "";
  if (!confirm(`Run ${tests.length} test${tests.length === 1 ? "" : "s"}? The panel starts and stops load, stresses, mutes and restarts nodes by itself.${cut}`)) return;
  await runAction("demo_run", { tests, rate: +$("#run-rate").value || 20, clients: +$("#run-clients").value || 4 }, btn);
  showView("live");
}
$("#run-go").addEventListener("click", e => startRun(selectedTests(), e.target));
$("#run-stop").addEventListener("click", e => runAction("demo_stop", {}, e.target));
$("#run-all").addEventListener("click", () => { $$(".rt-pick").forEach(c => c.checked = true); saveSel(); runEstimate(); });
$("#run-none").addEventListener("click", () => { $$(".rt-pick").forEach(c => c.checked = false); saveSel(); runEstimate(); });
document.addEventListener("click", e => {
  if (e.target.closest("#rb-stop")) runAction("demo_stop", {}, e.target);
  if (e.target.closest("#rb-close")) { S.bannerClosed = S.snap && S.snap.demo && S.snap.demo.id; $("#run-banner").classList.add("hidden"); }
});

function renderRun(snap) {
  if (!snap) return;
  buildRun(snap.demo_tests);
  const d = snap.demo, running = !!d && d.status === "running";
  const byId = {}; if (d) d.tests.forEach(t => byId[t.id] = t);
  // the tab badge and the Mission Control banner are drawn on every snapshot
  const done = d ? d.tests.filter(t => t.status === "pass").length : 0, total = d ? d.tests.length : 0;
  const badge = $("#run-badge");
  badge.textContent = running ? `${d.current + 1}/${total}` : d ? `${done}/${total}` : "";
  badge.className = "tab-badge " + (running ? "run" : d ? (d.status === "ok" ? "ok" : "fail") : "");
  renderBanner(snap, d, running, done, total);
  if (S.view !== "run") return;
  $("#run-go").disabled = running; $("#run-stop").disabled = !running;
  $$(".rt-one").forEach(b => b.disabled = running);
  $("#run-total").textContent = total || selectedTests().length; $("#run-passed").textContent = done;
  const p = total ? done / total : 0;
  $("#run-ring").style.background = `conic-gradient(${OK} ${p * 360}deg, rgba(255,255,255,0.07) 0)`;
  $("#run-ring").innerHTML = `<span style="background:#0b1220;border-radius:50%;width:42px;height:42px;display:grid;place-items:center">${Math.round(p * 100)}%</span>`;
  $("#run-status").textContent = !d ? "" : running ? fmtDur((snap.now - d.started) / 1000) : `${hhmmss(d.started)} · ${fmtDur((d.ended - d.started) / 1000)}`;
  const left = runLeft(d);
  $("#run-phase").innerHTML = running ? `<span class="chip run">now</span> ${esc(d.phase)}${left != null && left > 0 ? ` <b>${fmtDur(left)}</b>` : ""}` : "";
  for (const t of snap.demo_tests || []) {
    const card = $("#rt-" + t.id); if (!card) continue;
    const r = byId[t.id], st = r ? r.status : null;
    const [cls, label] = st ? RUN_CHIP[st] : ["", "-"];
    const chip = $(".rt-chip", card); chip.className = "chip rt-chip " + cls + (st ? "" : " hidden"); chip.textContent = label;
    card.classList.toggle("done", st === "pass"); card.classList.toggle("failed", st === "fail"); card.classList.toggle("active", st === "running");
    $(".rt-checks", card).innerHTML = r ? r.checks.map(c => `<li class="${c.pass == null ? "info" : c.pass ? "pass" : "bad"}">${esc(c.label)}<span class="val">${esc(c.value)}</span></li>`).join("") +
      (st === "running" ? `<li class="wait">${esc(d.phase)}${left != null && left > 0 ? " · " + fmtDur(left) : ""}</li>` : "") : "";
    $(".rt-note", card).textContent = r && r.note ? "Error: " + r.note : "";
  }
}

function renderBanner(snap, d, running, done, total) {
  const b = $("#run-banner");
  const recent = d && !running && snap.now - d.ended < 15 * 60 * 1000 && S.bannerClosed !== d.id;
  if (!d || !(running || recent)) { b.classList.add("hidden"); return; }
  b.classList.remove("hidden");
  b.className = "run-banner " + (running ? "running" : d.status === "ok" ? "ok" : "fail");
  const dots = d.tests.map((t, i) => `<i class="rd ${t.status}" title="${esc(t.title)}: ${t.status}">${i + 1}</i>`).join("");
  const cur = d.tests[d.current] || {}, left = runLeft(d);
  b.innerHTML = running
    ? `<b>DEMO RUN</b><span class="rb-test">${d.current + 1}/${total} ${esc(cur.title || "")}</span><span class="rb-phase">${esc(d.phase)}${left != null && left > 0 ? ` <b>${fmtDur(left)}</b>` : ""}</span><span class="rb-dots">${dots}</span><button class="btn stop xs" id="rb-stop">Stop</button>`
    : `<b>DEMO RUN</b><span class="rb-test">${done}/${total} passed</span><span class="rb-dots">${dots}</span><button class="btn ghost xs" data-goto="run">results</button><button class="btn ghost xs" id="rb-close">&times;</button>`;
}

/* ======================================================================= main loop */
function render(snap) {
  renderHeader(snap);
  trackActions(snap);
  if (S.view === "live") { renderKpis(snap); renderElection(snap); renderDock(snap); renderCharts(snap); renderFailures(snap); renderTimeline(snap); }
  if (S.view === "script") { renderScript(snap); renderDock(snap); }
  else renderScript(snap); // keeps sticky checks up to date in the background
  if (S.view === "setup") renderSetup(snap);
  if (S.view === "console") renderConsoles(snap);
  renderRun(snap);
}

function onSnap(snap) {
  if (snap.error) { $("#cluster-text").textContent = snap.error; return; }
  S.snap = snap; S.snapAt = Date.now();
  render(snap);
}
function offline() { const p = $("#cluster-pill"); p.className = "pill bad"; $("#cluster-text").textContent = "panel server unreachable"; }
function connect() {
  // ?poll = one fetch per second instead of the stream (for proxies that break Server-Sent Events)
  if (location.search.includes("poll")) {
    const tick = () => fetch("/api/snapshot").then(r => r.json()).then(onSnap).catch(offline);
    tick(); setInterval(tick, 1000);
    return;
  }
  const es = new EventSource("/api/stream");
  es.onmessage = ev => onSnap(JSON.parse(ev.data));
  es.onerror = offline;
}

setInterval(() => { $("#clock").textContent = new Date().toTimeString().slice(0, 8); }, 500);
window.addEventListener("resize", () => { if (S.snap && S.view === "live") renderCharts(S.snap); });
buildActs();
loadSettings().catch(e => toast("Could not load settings", e.message, "fail"));
connect();
if (location.hash.length > 1 && $("#v-" + location.hash.slice(1))) showView(location.hash.slice(1));
window.addEventListener("hashchange", () => { if ($("#v-" + location.hash.slice(1))) showView(location.hash.slice(1)); });
