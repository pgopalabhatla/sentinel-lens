// Variant B: one static page, no build step. ECharts from a CDN (SVG renderer), data from demo.json.
// What may be shown at the clock comes only from ../shared/visibility.js (the tested port of
// dashboard/data.py); this file only draws it.
import { uiState, seriesPoints, attackerView, nextPageOut } from "../shared/visibility.js";
import { hhmm, hhmmss, day, isoMinute, verdictText, verdictClass, speedLabel, readUrl, fmtNum } from "../shared/format.js";

const DATA = "../demo/live-week-2/ui/";
const H = 3600e3;
const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
async function loadData() {
  const inline = document.getElementById("demo-data");             // the single-page build inlines both files
  if (inline) return JSON.parse(inline.textContent);
  const [demo, series] = await Promise.all(["demo.json", "series.json"].map((f) => fetch(DATA + f).then((r) => r.json())));
  return { demo, series };
}

const { demo, series } = await loadData();
const S = demo.settings, START = demo.meta.start_ms, END = demo.meta.end_ms;
const state = { ...readUrl(location.search, START, END, S.default_speed_min), playing: false };
const openEvidence = new Set();
let shownPages = null, timer = null, chart = null;

function setT(t) { state.t = Math.min(END, Math.max(START, t)); render(); }
function syncUrl() {
  const u = new URLSearchParams({ t: isoMinute(state.t) });
  if (state.page != null) u.set("page", state.page);
  if (state.series) u.set("series", state.series);
  if (state.reveal) u.set("reveal", "1");
  history.replaceState(null, "", "?" + u);
}
function play(on) {
  state.playing = on;
  clearInterval(timer);
  if (on) timer = setInterval(() => {
    if (state.t >= END) return play(false);
    setT(state.t + state.speed * 60e3);
    if (state.t >= END) play(false);
  }, S.tick_ms);
  render();
}

// ------------------------------------------------------------------ static shell, built once
$("#app").innerHTML = `
  <header class="sl-strip">
    <span class="sl-brand">Sentinel Lens</span>
    <span class="sl-clock" data-testid="clock"></span>
    <div class="sl-controls">
      <button class="sl-btn" data-testid="play" id="play"></button>
      <button class="sl-btn" data-testid="next-page" id="next">Next page</button>
      <select class="sl-select" data-testid="speed" id="speed" aria-label="Speed">
        ${S.speeds_min.map((m) => `<option value="${m}">${speedLabel(m)}</option>`).join("")}
      </select>
    </div>
    <label class="sl-toggle"><input type="checkbox" data-testid="reveal" id="reveal"> Reveal answer key</label>
    <div class="sl-count"><b data-testid="page-count"></b><span>page-outs so far</span></div>
  </header>
  <div class="sl-scrubrow"><input class="sl-scrub" type="range" id="scrub" aria-label="Simulated time"
       min="${START}" max="${END}" step="${demo.meta.bucket_ms}"></div>
  <main class="sl-main">
    <section class="sl-timeline" data-testid="timeline"><p class="sl-eyebrow">Pages, newest first</p><div id="cards"></div></section>
    <section class="sl-detail" id="detail"></section>
  </main>
  <section class="sl-chartwrap">
    <div class="sl-chart-head"><h2 id="chart-title"></h2><div class="sl-chips" id="chips"></div></div>
    <div class="sl-legend" id="legend"></div>
    <div class="sl-chart" data-testid="chart" id="chart"></div>
  </section>
  <section id="key"></section>`;

$("#play").onclick = () => play(!state.playing);
$("#next").onclick = () => { const n = nextPageOut(demo, state.t); play(false); if (n != null) { state.page = null; setT(n); } };
$("#speed").onchange = (e) => { state.speed = Number(e.target.value); };
$("#reveal").onchange = (e) => { state.reveal = e.target.checked; render(); };
$("#scrub").oninput = (e) => { play(false); setT(Number(e.target.value)); };
$("#cards").onclick = (e) => {
  const card = e.target.closest("[data-page]");
  if (card) { state.page = Number(card.dataset.page); state.series = null; render(); }
};
$("#chips").onclick = (e) => { const c = e.target.closest("[data-series]"); if (c) { state.series = c.dataset.series; render(); } };
$("#detail").addEventListener("toggle", (e) => {
  const id = e.target.dataset?.note;
  if (id) e.target.open ? openEvidence.add(id) : openEvidence.delete(id);
}, true);

// ------------------------------------------------------------------ render
function noteCard(r, title) {
  const id = `${r.page}-${r.type}-${r.ready_at}`;
  const ev = r.evidence || [];
  const guard = r.guard ? `<p class="sl-advisory">Guard: converted from "${esc(r.guard.converted_from)}": ${esc(r.guard.reason)}</p>` : "";
  return `<article class="sl-note" data-testid="note" data-type="${r.type}">
    <h3>${title}<span class="sl-when">written ${day(r.ready_at)} ${hhmmss(r.ready_at)} UTC</span></h3>
    <p><span class="sl-pill ${verdictClass(r.needs_human)}">${esc(verdictText(r.needs_human))}</span>
       <span class="sl-advisory">confidence ${esc(r.confidence)} · advisory only</span></p>${guard}
    <div class="sl-field" data-testid="cause"><b>Cause · ${esc(r.cause?.category || "-")}</b><p>${esc(r.cause?.description || "-")}</p></div>
    <div class="sl-field" data-testid="impact"><b>Impact · ${esc((r.impact?.degraded || []).join(", ") || "none")}</b><p>${esc(r.impact?.description || "-")}</p></div>
    <details class="sl-evidence" data-testid="evidence" data-note="${id}" ${openEvidence.has(id) ? "open" : ""}>
      <summary>Evidence (${ev.length})</summary>
      <ul>${ev.map((e) => `<li><code>${esc(e.tool)}</code>${esc(e.finding)}</li>`).join("")}</ul>
    </details>
    <div class="sl-field" data-testid="next-step" style="margin-top:10px"><b>Next step</b><p>${esc(r.next_step || "-")}</p></div>
  </article>`;
}

function renderDetail(page, notes) {
  if (!page) return `<p class="sl-eyebrow">Selected page</p><p class="sl-empty">No page-out yet. Press play or Next page.</p>`;
  const written = notes.filter((r) => r.type !== "link");
  const last = written[written.length - 1];
  const from = last ? (last.type === "follow_up" ? "from the follow-up" : "from the first note") : "no note written yet";
  const cards = notes.map((r) => r.type === "link"
    ? `<p class="sl-link">${hhmm(r.ready_at)} UTC: page ${esc((r.absorbed || []).join(", "))} merged into this page after it paged.</p>`
    : noteCard(r, r.type === "follow_up" ? "Follow-up" : "Triage note")).join("");
  return `<div data-testid="page-detail" data-page="${page.page}">
    <p class="sl-eyebrow">Page ${page.page} · paged ${day(page.paged_at)} ${hhmm(page.paged_at)} UTC · ${page.alerts.length} alerts now${page.merged_into ? ` · merged into page ${page.merged_into}` : ""}</p>
    <h1 class="sl-headline" data-testid="reason-headline">${esc(page.reason)}</h1>
    <div class="sl-badge ${verdictClass(page.verdict)}" data-testid="verdict">${esc(verdictText(page.verdict))}</div>
    <span class="sl-advisory" style="margin-left:10px">${from}</span>
    ${cards || `<p class="sl-empty">The triage agent has not written a note yet.</p>`}
  </div>`;
}

function render() {
  const t = state.t, ui = uiState(demo, t);
  if (state.page != null && !ui.pages.some((p) => p.page === state.page)) state.page = null;
  const page = state.page != null ? ui.pages.find((p) => p.page === state.page) : ui.pages[0];

  // strip
  const clock = $("[data-testid=clock]");
  clock.dataset.ms = t;
  clock.innerHTML = `${day(t)} ${hhmm(t)}<small>UTC</small>`;
  $("#play").textContent = state.playing ? "Pause" : "Play";
  $("#play").classList.toggle("is-on", state.playing);
  $("#speed").value = state.speed;
  $("#reveal").checked = state.reveal;
  $("#scrub").value = t;
  $("[data-testid=page-count]").textContent = ui.page_count;

  // timeline: a card that was not there on the previous render slides in
  const prev = shownPages;
  shownPages = new Set(ui.pages.map((p) => p.page));
  $("#cards").innerHTML = ui.pages.length ? ui.pages.map((p) => {
    const fresh = prev && !prev.has(p.page) ? " is-new" : "";
    return `<button class="sl-card${fresh}${page && p.page === page.page ? " is-selected" : ""}" data-testid="page-card" data-page="${p.page}" data-paged-at-ms="${p.paged_at}">
      <span class="sl-card-time">${day(p.paged_at)} ${hhmm(p.paged_at)} UTC · page ${p.page}</span>
      <span class="sl-card-reason" data-testid="card-reason">${esc(p.reason)}</span>
      <span class="sl-pill ${verdictClass(p.verdict)}">${esc(verdictText(p.verdict))}</span>
      <span class="sl-card-meta">${p.alerts.length} alerts${p.merged_into ? ` · merged into page ${p.merged_into}` : ""}</span>
    </button>`;
  }).join("") : `<p class="sl-empty">No page-out yet.</p>`;

  $("#detail").innerHTML = renderDetail(page, page ? ui.notes[String(page.page)] : []);
  renderChart(page, ui);
  renderKey();
  if (!state.playing) syncUrl();
}

function renderChart(page, ui) {
  const el = $("#chart");
  chart ??= echarts.init(el, null, { renderer: "svg" });
  if (!page) {
    el.dataset.series = "";
    $("#chart-title").textContent = "Series of the selected page";
    $("#chips").innerHTML = ""; $("#legend").innerHTML = "";
    chart.clear();
    return;
  }
  const sid = page.series.includes(state.series) ? state.series : page.series.includes(page.paging_target) ? page.paging_target : page.series[0];
  const info = demo.series_index[sid], seasonal = info.rule === "seasonal", t = state.t, from = t - S.lookback_hours * H;
  const pts = seriesPoints(demo, series, sid, t, S.lookback_hours);
  const alerts = page.alerts.filter((a) => a.target === sid && a.time >= from);
  const down = alerts.length && alerts.every((a) => a.direction === "down");
  el.dataset.series = sid;
  $("#chart-title").textContent = `${info.entity} ${info.label}`;
  $("#chips").innerHTML = page.series.length > 1 ? page.series.map((s) =>
    `<button class="sl-chip${s === sid ? " is-on" : ""}" data-series="${esc(s)}">${esc(demo.series_index[s].entity + " " + demo.series_index[s].label)}</button>`).join("") : "";
  $("#legend").innerHTML = [
    `<span><i style="border-color:var(--chart-value)"></i>5-min mean</span>`,
    seasonal && `<span><i style="border-color:var(--chart-expected);border-top-style:dashed"></i>expected</span>`,
    seasonal && `<span><i style="border-color:var(--chart-band);border-top-width:8px"></i>wobble band (expected +/- 1 wobble)</span>`,
    seasonal && `<span><i style="border-color:var(--chart-hot);border-top-style:dotted"></i>hot line (expected ${down ? "-" : "+"} ${S.hot_z} x wobble)</span>`,
    `<span><i style="border-color:var(--chart-hot);border-top-style:dashed"></i>alert</span>`,
    `<span><i style="border-color:var(--chart-hot);border-top-width:3px"></i>page fired</span>`,
    `<span><i style="border-color:var(--chart-change)"></i>change</span>`,
    state.reveal && `<span><i style="border-color:var(--key);border-top-width:8px"></i>injection (answer key)</span>`,
  ].filter(Boolean).join("");

  const css = getComputedStyle(document.documentElement), v = (n) => css.getPropertyValue(n).trim();
  const font = { fontFamily: "IBM Plex Mono, monospace", fontSize: 11, color: v("--text-dim") };
  const xy = (i) => pts.map((p) => [p[0], p[i]]);
  const lo = pts.map((p) => [p[0], p[2] == null || p[3] == null ? null : p[2] - p[3]]);
  const width = pts.map((p) => [p[0], p[3] == null ? null : 2 * p[3]]);
  const hot = pts.map((p) => [p[0], p[2] == null || p[3] == null ? null : p[2] + (down ? -1 : 1) * S.hot_z * p[3]]);
  const marks = [
    ...alerts.map((a) => ({ xAxis: a.time, name: "alert", lineStyle: { color: v("--chart-hot"), type: "dashed", width: 1, opacity: 0.75 }, label: { show: false } })),
    ...(page.paged_at >= from ? [{ xAxis: page.paged_at, name: "page", lineStyle: { color: v("--chart-hot"), type: "solid", width: 3 }, label: { show: true, formatter: `page ${page.page}`, color: v("--amber"), fontFamily: font.fontFamily, fontSize: 11, position: "insideEndTop" } }] : []),
    ...ui.changes.filter((c) => c.ts_ms >= from).map((c) => ({ xAxis: c.ts_ms, name: "change", lineStyle: { color: v("--chart-change"), type: "solid", width: 1.5 }, label: { show: true, formatter: c.change_id, color: v("--teal"), fontFamily: font.fontFamily, fontSize: 11, position: "insideEndBottom" } })),
  ];
  const areas = state.reveal ? attackerView(demo, t).all.filter((i) => i.stop > from && i.start < t && i.series.includes(sid))
    .map((i) => [{ xAxis: Math.max(i.start, from), name: i.id }, { xAxis: Math.min(i.stop, t) }]) : [];
  chart.setOption({
    animation: false, useUTC: true, backgroundColor: "transparent",
    grid: { left: 70, right: 28, top: 28, bottom: 34 },
    tooltip: { trigger: "axis", backgroundColor: v("--bg-raise"), borderColor: v("--line-strong"), textStyle: { ...font, color: v("--text") } },
    xAxis: { type: "time", min: from, max: t, axisLabel: { ...font, formatter: "{HH}:{mm}", hideOverlap: true },
             axisLine: { lineStyle: { color: v("--line-strong") } }, splitLine: { show: true, lineStyle: { color: v("--line"), opacity: 0.5 } } },
    yAxis: { type: "value", scale: true, axisLabel: { ...font, formatter: fmtNum }, splitLine: { lineStyle: { color: v("--line"), opacity: 0.5 } } },
    series: [
      ...(seasonal ? [
        { name: "band-lo", type: "line", data: lo, stack: "band", symbol: "none", lineStyle: { opacity: 0 }, silent: true, tooltip: { show: false } },
        { name: "wobble band", type: "line", data: width, stack: "band", symbol: "none", lineStyle: { opacity: 0 }, areaStyle: { color: v("--chart-band") }, silent: true, tooltip: { show: false } },
        { name: "expected", type: "line", data: xy(2), symbol: "none", lineStyle: { color: v("--chart-expected"), type: "dashed", width: 1.5 } },
        { name: "hot line", type: "line", data: hot, symbol: "none", lineStyle: { color: v("--chart-hot"), type: "dotted", width: 1.5 } },
      ] : []),
      { name: "5-min mean", type: "line", data: xy(1), symbol: "none", lineStyle: { color: v("--chart-value"), width: 1.8 },
        markLine: { symbol: "none", silent: true, data: marks, animation: false },
        markArea: { silent: true, itemStyle: { color: v("--key-soft"), borderColor: v("--key"), borderWidth: 1, borderType: "dashed" },
                    label: { color: v("--key"), fontFamily: font.fontFamily, fontSize: 11 }, data: areas } },
    ],
  }, { notMerge: true });
}

function renderKey() {
  const el = $("#key");
  if (!state.reveal) { el.innerHTML = ""; return; }
  const view = attackerView(demo, state.t), active = new Set(view.active.map((i) => i.id));
  el.innerHTML = `<div class="sl-key" data-testid="answer-key">
    <h2>Answer key: hidden from the system</h2>
    <p class="sl-advisory">The attacker's injections for the whole week, including after the clock. Nothing here reaches the detector, the pages or the triage agent. Rows active at the clock are bright; on the chart, an injection that touched the shown series is shaded.</p>
    <table><thead><tr><th>id</th><th>start</th><th>stop</th><th>kind</th><th>severity</th><th>intent</th></tr></thead><tbody>
    ${view.all.map((i) => `<tr class="${active.has(i.id) ? "is-active" : ""}"><td>${esc(i.id)}${active.has(i.id) ? " (active)" : ""}</td><td>${day(i.start)} ${hhmm(i.start)}</td><td>${hhmm(i.stop)}</td><td>${esc(i.kind === "named" ? i.scenario : "generic")}</td><td>${esc(i.severity)}</td><td>${esc(i.intent)}</td></tr>`).join("")}
    </tbody></table></div>`;
}

window.addEventListener("resize", () => chart && chart.resize());
window.__sl = { state, get chart() { return chart; }, demo };     // read by the Playwright checks only
render();
document.body.dataset.ready = "1";
