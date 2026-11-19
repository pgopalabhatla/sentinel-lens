// The live week 2 replay: one static page, no build step. ECharts from a CDN (SVG renderer), data from
// demo/live-week-2/ui/demo.json. What may be shown at the clock comes only from shared/visibility.js (the
// tested port of dashboard/data.py); this file only draws it. Chosen over a Streamlit and a React build:
// docs/dashboard-compare/README.md.
import { uiState, seriesPoints, attackerView, nextPageOut } from "./shared/visibility.js";
import { hhmm, hhmmss, day, isoMinute, verdictText, verdictClass, speedLabel, readUrl, fmtNum } from "./shared/format.js";
import { chartWindow, rankSeries, pageStatus, causeLabel, FIRING_MS } from "./shared/view.js";

const DATA = "demo/live-week-2/ui/";
const CHIPS_SHOWN = 5;
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
const state = { ...readUrl(location.search, START, END, S.default_speed_min), playing: false, allChips: false };
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
// where a time sits on the slider track, as a CSS length (the thumb is --thumb wide and never leaves the track)
const trackPos = (ms) => `calc(var(--thumb) / 2 + (100% - var(--thumb)) * ${((ms - START) / (END - START)).toFixed(6)})`;

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
    <div class="sl-count" data-testid="page-counter"><b data-testid="page-count"></b><span>page-outs so far</span></div>
  </header>
  <div class="sl-scrubrow">
    <div class="sl-track" data-testid="slider">
      <div class="sl-track-shade" id="track-shade"></div>
      <input class="sl-scrub" type="range" id="scrub" aria-label="Simulated time" min="${START}" max="${END}" step="${demo.meta.bucket_ms}">
      <div class="sl-track-ticks" id="track-ticks"></div>
    </div>
    <span class="sl-keys" data-testid="shortcuts"><kbd>space</kbd> play/pause <kbd>N</kbd> next page <kbd>R</kbd> answer key</span>
  </div>
  <main class="sl-main">
    <section class="sl-timeline" data-testid="timeline"><p class="sl-eyebrow">Pages, newest first</p><div id="cards"></div></section>
    <div class="sl-right">
      <section class="sl-detail" id="detail"></section>
      <section class="sl-chartwrap">
        <div class="sl-chart-head"><h2 id="chart-title"></h2><div class="sl-chips" id="chips" data-testid="chips"></div></div>
        <div class="sl-legend" id="legend" data-testid="legend"></div>
        <div class="sl-chart" data-testid="chart" id="chart"></div>
      </section>
    </div>
  </main>
  <section id="key"></section>`;

const nextPage = () => { const n = nextPageOut(demo, state.t); play(false); if (n != null) { state.page = null; setT(n); } };
const toggleReveal = () => { state.reveal = !state.reveal; render(); };
$("#play").onclick = () => play(!state.playing);
$("#next").onclick = nextPage;
$("#speed").onchange = (e) => { state.speed = Number(e.target.value); };
$("#reveal").onchange = (e) => { state.reveal = e.target.checked; render(); };

// keyboard: space = play/pause, N = next page, R = answer key. Not while typing in a field or a select is
// focused, and not with a modifier (browser shortcuts keep working). Space is taken over on buttons too,
// so a focused button is not also "clicked" by it.
const typing = (el) => el && (el.tagName === "SELECT" || el.tagName === "TEXTAREA" || (el.tagName === "INPUT" && !["checkbox", "range"].includes(el.type)));
document.addEventListener("keydown", (e) => {
  if (e.ctrlKey || e.metaKey || e.altKey || typing(e.target)) return;
  const k = e.key.toLowerCase();
  if (k === " ") { e.preventDefault(); if (!e.repeat) play(!state.playing); }
  else if (k === "n" && !e.repeat) nextPage();
  else if (k === "r" && !e.repeat) toggleReveal();
});
document.addEventListener("keyup", (e) => { if (e.key === " " && !typing(e.target)) e.preventDefault(); });
$("#scrub").oninput = (e) => { play(false); setT(Number(e.target.value)); };
// a tick on the slider: go to the moment that page fired, with that page selected
$("#track-ticks").onclick = (e) => {
  const tick = e.target.closest("[data-page]");
  if (!tick) return;
  play(false);
  state.page = Number(tick.dataset.page); state.series = null; state.allChips = false;
  setT(Number(tick.dataset.ms));
};
$("#cards").onclick = (e) => {
  const card = e.target.closest("[data-page]");
  if (card) { state.page = Number(card.dataset.page); state.series = null; state.allChips = false; render(); }
};
$("#chips").onclick = (e) => {
  if (e.target.closest("[data-more]")) { state.allChips = !state.allChips; render(); return; }
  const c = e.target.closest("[data-series]");
  if (c) { state.series = c.dataset.series; render(); }
};
$("#detail").addEventListener("toggle", (e) => {
  const id = e.target.dataset?.note;
  if (id) e.target.open ? openEvidence.add(id) : openEvidence.delete(id);
}, true);

// ------------------------------------------------------------------ render
const noteId = (r) => `${r.page}-${r.type}-${r.ready_at}`;
const written = (r) => `written ${day(r.ready_at)} ${hhmmss(r.ready_at)} UTC`;
const tags = (xs) => xs.map((x) => `<span class="sl-tag">${esc(x)}</span>`).join("");

function noteCard(r, title, withHeading = true) {
  const id = noteId(r);
  const ev = r.evidence || [];
  const cause = causeLabel(r.cause?.category);
  const guard = r.guard ? `<p class="sl-advisory">Guard: converted from "${esc(r.guard.converted_from)}": ${esc(r.guard.reason)}</p>` : "";
  return `<article class="sl-note" data-testid="note" data-type="${r.type}">
    ${withHeading ? `<h3>${title}<span class="sl-when">${written(r)}</span></h3>` : ""}
    <p><span class="sl-pill ${verdictClass(r.needs_human)}" data-testid="note-verdict">${esc(verdictText(r.needs_human))}</span>
       <span class="sl-advisory">confidence ${esc(r.confidence)} · advisory only</span></p>${guard}
    <div class="sl-field" data-testid="cause"><b data-testid="cause-label">${esc(cause.label)}${cause.tag ? tags([cause.tag]) : ""}</b><p class="sl-prose">${esc(r.cause?.description || "-")}</p></div>
    <div class="sl-field" data-testid="impact"><b>Impact${tags(r.impact?.degraded?.length ? r.impact.degraded : ["none"])}</b><p class="sl-prose">${esc(r.impact?.description || "-")}</p></div>
    <details class="sl-evidence" data-testid="evidence" data-note="${id}" ${openEvidence.has(id) ? "open" : ""}>
      <summary>Evidence (${ev.length})</summary>
      <ul>${ev.map((e) => `<li><code>${esc(e.tool)}</code><span class="sl-prose">${esc(e.finding)}</span></li>`).join("")}</ul>
    </details>
    <div class="sl-field" data-testid="next-step" style="margin-top:10px"><b>Next step</b><p class="sl-prose">${esc(r.next_step || "-")}</p></div>
  </article>`;
}

function renderDetail(page, notes, t) {
  if (!page) return `<p class="sl-eyebrow">Selected page</p><p class="sl-empty">No page-out yet. Press play or Next page.</p>`;
  // the final answer first (the follow-up when there is one); earlier notes collapsed below it, each
  // labelled with its own answer and time; merge records last
  const notesOnly = notes.filter((r) => r.type !== "link");
  const last = notesOnly[notesOnly.length - 1];
  const from = last ? (last.type === "follow_up" ? "from the follow-up" : "from the first note") : "no note written yet";
  const title = (r) => (r.type === "follow_up" ? "Follow-up" : "Triage note");
  const earlier = notesOnly.slice(0, -1).reverse().map((r) => {
    const id = noteId(r) + "-earlier";
    return `<details class="sl-earlier" data-testid="earlier-note" data-note="${id}" ${openEvidence.has(id) ? "open" : ""}>
      <summary>Earlier: ${title(r).toLowerCase()} · <span class="sl-pill ${verdictClass(r.needs_human)}">${esc(verdictText(r.needs_human))}</span> ${written(r)}</summary>
      ${noteCard(r, title(r), false)}
    </details>`;
  }).join("");
  const links = notes.filter((r) => r.type === "link").map((r) =>
    `<p class="sl-link">${hhmm(r.ready_at)} UTC: page ${esc((r.absorbed || []).join(", "))} merged into this page after it paged.</p>`).join("");
  const cards = (last ? noteCard(last, last.type === "follow_up" ? "Follow-up: the final answer" : "Triage note")
                      : `<p class="sl-empty">The triage agent has not written a note yet.</p>`) + earlier + links;
  const st = pageStatus(page, t);
  return `<div data-testid="page-detail" data-page="${page.page}">
    <p class="sl-eyebrow">Page ${page.page} · paged ${day(page.paged_at)} ${hhmm(page.paged_at)} UTC · ${page.alerts.length} alerts now${page.merged_into ? ` · merged into page ${page.merged_into}` : ""}</p>
    <h1 class="sl-headline" data-testid="reason-headline">${esc(page.reason)}</h1>
    <p class="sl-status ${st.firing ? "is-firing" : ""}" data-testid="page-status">${st.firing ? "still firing" : `quiet since ${hhmm(st.lastAlertMs)} UTC`}</p>
    <div class="sl-badge ${verdictClass(page.verdict)}" data-testid="verdict">${esc(verdictText(page.verdict))}</div>
    <span class="sl-advisory" style="margin-left:10px">${from}</span>
    ${cards}
  </div>`;
}

function renderTrack(ui) {
  // only pages that have fired by the clock: a tick for a later page would show the future
  $("#track-ticks").innerHTML = ui.pages.map((p) =>
    `<button class="sl-tick" data-testid="slider-tick" data-page="${p.page}" data-ms="${p.paged_at}" style="left:${trackPos(p.paged_at)}"
       title="page ${p.page} · ${day(p.paged_at)} ${hhmm(p.paged_at)} UTC" aria-label="Go to page ${p.page}, ${day(p.paged_at)} ${hhmm(p.paged_at)} UTC"></button>`).join("");
  // answer key on: every injection of the week (hidden from the system; the labelled exception)
  $("#track-shade").innerHTML = state.reveal ? attackerView(demo, state.t).all.map((i) =>
    `<div class="sl-shade" data-testid="slider-injection" data-id="${esc(i.id)}" title="${esc(i.id)} (answer key)"
       style="left:${trackPos(i.start)};width:calc((100% - var(--thumb)) * ${((i.stop - i.start) / (END - START)).toFixed(6)})"></div>`).join("") : "";
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
  renderTrack(ui);

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

  $("#detail").innerHTML = renderDetail(page, page ? ui.notes[String(page.page)] : [], t);
  renderChart(page, ui);
  renderKey();
  if (!state.playing) syncUrl();
}

function renderChips(ranked, sid, t) {
  const many = ranked.length > CHIPS_SHOWN;
  const shown = state.allChips || !many ? ranked : ranked.filter((r, i) => i < CHIPS_SHOWN || r.sid === sid);
  const chip = (r) => {
    const info = demo.series_index[r.sid];
    return `<button class="sl-chip${r.sid === sid ? " is-on" : ""}${r.firing ? " is-firing" : ""}" data-testid="series-chip" data-series="${esc(r.sid)}"
      data-firing="${r.firing ? 1 : 0}" title="${r.count} alerts${r.firing ? ", alerting in the last 10 min" : ""}">${esc(info.entity + " " + info.label)}<span class="sl-chip-n">${r.count}</span></button>`;
  };
  const more = many ? `<button class="sl-chip sl-chip-more" data-more="1" data-testid="chips-more">${state.allChips ? "fewer" : `+${ranked.length - shown.length} more`}</button>` : "";
  return shown.map(chip).join("") + more;
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
  const t = state.t;
  // the series that paged first (the headline's, preselected), then the rest by alert strength (alerts up to the clock)
  const ranked = rankSeries(page, t);
  const sid = page.series.includes(state.series) ? state.series : ranked[0].sid;
  const info = demo.series_index[sid], seasonal = info.rule === "seasonal";
  // the window is anchored on the page, not the clock: paged_at - 3 h to paged_at + 2 h, capped at the clock
  const { from, to, nowInside } = chartWindow(page, t);
  const H = 3600e3;
  const pts = seriesPoints(demo, series, sid, t, (t - from) / H).filter((p) => p[0] <= to);
  const alerts = page.alerts.filter((a) => a.target === sid && a.time >= from && a.time <= to);
  const down = alerts.length && alerts.every((a) => a.direction === "down");
  const inWin = (ms) => ms >= from && ms <= to;
  const streamIn = START > from && START <= to;
  el.dataset.series = sid; el.dataset.from = from; el.dataset.to = to;
  $("#chart-title").textContent = `${info.entity} ${info.label}`;
  $("#chips").innerHTML = page.series.length > 1 ? renderChips(ranked, sid, t) : "";
  const leg = (style, text) => `<span><i style="${style}"></i>${text}</span>`;
  $("#legend").innerHTML = [
    leg("border-color:var(--chart-value)", "mean"),
    seasonal && leg("border-color:var(--chart-expected);border-top-style:dashed", "expected"),
    seasonal && leg("border-color:var(--chart-band);border-top-width:8px", "±1 wobble"),
    seasonal && leg("border-color:var(--chart-hot);border-top-style:dotted", `hot (${down ? "-" : "+"}${S.hot_z} wobble)`),
    leg("border-color:var(--chart-hot);border-top-style:dashed", "alert"),
    leg("border-color:var(--chart-hot);border-top-width:3px", "page"),
    leg("border-color:var(--chart-change)", "change"),
    nowInside && leg("border-color:var(--text)", "now"),
    streamIn && `<span data-testid="stream-start-legend"><i style="border-color:var(--text-faint);border-top-style:dotted"></i>stream start</span>`,
    state.reveal && leg("border-color:var(--key);border-top-width:8px", "injection"),
  ].filter(Boolean).join("");

  const css = getComputedStyle(document.documentElement), v = (n) => css.getPropertyValue(n).trim();
  const font = { fontFamily: "IBM Plex Mono, monospace", fontSize: 11, color: v("--text-dim") };
  const xy = (i) => pts.map((p) => [p[0], p[i]]);
  const lo = pts.map((p) => [p[0], p[2] == null || p[3] == null ? null : p[2] - p[3]]);
  const width = pts.map((p) => [p[0], p[3] == null ? null : 2 * p[3]]);
  const hot = pts.map((p) => [p[0], p[2] == null || p[3] == null ? null : p[2] + (down ? -1 : 1) * S.hot_z * p[3]]);
  const label = (text, color, position) => ({ show: true, formatter: text, color, fontFamily: font.fontFamily, fontSize: 11, position });
  const marks = [
    ...alerts.map((a) => ({ xAxis: a.time, name: "alert", lineStyle: { color: v("--chart-hot"), type: "dashed", width: 1, opacity: 0.75 }, label: { show: false } })),
    { xAxis: page.paged_at, name: "page", lineStyle: { color: v("--chart-hot"), type: "solid", width: 3 }, label: label(`page ${page.page}`, v("--amber"), "insideEndTop") },
    ...ui.changes.filter((c) => inWin(c.ts_ms)).map((c) => ({ xAxis: c.ts_ms, name: "change", lineStyle: { color: v("--chart-change"), type: "solid", width: 1.5 }, label: label(c.change_id, v("--teal"), "insideEndBottom") })),
    ...(streamIn ? [{ xAxis: START, name: "stream", lineStyle: { color: v("--text-faint"), type: "dotted", width: 1.5 }, label: label("stream starts", v("--text-dim"), "insideEndTop") }] : []),
    ...(nowInside ? [{ xAxis: t, name: "now", lineStyle: { color: v("--text"), type: "solid", width: 1 }, label: label("now", v("--text"), "insideEndBottom") }] : []),
  ];
  const areas = state.reveal ? attackerView(demo, t).all.filter((i) => i.stop > from && i.start < to && i.series.includes(sid))
    .map((i) => [{ xAxis: Math.max(i.start, from), name: i.id }, { xAxis: Math.min(i.stop, to) }]) : [];
  chart.setOption({
    animation: false, useUTC: true, backgroundColor: "transparent",
    grid: { left: 70, right: 28, top: 28, bottom: 34 },
    tooltip: { trigger: "axis", backgroundColor: v("--bg-raise"), borderColor: v("--line-strong"), textStyle: { ...font, color: v("--text") } },
    xAxis: { type: "time", min: from, max: to, axisLabel: { ...font, formatter: "{HH}:{mm}", hideOverlap: true },
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
    <p class="sl-advisory">The attacker's injections for the whole week, including after the clock. Nothing here reaches the detector, the pages or the triage agent. Rows active at the clock are bright; on the chart, an injection that touched the shown series is shaded, and on the time slider every injection is.</p>
    <table><thead><tr><th>id</th><th>start</th><th>stop</th><th>kind</th><th>severity</th><th>intent</th></tr></thead><tbody>
    ${view.all.map((i) => `<tr class="${active.has(i.id) ? "is-active" : ""}"><td>${esc(i.id)}${active.has(i.id) ? " (active)" : ""}</td><td>${day(i.start)} ${hhmm(i.start)}</td><td>${hhmm(i.stop)}</td><td>${esc(i.kind === "named" ? i.scenario : "generic")}</td><td>${esc(i.severity)}</td><td class="sl-prose">${esc(i.intent)}</td></tr>`).join("")}
    </tbody></table></div>`;
}

window.addEventListener("resize", () => chart && chart.resize());
window.__sl = { state, get chart() { return chart; }, demo, FIRING_MS };     // read by the Playwright checks only
render();
document.body.dataset.ready = "1";
