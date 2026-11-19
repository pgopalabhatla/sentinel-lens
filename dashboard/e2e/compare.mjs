// One Playwright script for every dashboard variant (docs/dashboard-compare/README.md).
//
//   cd dashboard/e2e && npm install && npx playwright install chromium
//   node compare.mjs            # all three: a (Streamlit), b (static), c (React)
//   node compare.mjs b c        # some
//
// For each variant it starts the variant's own server, then, through the same URL contract
// (?t=<ISO>&page=<n>&series=<id>&reveal=1&speed=<min>) and the same DOM contract (data-testid=clock,
// page-count, page-card[data-page], page-detail, verdict, note, cause, impact, evidence, ...):
//   - screenshots at 5 fixed clock times x 2 viewports (+1 with the answer key revealed)
//   - no future data in the system panels, against the reference view (visibility.js, which
//     tests/test_dashboard_ui.py checks equals dashboard/data.py)
//   - chart: x labels are UTC 24 h, and the alert / page / change markers sit where those labels say
//   - select page 14 then page 17: page 17 shows only its own notes (the chg-0007 regression)
//   - no ellipsis truncation in reason, cause, impact, evidence (full text present, nothing clipped)
//   - time to first render (median of 3 cold loads) and a full play-through at 1 h / tick
// Results: docs/dashboard-compare/<variant>/results.json and the PNGs next to it.
//
// The browser runs in America/Los_Angeles on purpose: any axis drawn in local time shows up as a
// 7-8 hour offset between the labels and the markers.
import { chromium } from "playwright";
import { spawn, execSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { uiState, seriesPoints, attackerView } from "../shared/visibility.js";
import { verdictText } from "../shared/format.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const DASH = resolve(HERE, "..");
const ROOT = resolve(DASH, "..");
const OUT = join(ROOT, "docs", "dashboard-compare");
const UI = join(DASH, "demo", "live-week-2", "ui");
const demo = JSON.parse(readFileSync(join(UI, "demo.json"), "utf8"));
const series = JSON.parse(readFileSync(join(UI, "series.json"), "utf8"));
const H = 3600e3, LOOKBACK = demo.settings.lookback_hours;

export const CLOCKS = ["2026-10-26T04:30Z", "2026-10-28T13:40Z", "2026-10-29T00:20Z", "2026-10-30T00:30Z", "2026-11-01T01:00Z"];
export const VIEWPORTS = [[1440, 900], [1280, 800]];
const PAGE_SWITCH_CLOCK = "2026-11-01T01:00Z";       // pages 14 and 17 both exist, page 17's follow-up is written
const TZ = "America/Los_Angeles";

const VARIANTS = {
  a: {
    name: "A: Streamlit", port: 8601, path: "/",
    server: () => spawn(join(ROOT, ".venv-dashboard", "bin", "streamlit"),
      ["run", "app.py", "--server.port", "8601", "--server.address", "127.0.0.1", "--server.headless", "true"],
      { cwd: join(DASH, "a_streamlit"), stdio: "ignore" }),
    chartSvg: '[class*="st-key-chart-"] .vega-embed svg', detailRoot: ".st-key-detail", timelineRoot: ".st-key-timeline",
    chartSeries: '[data-testid="chart-meta"]',
    cardClick: (n) => `.st-key-open-${n} button`,
    async settle(page) {
      await page.waitForSelector('[data-testid="ready"]', { state: "attached", timeout: 60e3 });
      await page.waitForFunction(() => {
        const app = document.querySelector('[data-testid="stApp"]');
        return app && app.getAttribute("data-test-script-state") === "notRunning" && !document.querySelector('[data-stale="true"]');
      }, null, { timeout: 60e3 });
    },
  },
  b: {
    name: "B: static HTML + ECharts", port: 8600, path: "/b_static/index.html",
    server: () => spawn("python3", ["-m", "http.server", "8600", "--bind", "127.0.0.1"], { cwd: DASH, stdio: "ignore" }),
    chartSvg: '[data-testid="chart"] svg', chartSeries: '[data-testid="chart"]', detailRoot: ".sl-detail", timelineRoot: '[data-testid="timeline"]',
    cardClick: (n) => `[data-testid="page-card"][data-page="${n}"]`,
    async settle(page) { await page.waitForSelector('body[data-ready="1"]', { timeout: 60e3 }); },
  },
  c: {
    name: "C: React + Vite + Recharts", port: 8602, path: "/",
    build: () => execSync("npm run build", { cwd: join(DASH, "c_react"), stdio: "ignore" }),
    server: () => spawn("npm", ["run", "preview"], { cwd: join(DASH, "c_react"), stdio: "ignore" }),
    chartSvg: '[data-testid="chart"] svg.recharts-surface', chartSeries: '[data-testid="chart"]', detailRoot: ".sl-detail", timelineRoot: '[data-testid="timeline"]',
    cardClick: (n) => `[data-testid="page-card"][data-page="${n}"]`,
    async settle(page) { await page.waitForSelector('body[data-ready="1"]', { timeout: 60e3 }); },
  },
};

// ---------------------------------------------------------------- helpers
const ms = (iso) => Date.parse(iso);
const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
const url = (v, q) => `http://127.0.0.1:${v.port}${v.path}?` + new URLSearchParams(q);

async function waitPort(port, timeout = 90e3) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    try { const r = await fetch(`http://127.0.0.1:${port}/`); if (r.status < 500) return; } catch {}
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`port ${port} did not open`);
}

// settled = the variant says it is done, fonts are in, the chart (if there is a page) is drawn
async function settle(v, page, expectChart) {
  await page.waitForSelector('[data-testid="clock"]', { timeout: 60e3 });
  await v.settle(page);
  await page.evaluate(() => document.fonts.ready);
  if (expectChart) await page.waitForSelector(v.chartSvg, { timeout: 30e3 });
  await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));
  await page.waitForTimeout(250);
  await v.settle(page);
}

async function open(v, ctx, q) {
  const page = await ctx.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("console", (m) => { if (m.type() === "error") errors.push(m.text().slice(0, 300)); });
  const t0 = Date.now();
  await page.goto(url(v, q));
  const ref = uiState(demo, ms(q.t));
  await settle(v, page, ref.page_count > 0);
  return { page, errors, loadMs: Date.now() - t0, ref };
}

// what the selected page's chart should show at t (from the reference view)
function expectedChart(ref, t, pageNo, sid) {
  const p = pageNo != null ? ref.pages.find((x) => x.page === pageNo) : ref.pages[0];
  if (!p) return null;
  const s = sid && p.series.includes(sid) ? sid : p.series.includes(p.paging_target) ? p.paging_target : p.series[0];
  const from = t - LOOKBACK * H;
  return {
    page: p, sid: s, from,
    alert: p.alerts.filter((a) => a.target === s && a.time >= from).map((a) => a.time),
    page_mark: p.paged_at >= from ? [p.paged_at] : [],
    change: ref.changes.filter((c) => c.ts_ms >= from).map((c) => c.ts_ms),
    lastPoint: (seriesPoints(demo, series, s, t, LOOKBACK).at(-1) || [null])[0],
  };
}

// read the chart as drawn: x labels (text) and markers (short vertical strokes, by colour and width)
async function readChart(page, svgSel) {
  return page.evaluate((sel) => {
    const svg = document.querySelector(sel);
    if (!svg) return null;
    const box = svg.getBoundingClientRect();
    const labels = [...svg.querySelectorAll("text")].map((t) => ({ text: t.textContent.trim(), r: t.getBoundingClientRect() }))
      .filter((l) => l.text && l.r.width > 0);
    const xLabels = labels.filter((l) => /^\d{1,2}(:\d{2})?\s?(AM|PM|am|pm)?$/.test(l.text) && /:|AM|PM|am|pm/.test(l.text))
      .map((l) => ({ text: l.text, x: l.r.left + l.r.width / 2, y: l.r.top }));
    const rgb = (c) => (c.match(/\d+(\.\d+)?/g) || []).slice(0, 3).map(Number);
    const near = (c, ref) => { const [r, g, b] = rgb(c); return Math.abs(r - ref[0]) < 12 && Math.abs(g - ref[1]) < 12 && Math.abs(b - ref[2]) < 12; };
    const AMBER = [240, 160, 48], TEAL = [111, 165, 154];
    let plotTop = Infinity, plotBottom = -Infinity;
    const marks = [];
    for (const el of svg.querySelectorAll("line, path")) {
      const r = el.getBoundingClientRect(), cs = getComputedStyle(el);
      if (r.width > 4 || r.height < 60) continue;                 // vertical strokes only
      const amber = near(cs.stroke, AMBER), teal = near(cs.stroke, TEAL);
      if (!amber && !teal) continue;
      const w = parseFloat(cs.strokeWidth) || 1, dashed = cs.strokeDasharray && cs.strokeDasharray !== "none";
      const kind = teal ? "change" : w >= 2.5 ? "page_mark" : dashed ? "alert" : null;
      if (kind) marks.push({ kind, x: r.left + r.width / 2 });
      plotTop = Math.min(plotTop, r.top); plotBottom = Math.max(plotBottom, r.bottom);
    }
    return { box: { left: box.left, right: box.right, width: box.width }, xLabels, marks, allText: labels.map((l) => l.text) };
  }, svgSel);
}

// fit x = a*t + b from the hour labels, reading each HH:MM as the one UTC time inside the window
function fitAxis(xLabels, from, t) {
  const pts = [];
  for (const l of xLabels) {
    const m = /^(\d{2}):(\d{2})$/.exec(l.text);
    if (!m) continue;
    const d = new Date(t);
    for (const back of [0, 1]) {
      const c = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - back, +m[1], +m[2]);
      if (c >= from - 60e3 && c <= t + 60e3) { pts.push([c, l.x]); break; }
    }
  }
  if (pts.length < 2) return null;
  const n = pts.length, mx = pts.reduce((s, p) => s + p[0], 0) / n, my = pts.reduce((s, p) => s + p[1], 0) / n;
  const a = pts.reduce((s, p) => s + (p[0] - mx) * (p[1] - my), 0) / pts.reduce((s, p) => s + (p[0] - mx) ** 2, 0);
  const b = my - a * mx, at = (x) => a * x + b;
  return { at, n, maxResidualPx: Math.max(...pts.map((p) => Math.abs(at(p[0]) - p[1]))) };
}

function checkChart(drawn, exp, t, tolPx = 3) {
  const out = { ok: true, problems: [] };
  const fail = (m) => { out.ok = false; out.problems.push(m); };
  if (!drawn) { fail("no chart svg"); return out; }
  const bad = drawn.xLabels.filter((l) => !/^\d{2}:\d{2}$/.test(l.text));
  if (bad.length) fail(`x labels not 24 h HH:MM: ${bad.map((l) => l.text).join(", ")}`);
  const fit = fitAxis(drawn.xLabels, exp.from, t);
  out.labels = drawn.xLabels.map((l) => l.text);
  if (!fit) { fail("fewer than 2 x labels readable as UTC times inside the window"); return out; }
  out.fit = { labels: fit.n, maxResidualPx: +fit.maxResidualPx.toFixed(2) };
  if (fit.maxResidualPx > tolPx) fail(`labels not evenly spaced in UTC time (residual ${fit.maxResidualPx.toFixed(1)} px)`);
  out.markers = {};
  for (const kind of ["alert", "page_mark", "change"]) {
    const want = exp[kind], got = drawn.marks.filter((m) => m.kind === kind).map((m) => m.x);
    const offsets = want.map((ts) => Math.min(...got.map((x) => Math.abs(x - fit.at(ts)))));
    out.markers[kind] = { expected: want.length, drawn: got.length, worstOffsetPx: want.length ? +Math.max(...offsets).toFixed(2) : null };
    if (got.length !== want.length) fail(`${kind}: ${got.length} drawn, ${want.length} expected`);
    if (offsets.some((o) => !(o <= tolPx))) fail(`${kind}: a marker is ${Math.max(...offsets).toFixed(1)} px from where the labels put its time`);
  }
  return out;
}

// ---------------------------------------------------------------- checks
async function checkClock(v, ctx, clock) {
  const t = ms(clock);
  const { page, errors, ref } = await open(v, ctx, { t: clock });
  const r = { clock, ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  const dom = await page.evaluate(([detailRoot, timelineRoot]) => ({
    count: document.querySelector('[data-testid="page-count"]')?.textContent.trim(),
    clockMs: Number(document.querySelector('[data-testid="clock"]')?.dataset.ms),
    cards: [...document.querySelectorAll('[data-testid="page-card"]')].map((c) => ({ page: Number(c.dataset.page), at: Number(c.dataset.pagedAtMs) })),
    detail: Number(document.querySelector('[data-testid="page-detail"]')?.dataset.page) || null,
    verdict: document.querySelector('[data-testid="verdict"]')?.textContent.trim() ?? null,
    notes: [...document.querySelectorAll('[data-testid="page-detail"] [data-testid="note"]')].map((n) => n.dataset.type),
    details: document.querySelectorAll('[data-testid="page-detail"]').length,
    system: [document.querySelector(timelineRoot), document.querySelector(detailRoot)].map((e) => e?.innerText || "").join("\n"),
    answerKey: !!document.querySelector('[data-testid="answer-key"]'),
    bodyText: document.body.innerText,
  }), [v.detailRoot, v.timelineRoot]);
  if (dom.details > 1) fail(`${dom.details} page details shown at once`);
  if (dom.clockMs !== t) fail(`clock shows ${dom.clockMs}, expected ${t}`);
  if (Number(dom.count) !== ref.page_count) fail(`page count ${dom.count}, expected ${ref.page_count}`);
  const want = ref.pages.map((p) => p.page);
  if (JSON.stringify(dom.cards.map((c) => c.page)) !== JSON.stringify(want)) fail(`cards ${dom.cards.map((c) => c.page)}, expected ${want}`);
  if (dom.cards.some((c) => c.at > t)) fail("a card from after the clock");
  const sel = ref.pages[0];
  if (sel) {
    if (dom.detail !== sel.page) fail(`detail shows page ${dom.detail}, expected the newest, ${sel.page}`);
    if (dom.verdict !== verdictText(sel.verdict)) fail(`verdict "${dom.verdict}", expected "${verdictText(sel.verdict)}"`);
    const notes = ref.notes[String(sel.page)].filter((n) => n.type !== "link").map((n) => n.type);
    if (JSON.stringify(dom.notes) !== JSON.stringify(notes)) fail(`notes ${dom.notes}, expected ${notes}`);
  }
  // nothing the system had not seen: no later note, page reason or change id anywhere in the system panels
  const future = [];
  for (const n of demo.triage) {
    if (n.type === "link") continue;
    const ready = ms(n.as_of) + Math.round((n.seconds || 0) * 1000);
    if (ready > t && n.cause?.description) future.push(["note", n.page, n.cause.description]);
  }
  const shown = new Set(ref.pages.map((p) => p.page));
  for (const [k, why] of Object.entries(demo.paging_reasons)) if (!shown.has(Number(k)) && !ref.pages.some((p) => p.reason === why)) future.push(["reason", k, why]);
  for (const c of demo.changes) if (ms(c.streamed_at) > t) future.push(["change", c.change_id, c.change_id]);
  const leaks = future.filter(([, , text]) => norm(dom.system).includes(norm(text)));
  if (leaks.length) fail(`future data shown: ${leaks.map((l) => l.slice(0, 2).join(" ")).join(", ")}`);
  if (dom.answerKey || /\batk-\d{3}\b/.test(dom.bodyText)) fail("answer key visible with the toggle off");
  // the chart
  const exp = expectedChart(ref, t, null, null);
  if (exp) {
    const shownSid = await page.evaluate((s) => document.querySelector(s)?.dataset.series, v.chartSeries);
    if (shownSid !== exp.sid) fail(`chart shows ${shownSid}, expected ${exp.sid}`);
    const drawn = await readChart(page, v.chartSvg);
    r.chart = checkChart(drawn, exp, t);
    if (!r.chart.ok) fail(...r.chart.problems.map((p) => "chart: " + p));
    const leakText = (drawn?.allText || []).filter((x) => /^chg-\d+$/.test(x) && !ref.changes.some((c) => c.change_id === x));
    if (leakText.length) fail(`chart labels a change not yet streamed: ${leakText}`);
  }
  if (errors.length) fail(`console errors: ${errors.slice(0, 3).join(" | ")}`);
  await page.close();
  return r;
}

async function checkPageSwitch(v, ctx) {
  const { page, errors, ref } = await open(v, ctx, { t: PAGE_SWITCH_CLOCK });
  const r = { ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  const text = (n) => ref.notes[String(n)].filter((x) => x.type !== "link")
    .flatMap((x) => [x.cause?.description, x.impact?.description, x.next_step, ...(x.evidence || []).map((e) => e.finding)]).filter(Boolean);
  // the whole detail column, not just the first page-detail element: stale content from the previous
  // page would sit next to the new one
  const detail = () => page.evaluate((rootSel) => {
    const root = document.querySelector(rootSel);
    for (const s of root.querySelectorAll("details")) s.open = true;                                  // evidence text too
    const ds = root.querySelectorAll('[data-testid="page-detail"]');
    return { page: ds.length === 1 ? Number(ds[0].dataset.page) : [...ds].map((d) => d.dataset.page).join("+"),
             text: root.innerText, notes: root.querySelectorAll('[data-testid="note"]').length, details: ds.length };
  }, v.detailRoot);
  await page.click(v.cardClick(14)); await settle(v, page, true);
  const d14 = await detail();
  if (d14.page !== 14 || !d14.text.includes("chg-0007")) fail(`after selecting 14: detail page ${d14.page}, chg-0007 ${d14.text.includes("chg-0007")}`);
  // detail() just opened page 14's evidence; page 17's must start collapsed (no state carried over)
  await page.click(v.cardClick(17)); await settle(v, page, true);
  const carried = await page.evaluate((root) => [...document.querySelectorAll(root + ' details[data-testid="evidence"]')]
    .filter((d) => d.open).length, v.detailRoot);
  if (carried) fail(`page 17 opens with ${carried} evidence section(s) already expanded (carried over from page 14)`);
  const d17 = await detail();
  r.after17 = { page: d17.page, notes: d17.notes, mentionsChg0007: d17.text.includes("chg-0007"), evidenceOpenOnArrival: carried };
  if (d17.page !== 17) fail(`detail column shows page(s) ${d17.page}`);
  if (d17.text.includes("chg-0007")) fail("page 17 shows chg-0007 (page 14's note)");
  const foreign = text(14).filter((s) => norm(d17.text).includes(norm(s)));
  if (foreign.length) fail(`page 17 shows ${foreign.length} of page 14's note texts`);
  const own = text(17).filter((s) => !norm(d17.text).includes(norm(s)));
  if (own.length) fail(`page 17 is missing ${own.length} of its own note texts`);
  const want = ref.notes["17"].filter((x) => x.type !== "link").length;
  if (d17.notes !== want) fail(`page 17 shows ${d17.notes} note cards, expected ${want}`);
  if (errors.length) fail(`console errors: ${errors.slice(0, 3).join(" | ")}`);
  await page.close();
  return r;
}

async function checkTruncation(v, ctx, clocks) {
  const r = { ok: true, problems: [], checked: 0 };
  for (const clock of clocks) {
    const { page, ref } = await open(v, ctx, { t: clock });
    for (const p of ref.pages.slice(0, 3)) {                    // the three newest pages at this clock
      if (p !== ref.pages[0]) { await page.click(v.cardClick(p.page)); await settle(v, page, true); }
      for (const s of await page.$$('[data-testid="page-detail"] details[data-testid="evidence"] > summary')) await s.click();
      await page.waitForTimeout(100);
      const notes = ref.notes[String(p.page)].filter((x) => x.type !== "link");
      const want = {
        reason: [p.reason],
        cause: notes.map((n) => n.cause?.description).filter(Boolean),
        impact: notes.map((n) => n.impact?.description).filter(Boolean),
        evidence: notes.flatMap((n) => (n.evidence || []).map((e) => e.finding)).filter(Boolean),
      };
      const got = await page.evaluate(() => {
        const sel = { reason: '[data-testid="reason-headline"], [data-testid="page-card"] [data-testid="card-reason"]',
                      cause: '[data-testid="cause"] p', impact: '[data-testid="impact"] p', evidence: '[data-testid="evidence"] li' };
        const out = {};
        for (const [k, s] of Object.entries(sel)) {
          out[k] = [...document.querySelectorAll(s)].map((el) => {
            const cs = getComputedStyle(el);
            const clipsX = cs.overflowX !== "visible" && el.scrollWidth > el.clientWidth + 1;
            const clipsY = cs.overflowY !== "visible" && el.scrollHeight > el.clientHeight + 1;
            return { text: el.innerText, ellipsis: cs.textOverflow === "ellipsis", clamp: cs.webkitLineClamp && cs.webkitLineClamp !== "none", clipsX, clipsY };
          });
        }
        return out;
      });
      for (const [k, texts] of Object.entries(want)) {
        for (const w of texts) {
          r.checked++;
          if (!got[k].some((g) => norm(g.text).includes(norm(w)))) { r.ok = false; r.problems.push(`${clock} page ${p.page} ${k}: full text not shown: "${w.slice(0, 60)}"`); }
        }
        const cut = got[k].filter((g) => g.ellipsis || g.clamp || g.clipsX || g.clipsY);
        if (cut.length) { r.ok = false; r.problems.push(`${clock} page ${p.page} ${k}: ${cut.length} element(s) clipped or ellipsized`); }
      }
    }
    await page.close();
  }
  return r;
}

async function checkAnswerKey(v, ctx) {
  const clock = CLOCKS[0];
  const { page } = await open(v, ctx, { t: clock, reveal: "1" });
  const r = { ok: true, problems: [] };
  const key = await page.evaluate(() => document.querySelector('[data-testid="answer-key"]')?.innerText || "");
  const ids = attackerView(demo, ms(clock)).all.map((i) => i.id);
  if (!/hidden from the system/i.test(key)) { r.ok = false; r.problems.push('no "hidden from the system" label'); }
  const missing = ids.filter((id) => !key.includes(id));
  if (missing.length) { r.ok = false; r.problems.push(`injections missing: ${missing}`); }
  r.listed = ids.length - missing.length;
  await page.close();
  return r;
}

async function firstRender(v, browser, n = 3) {
  const times = [];
  for (let i = 0; i < n; i++) {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, timezoneId: TZ });
    const { loadMs, page } = await open(v, ctx, { t: "2026-11-02T00:00Z" });
    times.push(loadMs);
    await page.close(); await ctx.close();
  }
  return { runs_ms: times, median_ms: [...times].sort((a, b) => a - b)[Math.floor(n / 2)] };
}

async function playThrough(v, ctx) {
  const { page } = await open(v, ctx, { t: "2026-10-26T00:00Z", speed: "60" });
  const end = demo.meta.end_ms;
  const playSel = v === VARIANTS.a ? ".st-key-play button" : '[data-testid="play"]';
  const counts = [];
  const t0 = Date.now();
  await page.click(playSel);
  const poll = setInterval(async () => {
    try { counts.push(Number(await page.locator('[data-testid="page-count"]').textContent({ timeout: 1000 }))); } catch {}
  }, 500);
  await page.waitForFunction((e) => Number(document.querySelector('[data-testid="clock"]')?.dataset.ms) >= e, end, { timeout: 15 * 60e3, polling: 100 });
  const wall = Date.now() - t0;
  clearInterval(poll);
  await settle(v, page, true);
  const final = Number(await page.locator('[data-testid="page-count"]').textContent());
  const monotonic = counts.every((c, i) => i === 0 || c >= counts[i - 1]);
  const ticks = Math.ceil((end - demo.meta.start_ms) / 3600e3);
  await page.close();
  return { simulated_hours: ticks, wall_ms: wall, ideal_ms: ticks * demo.settings.tick_ms, ms_per_tick: Math.round(wall / ticks),
           final_page_count: final, count_monotonic: monotonic, ok: final === 13 && monotonic };
}

async function screenshots(v, browser, dir) {
  const files = [];
  for (const [w, h] of VIEWPORTS) {
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, timezoneId: TZ, reducedMotion: "reduce" });
    for (const clock of CLOCKS) {
      const { page } = await open(v, ctx, { t: clock });
      const f = `${clock.replace(/[:]/g, "")}_${w}x${h}.png`;
      await page.screenshot({ path: join(dir, f) });
      files.push(f);
      await page.close();
    }
    if (w === 1440) {
      const { page } = await open(v, ctx, { t: CLOCKS[0], reveal: "1" });
      await page.evaluate(() => document.querySelector('[data-testid="answer-key"]')?.scrollIntoView());
      await page.waitForTimeout(200);
      const f = `${CLOCKS[0].replace(/[:]/g, "")}_${w}x${h}_reveal.png`;
      await page.screenshot({ path: join(dir, f) });
      files.push(f);
      await page.close();
    }
    await ctx.close();
  }
  return files;
}

// ---------------------------------------------------------------- main
async function run(key) {
  const v = VARIANTS[key];
  const dir = join(OUT, key);
  mkdirSync(dir, { recursive: true });
  if (v.build) v.build();
  const server = v.server();
  try {
    await waitPort(v.port);
    const browser = await chromium.launch();
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, timezoneId: TZ, reducedMotion: "reduce" });
    // a throwaway load first, so the server's own warm-up (Streamlit's first script run) is not counted
    { const { page } = await open(v, ctx, { t: CLOCKS[0] }); await page.close(); }
    const res = { variant: v.name, browser_timezone: TZ, at: new Date().toISOString() };
    if (process.env.ONLY) {                             // quick runs while checking the checks themselves
      const only = process.env.ONLY;
      if (only === "clocks") res.clocks = await Promise.all([CLOCKS[0], CLOCKS[3]].map((c) => checkClock(v, ctx, c)));
      if (only === "switch") res.page_switch_14_17 = await checkPageSwitch(v, ctx);
      if (only === "trunc") res.truncation = await checkTruncation(v, ctx, [CLOCKS[3]]);
      console.log(key, JSON.stringify(res, (k, x) => (k === "labels" ? undefined : x)).slice(0, 1500));
      await browser.close();
      return res;
    }
    res.first_render = await firstRender(v, browser);
    res.clocks = [];
    for (const c of [...CLOCKS, PAGE_SWITCH_CLOCK, "2026-10-26T00:00Z", "2026-11-02T00:00Z"]) res.clocks.push(await checkClock(v, ctx, c));
    res.page_switch_14_17 = await checkPageSwitch(v, ctx);
    res.truncation = await checkTruncation(v, ctx, [CLOCKS[1], CLOCKS[3], CLOCKS[4]]);
    res.answer_key = await checkAnswerKey(v, ctx);
    res.screenshots = await screenshots(v, browser, dir);
    res.play_through = await playThrough(v, ctx);
    res.summary = {
      no_future_data: res.clocks.every((c) => !c.problems.some((p) => /future|card from after|answer key|not yet streamed|page count|cards|clock shows|notes|verdict|detail shows/.test(p))),
      chart_utc_and_markers: res.clocks.every((c) => !c.chart || c.chart.ok),
      page_switch_14_17: res.page_switch_14_17.ok,
      no_truncation: res.truncation.ok,
      answer_key_labelled: res.answer_key.ok,
      play_through: res.play_through.ok,
      all_clock_checks: res.clocks.every((c) => c.ok),
    };
    writeFileSync(join(dir, "results.json"), JSON.stringify(res, null, 1));
    console.log(key, JSON.stringify(res.summary), `first render ${res.first_render.median_ms} ms`, `play-through ${res.play_through.wall_ms} ms`);
    for (const c of res.clocks) if (!c.ok) console.log("  ", c.clock, c.problems.join(" | "));
    for (const k of ["page_switch_14_17", "truncation", "answer_key"]) if (!res[k].ok) console.log("  ", k, res[k].problems.join(" | "));
    await browser.close();
    return res;
  } finally {
    server.kill();
  }
}

const keys = process.argv.slice(2).length ? process.argv.slice(2) : ["a", "b", "c"];
for (const k of keys) await run(k);
