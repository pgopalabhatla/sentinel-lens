// The dashboard's browser checks (dashboard/index.html). The same script, with three variants, chose this
// dashboard: docs/dashboard-compare/README.md (that version is on branch dashboard-v2).
//
//   cd dashboard/e2e && npm install && npx playwright install chromium && node check.mjs
//
// It serves dashboard/ itself, then, through the URL contract (?t=<ISO>&page=<n>&series=<id>&reveal=1&speed=<min>)
// and the DOM's data-testid hooks:
//   - screenshots at 5 fixed clock times x 2 viewports (+1 with the answer key revealed)
//   - no future data in the system panels, against the reference view (visibility.js, which
//     tests/test_dashboard_ui.py checks equals dashboard/data.py)
//   - the first triage card is the final answer: its answer equals the headline badge, earlier notes follow
//   - chart: x labels are UTC 24 h, the alert / page / change markers sit where those labels say, and the line
//     starts at the window's left edge (no gap; at the start of the week that is the quiet-world history)
//   - select page 14 then page 17: page 17 shows only its own notes (the chg-0007 regression)
//   - no ellipsis truncation in reason, cause, impact, evidence (full text present, nothing clipped)
//   - keyboard: space play/pause, N next page, R answer key
//   - time to first render (median of 3 cold loads) and a full play-through at 1 h / tick
// Results: docs/dashboard/results.json and the PNGs next to it.
//
// The browser runs in America/Los_Angeles on purpose: any axis drawn in local time shows up as a
// 7-8 hour offset between the labels and the markers.
import { chromium } from "playwright";
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { uiState, attackerView } from "../shared/visibility.js";
import { verdictText, hhmm } from "../shared/format.js";
import { chartWindow, rankSeries, pageStatus } from "../shared/view.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const DASH = resolve(HERE, "..");
const ROOT = resolve(DASH, "..");
const OUT = join(ROOT, "docs", "dashboard");
const UI = join(DASH, "demo", "live-week-2", "ui");
const demo = JSON.parse(readFileSync(join(UI, "demo.json"), "utf8"));
const H = 3600e3, BUCKET = demo.meta.bucket_ms;

export const CLOCKS = ["2026-10-26T04:30Z", "2026-10-28T13:40Z", "2026-10-29T00:20Z", "2026-10-30T00:30Z", "2026-11-01T01:00Z"];
export const VIEWPORTS = [[1440, 900], [1280, 800]];
const PAGE_SWITCH_CLOCK = "2026-11-01T01:00Z";       // pages 14 and 17 both exist, page 17's follow-up is written
const TZ = "America/Los_Angeles";

const TARGET = {
  name: "dashboard (static HTML + ECharts)", port: 8600, path: "/index.html",
  server: () => spawn("python3", ["-m", "http.server", "8600", "--bind", "127.0.0.1"], { cwd: DASH, stdio: "ignore" }),
  chartSvg: '[data-testid="chart"] svg', chartSeries: '[data-testid="chart"]', detailRoot: ".sl-detail", timelineRoot: '[data-testid="timeline"]',
  cardClick: (n) => `[data-testid="page-card"][data-page="${n}"]`,
  async settle(page) { await page.waitForSelector('body[data-ready="1"]', { timeout: 60e3 }); },
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

// what the selected page's chart should show at t (from the reference view): the window anchored on the
// page (shared/view.js chartWindow), the strongest series unless one was chosen
function expectedChart(ref, t, pageNo, sid) {
  const p = pageNo != null ? ref.pages.find((x) => x.page === pageNo) : ref.pages[0];
  if (!p) return null;
  const s = sid && p.series.includes(sid) ? sid : rankSeries(p, t)[0].sid;
  const { from, to, nowInside } = chartWindow(p, t);
  return {
    page: p, sid: s, from, to, nowInside,
    alert: p.alerts.filter((a) => a.target === s && a.time >= from && a.time <= to).map((a) => a.time),
    page_mark: [p.paged_at],
    change: ref.changes.filter((c) => c.ts_ms >= from && c.ts_ms <= to).map((c) => c.ts_ms),
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
    const AMBER = [240, 160, 48], TEAL = [111, 165, 154], VALUE = [236, 228, 212];
    let lineLeft = null;                                          // the 5-min mean line: widest stroke in the value colour
    for (const el of svg.querySelectorAll("path, polyline")) {
      const r = el.getBoundingClientRect(), cs = getComputedStyle(el);
      if (r.width > 50 && near(cs.stroke, VALUE) && cs.fill === "none" && (lineLeft === null || r.left < lineLeft)) lineLeft = r.left;
    }
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
    return { box: { left: box.left, right: box.right, width: box.width }, xLabels, marks, lineLeft, allText: labels.map((l) => l.text) };
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
  const fit = fitAxis(drawn.xLabels, exp.from, exp.to);
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
  // no gap: the line starts at the window's left edge (at the start of the week: the quiet-world history)
  const firstPoint = Math.ceil(exp.from / BUCKET) * BUCKET;          // the first 5-minute reading inside the window
  out.lineStartOffsetPx = drawn.lineLeft == null ? null : +Math.abs(drawn.lineLeft - fit.at(firstPoint)).toFixed(2);
  if (!(out.lineStartOffsetPx <= tolPx)) fail(`the line starts ${out.lineStartOffsetPx} px from the window's left edge`);
  const streamInWindow = demo.meta.start_ms > exp.from && demo.meta.start_ms <= exp.to;
  if (streamInWindow !== drawn.allText.includes("stream starts")) fail(`"stream starts" marker ${streamInWindow ? "missing" : "shown outside the window"}`);
  if (exp.nowInside !== drawn.allText.includes("now")) fail(`"now" line ${exp.nowInside ? "missing" : "shown though the clock is past the window"}`);
  // the paging time is inside the drawn x-range, at the place the labels give it
  out.pageInRange = exp.page.paged_at >= exp.from && exp.page.paged_at <= exp.to && drawn.marks.some((m) => m.kind === "page_mark")
    && drawn.marks.filter((m) => m.kind === "page_mark").every((m) => m.x >= drawn.box.left && m.x <= drawn.box.right);
  if (!out.pageInRange) fail("the page's own time is not inside the chart's visible x-range");
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
    firstCardAnswer: document.querySelector('[data-testid="page-detail"] [data-testid="note"] [data-testid="note-verdict"]')?.textContent.trim() ?? null,
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
    // the final answer first (the follow-up when there is one), then earlier notes, newest first
    const notes = ref.notes[String(sel.page)].filter((n) => n.type !== "link").map((n) => n.type).reverse();
    if (JSON.stringify(dom.notes) !== JSON.stringify(notes)) fail(`notes ${dom.notes}, expected ${notes}`);
    r.firstCardAnswer = dom.firstCardAnswer;
    if (notes.length && dom.firstCardAnswer !== dom.verdict) fail(`first card says "${dom.firstCardAnswer}", headline badge "${dom.verdict}"`);
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
  const carried = await page.evaluate((root) => [...document.querySelectorAll(root + " details")].filter((d) => d.open).length, v.detailRoot);
  if (carried) fail(`page 17 opens with ${carried} evidence or earlier-note section(s) already expanded (carried over from page 14)`);
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
      for (const s of await page.$$('[data-testid="page-detail"] details[data-testid="earlier-note"] > summary')) await s.click();
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

async function checkKeyboard(v, ctx) {
  const { page } = await open(v, ctx, { t: "2026-10-26T00:00Z" });
  const r = { ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  const clock = () => page.evaluate(() => Number(document.querySelector('[data-testid="clock"]').dataset.ms));
  const hint = await page.evaluate(() => document.querySelector('[data-testid="shortcuts"]')?.innerText || "");
  if (!/space/i.test(hint) || !/\bN\b/.test(hint) || !/\bR\b/.test(hint)) fail(`shortcut hint missing or incomplete: "${hint}"`);
  await page.keyboard.press("n");
  const first = demo.page_records.find((x) => x.kind === "paged");
  if ((await clock()) !== ms(first.emitted_at)) fail("N did not jump to the first page-out");
  await page.keyboard.press("r");
  if (!(await page.$('[data-testid="answer-key"]'))) fail("R did not show the answer key");
  await page.keyboard.press("r");
  if (await page.$('[data-testid="answer-key"]')) fail("R did not hide the answer key again");
  await page.focus('[data-testid="next-page"]');             // space on a focused button must not also click it
  const before = await clock();
  await page.keyboard.press(" ");
  await page.waitForTimeout(1000);
  const label = await page.evaluate(() => document.querySelector('[data-testid="play"]').textContent.trim());
  if (label !== "Pause" || (await clock()) <= before) fail(`space did not start play (button "${label}")`);
  if (await page.evaluate((f) => Number(document.querySelector('[data-testid="clock"]').dataset.ms) === f, before)) fail("clock did not move");
  await page.keyboard.press(" ");
  const stopped = await clock();
  await page.waitForTimeout(1000);
  if ((await clock()) !== stopped) fail("space did not pause");
  r.hint = hint;
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
  const playSel = '[data-testid="play"]';
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
      await page.screenshot({ path: join(dir, f), fullPage: true });
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

// every page, selected in turn: its paging time is inside the chart's visible x-range, and the window is the
// page's own (paged_at - 3 h to paged_at + 2 h, capped at the clock)
async function checkAllPagesInWindow(v, ctx, clocks) {
  const r = { ok: true, problems: [], checked: 0 };
  for (const clock of clocks) {
    const t = ms(clock);
    const { page, ref } = await open(v, ctx, { t: clock });
    for (const p of ref.pages) {
      await page.click(v.cardClick(p.page)); await settle(v, page, true);
      const exp = expectedChart(ref, t, p.page, null);
      const dom = await page.evaluate((sel) => { const el = document.querySelector(sel); return { from: Number(el.dataset.from), to: Number(el.dataset.to) }; }, v.chartSeries);
      const c = checkChart(await readChart(page, v.chartSvg), exp, t);
      r.checked++;
      if (dom.from !== exp.from || dom.to !== exp.to) { r.ok = false; r.problems.push(`${clock} page ${p.page}: window ${dom.from}-${dom.to}, expected ${exp.from}-${exp.to}`); }
      if (!c.ok) { r.ok = false; r.problems.push(`${clock} page ${p.page}: ${c.problems.join("; ")}`); }
    }
    await page.close();
  }
  return r;
}

// nothing clipped: the final note's last line can be scrolled into view, uncovered and not cut by any
// ancestor; the page-out counter is fully on screen at the top; the legend is one line
async function checkLayout(v, browser, w, hgt) {
  const r = { viewport: `${w}x${hgt}`, ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  const ctx = await browser.newContext({ viewport: { width: w, height: hgt }, timezoneId: TZ, reducedMotion: "reduce" });
  for (const clock of [CLOCKS[1], CLOCKS[3], CLOCKS[4]]) {
    const { page } = await open(v, ctx, { t: clock });
    const counter = await page.evaluate(() => {
      window.scrollTo(0, 0);
      const el = document.querySelector('[data-testid="page-counter"]'), b = el.getBoundingClientRect();
      const hit = document.elementFromPoint(b.left + b.width / 2, b.top + b.height / 2);
      return { inView: b.left >= 0 && b.right <= innerWidth && b.top >= 0 && b.bottom <= innerHeight, hit: !!hit && el.contains(hit) };
    });
    if (!counter.inView || !counter.hit) fail(`${clock}: page-out counter cut off or covered (${JSON.stringify(counter)})`);
    const last = await page.evaluate(() => {
      const el = [...document.querySelectorAll('[data-testid="page-detail"] > [data-testid="note"] [data-testid="next-step"] p')].pop();
      if (!el) return { missing: true };
      // scroll only what a reader can: containers with a scrollbar (overflow auto/scroll), then the page.
      // Not scrollIntoView: it also scrolls overflow:hidden boxes, which a reader cannot.
      for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) {
        const oy = getComputedStyle(a).overflowY;
        if (oy === "auto" || oy === "scroll") a.scrollTop += el.getBoundingClientRect().bottom - a.getBoundingClientRect().bottom + 4;
      }
      window.scrollBy(0, el.getBoundingClientRect().bottom - innerHeight + 8);
      const b = el.getBoundingClientRect();
      let clipped = null;
      for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) {
        const cs = getComputedStyle(a);
        if (cs.overflowY !== "visible" || cs.overflowX !== "visible") {
          const ab = a.getBoundingClientRect();
          if (b.top < ab.top - 1 || b.bottom > ab.bottom + 1 || b.left < ab.left - 1 || b.right > ab.right + 1) clipped = a.className;
        }
      }
      const hit = document.elementFromPoint(b.left + Math.min(20, b.width / 2), b.bottom - 4);
      return { inView: b.top >= 0 && b.bottom <= innerHeight + 1, covered: !(hit && el.contains(hit)), clipped };
    });
    if (last.missing) fail(`${clock}: no final note`);
    else if (!last.inView || last.covered || last.clipped) fail(`${clock}: the note's last line is not readable (${JSON.stringify(last)})`);
    const legend = await page.evaluate(() => {
      const el = document.querySelector('[data-testid="legend"]'), cs = getComputedStyle(el);
      const lines = new Set([...el.children].map((c) => Math.round(c.getBoundingClientRect().top))).size;
      return { lines, overflow: el.scrollWidth > el.clientWidth + 1 };
    });
    if (legend.lines > 1 || legend.overflow) fail(`${clock}: legend is ${legend.lines} lines${legend.overflow ? ", overflowing" : ""}`);
    await page.close();
  }
  await ctx.close();
  return r;
}

// slider: one tick per page fired by the clock (none later), at the right place, a click goes to that page;
// answer key on: every injection shaded on the track, off: none
async function checkSlider(v, ctx) {
  const r = { ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  for (const clock of [CLOCKS[0], CLOCKS[3], "2026-11-02T00:00Z"]) {
    const t = ms(clock);
    const { page, ref } = await open(v, ctx, { t: clock });
    const ticks = await page.evaluate(() => {
      const track = document.querySelector('[data-testid="slider"]').getBoundingClientRect();
      const thumb = parseFloat(getComputedStyle(document.documentElement).getPropertyValue("--thumb"));
      return { track: { left: track.left, width: track.width }, thumb,
        ticks: [...document.querySelectorAll('[data-testid="slider-tick"]')].map((e) => { const b = e.getBoundingClientRect(); return { page: Number(e.dataset.page), ms: Number(e.dataset.ms), x: b.left + b.width / 2 }; }),
        shades: document.querySelectorAll('[data-testid="slider-injection"]').length };
    });
    const want = ref.pages.map((p) => [p.page, p.paged_at]).sort((a, b) => a[0] - b[0]);
    const got = ticks.ticks.map((x) => [x.page, x.ms]).sort((a, b) => a[0] - b[0]);
    if (JSON.stringify(got) !== JSON.stringify(want)) fail(`${clock}: ticks ${JSON.stringify(got)}, expected ${JSON.stringify(want)}`);
    if (ticks.ticks.some((x) => x.ms > t)) fail(`${clock}: a tick for a page after the clock`);
    const scale = (m) => ticks.track.left + ticks.thumb / 2 + (ticks.track.width - ticks.thumb) * (m - demo.meta.start_ms) / (demo.meta.end_ms - demo.meta.start_ms);
    const off = Math.max(0, ...ticks.ticks.map((x) => Math.abs(x.x - scale(x.ms))));
    if (off > 2) fail(`${clock}: a tick is ${off.toFixed(1)} px from its time on the track`);
    if (ticks.shades) fail(`${clock}: injections shaded with the answer key off`);
    await page.close();
  }
  // click page 3's tick at the end of the week
  const { page } = await open(v, ctx, { t: "2026-11-02T00:00Z" });
  await page.click('[data-testid="slider-tick"][data-page="3"]'); await settle(v, page, true);
  const after = await page.evaluate(() => ({ clock: Number(document.querySelector('[data-testid="clock"]').dataset.ms),
    detail: Number(document.querySelector('[data-testid="page-detail"]').dataset.page) }));
  const p3 = demo.page_records.find((x) => x.kind === "paged" && x.page === 3);
  if (after.clock !== ms(p3.emitted_at) || after.detail !== 3) fail(`tick click: clock ${after.clock}, page ${after.detail}; expected page 3 at ${ms(p3.emitted_at)}`);
  await page.close();
  const { page: rev } = await open(v, ctx, { t: CLOCKS[0], reveal: "1" });
  const shaded = await rev.evaluate(() => [...document.querySelectorAll('[data-testid="slider-injection"]')].map((e) => e.dataset.id));
  const ids = attackerView(demo, ms(CLOCKS[0])).all.map((i) => i.id);
  if (JSON.stringify(shaded) !== JSON.stringify(ids)) fail(`answer key on: shaded ${shaded}, expected ${ids}`);
  await rev.close();
  return r;
}

// the selected page: status line, series chips (strength order, strongest preselected, firing marks, 5 + "+N
// more"), the cause label, monospace vs proportional faces, headline size
async function checkPageView(v, ctx, clocks) {
  const r = { ok: true, problems: [] };
  const fail = (m) => { r.ok = false; r.problems.push(m); };
  for (const clock of clocks) {
    const t = ms(clock);
    const { page, ref } = await open(v, ctx, { t: clock });
    const p = ref.pages[0];
    if (!p) { await page.close(); continue; }
    const st = pageStatus(p, t);
    const wantStatus = st.firing ? "still firing" : `quiet since ${hhmm(st.lastAlertMs)} UTC`;
    const ranked = rankSeries(p, t);
    const dom = await page.evaluate(() => {
      const q = (s) => document.querySelector(s);
      const font = (s) => (q(s) ? getComputedStyle(q(s)).fontFamily : "");
      return {
        status: q('[data-testid="page-status"]')?.textContent.trim(),
        chips: [...document.querySelectorAll('[data-testid="series-chip"]')].map((c) => ({ sid: c.dataset.series, firing: c.dataset.firing === "1", on: c.classList.contains("is-on") })),
        more: q('[data-testid="chips-more"]')?.textContent.trim() ?? null,
        causeLabels: [...document.querySelectorAll('[data-testid="page-detail"] [data-testid="cause-label"]')].map((b) => b.textContent.trim()),
        fonts: { cause: font('[data-testid="cause"] p'), finding: font('[data-testid="evidence"] li .sl-prose'), verdict: font('[data-testid="verdict"]'),
                 clock: font('[data-testid="clock"]'), causeLabel: font('[data-testid="cause-label"]') },
        sansLoaded: document.fonts.check('15px "IBM Plex Sans"'),
        headline: parseFloat(getComputedStyle(q('[data-testid="reason-headline"]')).fontSize),
      };
    });
    if (dom.status !== wantStatus) fail(`${clock}: status "${dom.status}", expected "${wantStatus}"`);
    if (ranked.length > 1) {
      const shown = ranked.length > 5 ? ranked.slice(0, 5) : ranked;
      if (JSON.stringify(dom.chips.map((c) => c.sid)) !== JSON.stringify(shown.map((x) => x.sid))) fail(`${clock}: chips not in strength order or not the top 5`);
      if (!dom.chips[0]?.on) fail(`${clock}: the strongest series is not preselected`);
      if (dom.chips.some((c, i) => c.firing !== shown[i]?.firing)) fail(`${clock}: firing marks do not match alerts in the last 10 min`);
      const wantMore = ranked.length > 5 ? `+${ranked.length - 5} more` : null;
      if (dom.more !== wantMore) fail(`${clock}: "${dom.more}", expected "${wantMore}"`);
    }
    const notes = ref.notes[String(p.page)].filter((n) => n.type !== "link").reverse();
    const wantLabels = notes.map((n) => (!n.cause?.category || n.cause.category === "unknown" ? "Likely cause (unconfirmed)" : `Cause${n.cause.category}`));
    if (JSON.stringify(dom.causeLabels) !== JSON.stringify(wantLabels)) fail(`${clock}: cause labels ${JSON.stringify(dom.causeLabels)}, expected ${JSON.stringify(wantLabels)}`);
    if (notes.length) {
      if (!/IBM Plex Sans/.test(dom.fonts.cause) || (dom.fonts.finding && !/IBM Plex Sans/.test(dom.fonts.finding))) fail(`${clock}: sentences not in the proportional face (${dom.fonts.cause})`);
      if (!/IBM Plex Mono/.test(dom.fonts.verdict) || !/IBM Plex Mono/.test(dom.fonts.clock) || !/IBM Plex Mono/.test(dom.fonts.causeLabel)) fail(`${clock}: badge, clock or label not monospace`);
      if (!dom.sansLoaded) fail(`${clock}: IBM Plex Sans did not load`);
    }
    if (/Inter|Roboto|Arial|Space Grotesk/.test(Object.values(dom.fonts).join(","))) fail(`${clock}: a banned face is in use`);
    if (dom.headline !== 40) fail(`${clock}: headline ${dom.headline}px, expected 40`);
    await page.close();
  }
  return r;
}

// ---------------------------------------------------------------- main
async function run() {
  const v = TARGET, key = "dashboard", dir = OUT;
  mkdirSync(dir, { recursive: true });
  const server = v.server();
  try {
    await waitPort(v.port);
    const browser = await chromium.launch();
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, timezoneId: TZ, reducedMotion: "reduce" });
    { const { page } = await open(v, ctx, { t: CLOCKS[0] }); await page.close(); }   // a throwaway load first
    const res = { target: v.name, browser_timezone: TZ, at: new Date().toISOString() };
    if (process.env.ONLY) {                             // quick runs while checking the checks themselves
      const only = process.env.ONLY;
      if (only === "clocks") res.clocks = await Promise.all([CLOCKS[0], CLOCKS[3]].map((c) => checkClock(v, ctx, c)));
      if (only === "switch") res.page_switch_14_17 = await checkPageSwitch(v, ctx);
      if (only === "trunc") res.truncation = await checkTruncation(v, ctx, [CLOCKS[3]]);
      if (only === "keys") res.keyboard = await checkKeyboard(v, ctx);
      if (only === "window") res.window_all_pages = await checkAllPagesInWindow(v, ctx, ["2026-10-26T04:30Z", "2026-11-02T00:00Z"]);
      if (only === "layout") res.layout = [await checkLayout(v, browser, 1440, 900), await checkLayout(v, browser, 1280, 800)];
      if (only === "slider") res.slider = await checkSlider(v, ctx);
      if (only === "view") res.page_view = await checkPageView(v, ctx, [CLOCKS[0], CLOCKS[3]]);
      console.log(key, JSON.stringify(res, (k, x) => (k === "labels" ? undefined : x)).slice(0, 1500));
      await browser.close();
      return res;
    }
    res.first_render = await firstRender(v, browser);
    res.clocks = [];
    for (const c of [...CLOCKS, PAGE_SWITCH_CLOCK, "2026-10-26T00:00Z", "2026-10-26T00:15Z", "2026-11-02T00:00Z"]) res.clocks.push(await checkClock(v, ctx, c));
    res.page_switch_14_17 = await checkPageSwitch(v, ctx);
    res.truncation = await checkTruncation(v, ctx, [CLOCKS[1], CLOCKS[3], CLOCKS[4]]);
    res.answer_key = await checkAnswerKey(v, ctx);
    res.keyboard = await checkKeyboard(v, ctx);
    res.window_all_pages = await checkAllPagesInWindow(v, ctx, ["2026-10-26T04:30Z", "2026-11-01T00:19Z", "2026-11-02T00:00Z"]);
    res.layout = [];
    for (const [w, h] of VIEWPORTS) res.layout.push(await checkLayout(v, browser, w, h));
    res.slider = await checkSlider(v, ctx);
    res.page_view = await checkPageView(v, ctx, [...CLOCKS, "2026-10-26T00:15Z", "2026-11-02T00:00Z"]);
    res.screenshots = await screenshots(v, browser, dir);
    res.play_through = await playThrough(v, ctx);
    res.summary = {
      no_future_data: res.clocks.every((c) => !c.problems.some((p) => /future|card from after|answer key|not yet streamed|page count|cards|clock shows|notes|verdict|detail shows/.test(p))),
      first_card_is_the_answer_in_the_badge: res.clocks.every((c) => !c.problems.some((p) => /first card/.test(p))),
      chart_utc_markers_no_gap: res.clocks.every((c) => !c.chart || c.chart.ok),
      page_switch_14_17: res.page_switch_14_17.ok,
      no_truncation: res.truncation.ok,
      answer_key_labelled: res.answer_key.ok,
      keyboard: res.keyboard.ok,
      chart_window_holds_every_page: res.window_all_pages.ok,
      layout_nothing_clipped: res.layout.every((l) => l.ok),
      slider_ticks_and_shading: res.slider.ok,
      status_chips_cause_type_legend: res.page_view.ok,
      play_through: res.play_through.ok,
      all_clock_checks: res.clocks.every((c) => c.ok),
    };
    writeFileSync(join(dir, "results.json"), JSON.stringify(res, null, 1));
    console.log(key, JSON.stringify(res.summary), `first render ${res.first_render.median_ms} ms`, `play-through ${res.play_through.wall_ms} ms`);
    for (const c of res.clocks) if (!c.ok) console.log("  ", c.clock, c.problems.join(" | "));
    for (const l of res.layout) if (!l.ok) console.log("   layout", l.viewport, l.problems.join(" | "));
    for (const k of ["page_switch_14_17", "truncation", "answer_key", "keyboard", "window_all_pages", "slider", "page_view"]) if (!res[k].ok) console.log("  ", k, res[k].problems.join(" | "));
    await browser.close();
    return res;
  } finally {
    server.kill();
  }
}

await run();
