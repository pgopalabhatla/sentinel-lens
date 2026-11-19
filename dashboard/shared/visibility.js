// What the dashboard knows at simulated time t: a line-by-line port of dashboard/data.py
// (page_outs, notes_for, changes_until, ui_state, series_until, attacker_view) for the JavaScript
// variants. The rule is the Python one: nothing from after t reaches the system's panels.
// tests/test_dashboard_ui.py runs this file in Node and checks that uiState / seriesPoints /
// attackerView give exactly what the Python functions give at the same clocks.
//
// Input: demo.json and series.json from dashboard/export_ui.py. Times are epoch milliseconds (UTC).
// No dependencies, no DOM: an ES module usable in a browser and in Node.

export const toMs = (x) => (typeof x === "number" ? x : Date.parse(x));

// a page-out exists from its 'paged' record's emitted_at; an alert from its own emitted_at
export function pageOuts(demo, t) {
  const held = new Map(), paged = new Map(), alias = new Map();
  for (const r of demo.page_records) {
    const at = toMs(r.emitted_at);
    if (at > t) break;                                   // records are in emission order
    const key = r.page;
    if (r.kind === "paged") {
      paged.set(key, { page: key, page_id: r.page_id, paged_at: at, paged_with: (held.get(key) || []).length });
      continue;
    }
    if (!held.has(key)) held.set(key, []);
    held.get(key).push({ ...r.alert, emitted_at: at });
    for (const absorbed of r.absorbed || []) {
      const moved = held.get(absorbed) || [];
      held.delete(absorbed);
      held.set(key, held.get(key).concat(moved));
      alias.set(absorbed, key);
    }
  }
  const out = [];
  for (const [key, p] of paged) {
    let home = key;
    while (alias.has(home)) home = alias.get(home);
    const alerts = [...(held.get(home) || [])].sort((a, b) => a.emitted_at - b.emitted_at);
    out.push({ ...p, merged_into: home !== key ? home : null, alerts });
  }
  return out;
}

// a note or follow-up exists once written (as_of + seconds, to the ms); a link record at its time
export function readyAt(r) {
  if (r.type === "note" || r.type === "follow_up") return toMs(r.as_of) + Math.round((r.seconds || 0) * 1000);
  return toMs(r.at);
}

export function notesFor(demo, page, t) {
  return demo.triage
    .filter((r) => r.page === page)
    .map((r) => ({ ...r, ready_at: readyAt(r) }))
    .filter((r) => r.ready_at <= t)
    .sort((a, b) => a.ready_at - b.ready_at);
}

// a change exists for the system once its stream record arrived
export function changesUntil(demo, t) {
  return demo.changes.filter((c) => toMs(c.streamed_at) <= t);
}

export function uiState(demo, t) {
  const pages = [], notes = {};
  const outs = pageOuts(demo, t).sort((a, b) => b.paged_at - a.paged_at || b.page - a.page);
  for (const p of outs) {
    const recs = notesFor(demo, p.page, t);
    const written = recs.filter((r) => r.type === "note" || r.type === "follow_up");
    const alerts = p.alerts.map((a) => ({ ...a, time: toMs(a.time), emitted_at: a.emitted_at }));
    const last = written[written.length - 1];
    pages.push({
      page: p.page, page_id: p.page_id, paged_at: p.paged_at, paged_with: p.paged_with,
      merged_into: p.merged_into, reason: demo.paging_reasons[String(p.page)],
      paging_target: demo.paging_targets[String(p.page)],
      series: [...new Set(alerts.map((a) => a.target))], alerts,
      verdict: last ? last.needs_human : null, verdict_at: last ? last.ready_at : null,
    });
    notes[String(p.page)] = recs;
  }
  return {
    clock: t, page_count: pages.length, pages, notes,
    changes: changesUntil(demo, t).map((c) => ({ ...c, ts_ms: toMs(c.ts), streamed_at_ms: toMs(c.streamed_at) })),
  };
}

// 5-minute readings whose bucket has CLOSED by t (ts + bucket <= t), over the last lookback hours,
// as [ts_ms, value, expected, wobble]
export function seriesPoints(demo, series, seriesId, t, lookbackHours = 12) {
  const s = series[seriesId];
  if (!s) return [];
  const bucket = demo.meta.bucket_ms, from = t - lookbackHours * 3600e3, out = [];
  for (let i = 0; i < s.value.length; i++) {
    const ts = s.t0_ms + i * s.step_ms;
    if (ts + bucket <= t && ts >= from) out.push([ts, s.value[i], s.expected[i], s.wobble[i]]);
  }
  return out;
}

// ANSWER KEY, hidden from the system: only for the "Reveal answer key" view
export function injections(demo) {
  const out = new Map();
  for (const r of demo.answer_key) {
    if (r.event === "started") out.set(r.id, { ...r, start: toMs(r.start), stop: toMs(r.stop) });
    else if (r.event === "stopped" && out.has(r.id)) out.get(r.id).stop = toMs(r.stop);
  }
  return [...out.values()].sort((a, b) => a.start - b.start);
}

export function attackerView(demo, t) {
  const row = (i) => ({
    id: i.id, scenario: i.scenario, kind: i.scenario === "custom" ? "generic" : "named",
    severity: i.severity, intent: i.intent, entities: i.entities, series: i.series || [],
    start: i.start, stop: i.stop,
  });
  const all = injections(demo);
  return { active: all.filter((i) => i.start <= t && t < i.stop).map(row), all: all.map(row) };
}

// the page-out after t (for "next page"), or null
export function nextPageOut(demo, t) {
  const r = demo.page_records.find((r) => r.kind === "paged" && toMs(r.emitted_at) > t);
  return r ? toMs(r.emitted_at) : null;
}
