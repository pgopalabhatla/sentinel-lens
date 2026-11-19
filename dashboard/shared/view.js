// View choices for the selected page, as pure functions of one page from uiState(demo, t) and the clock.
// They read only that page's alerts, which uiState already limits to those emitted by t, so nothing here
// can see the future. Tested in tests/test_dashboard_view.py (through Node); used by app.js and by the
// browser checks (dashboard/e2e/check.mjs).

const H = 3600e3;
export const BEFORE_MS = 3 * H, AFTER_MS = 2 * H;      // chart window around the page
export const FIRING_MS = 10 * 60e3;                    // "still firing": an alert emitted in the last 10 min

// The chart window is anchored on the page, not the clock: paged_at - 3 h to paged_at + 2 h, capped at
// the clock. `nowInside`: the clock is inside that span (so the window ends at the clock, drawn as "now").
export function chartWindow(page, t) {
  const from = page.paged_at - BEFORE_MS;
  const to = Math.min(page.paged_at + AFTER_MS, t);
  return { from, to, nowInside: t <= page.paged_at + AFTER_MS };
}

// The page's series by alert strength: the highest alert score (how far past its alert line, 1.0 for the
// rules that are always bad), then the number of alerts, then how far the strongest one went past its line
// (measure / line; infinite when the engine printed z=+inf), then the paging series, then the name.
export function rankSeries(page, t) {
  const by = new Map();
  for (const a of page.alerts) {
    const r = by.get(a.target) || { sid: a.target, score: -Infinity, count: 0, excess: -Infinity, last: -Infinity };
    r.score = Math.max(r.score, a.score ?? 1);
    r.count += 1;
    const excess = a.measure == null ? Infinity : a.line ? Math.abs(a.measure / a.line) : Math.abs(a.measure);
    r.excess = Math.max(r.excess, excess);
    r.last = Math.max(r.last, a.emitted_at);
    by.set(a.target, r);
  }
  const ranked = [...by.values()].map((r) => ({ ...r, firing: r.last >= t - FIRING_MS }));
  ranked.sort((a, b) => b.score - a.score || b.count - a.count || b.excess - a.excess
    || (b.sid === page.paging_target) - (a.sid === page.paging_target) || (a.sid < b.sid ? -1 : 1));
  return ranked;
}

// "still firing" when the page's newest alert was emitted within FIRING_MS of the clock; otherwise quiet
// since that alert
export function pageStatus(page, t) {
  const lastAlertMs = page.alerts.length ? Math.max(...page.alerts.map((a) => a.emitted_at)) : page.paged_at;
  return { firing: lastAlertMs >= t - FIRING_MS, lastAlertMs };
}

// the triage note's cause heading: an unknown category says so plainly
export function causeLabel(category) {
  if (!category || category === "unknown") return { label: "Likely cause (unconfirmed)", tag: null };
  return { label: "Cause", tag: category };
}
