// node dashboard/shared/dump_state.mjs <ui folder> <clock ISO>...
// Prints, for each clock, what visibility.js says the panels may show: uiState, the points of every
// series of every page, and the attacker view. Used by tests/test_dashboard_ui.py to compare the JS
// port with dashboard/data.py.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { uiState, seriesPoints, attackerView, toMs } from "./visibility.js";

const [folder, ...clocks] = process.argv.slice(2);
const demo = JSON.parse(readFileSync(join(folder, "demo.json"), "utf8"));
const series = JSON.parse(readFileSync(join(folder, "series.json"), "utf8"));
const lookback = demo.settings.lookback_hours;
const out = {};
for (const c of clocks) {
  const t = toMs(c);
  const ui = uiState(demo, t);
  const pts = {};
  for (const p of ui.pages) for (const sid of p.series) pts[sid] = seriesPoints(demo, series, sid, t, lookback);
  out[c] = { ui, series: pts, attacker: attackerView(demo, t) };
}
process.stdout.write(JSON.stringify(out));
