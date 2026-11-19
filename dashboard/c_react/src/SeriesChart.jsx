// The selected page's series: 5-minute mean, expected, wobble band, hot line, this page's alerts, the
// page-out, changes, and (answer key on) injection windows. X axis: the last 12 h up to the clock, UTC.
import { Area, ComposedChart, Line, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from "recharts";
import { seriesPoints, attackerView } from "../../shared/visibility.js";
import { hhmm, day, fmtNum, hourTicks } from "../../shared/format.js";

const H = 3600e3;
const tick = { fill: "var(--text-dim)", fontFamily: "var(--font-mono)", fontSize: 11 };

export default function SeriesChart({ demo, series, ui, page, t, seriesSel, onSeries, reveal }) {
  const S = demo.settings;
  if (!page) {
    return (
      <section className="sl-chartwrap">
        <div className="sl-chart-head"><h2>Series of the selected page</h2></div>
        <div className="sl-chart" data-testid="chart" data-series="" />
      </section>
    );
  }
  const sid = page.series.includes(seriesSel) ? seriesSel : page.series.includes(page.paging_target) ? page.paging_target : page.series[0];
  const info = demo.series_index[sid], seasonal = info.rule === "seasonal", from = t - S.lookback_hours * H;
  const alerts = page.alerts.filter((a) => a.target === sid && a.time >= from);
  const down = alerts.length > 0 && alerts.every((a) => a.direction === "down");
  const rows = seriesPoints(demo, series, sid, t, S.lookback_hours).map(([ts, value, expected, wobble]) => {
    const ok = expected != null && wobble != null;
    return { ts, value, expected, band: ok ? [expected - wobble, expected + wobble] : null,
             hot: ok ? expected + (down ? -1 : 1) * S.hot_z * wobble : null };
  });
  const changes = ui.changes.filter((c) => c.ts_ms >= from);
  const areas = reveal ? attackerView(demo, t).all.filter((i) => i.stop > from && i.start < t && i.series.includes(sid)) : [];

  return (
    <section className="sl-chartwrap">
      <div className="sl-chart-head">
        <h2>{info.entity} {info.label}</h2>
        {page.series.length > 1 && <div className="sl-chips">{page.series.map((s) => (
          <button key={s} className={"sl-chip" + (s === sid ? " is-on" : "")} data-series={s} onClick={() => onSeries(s)}>
            {demo.series_index[s].entity} {demo.series_index[s].label}
          </button>))}</div>}
      </div>
      <div className="sl-legend">
        <span><i style={{ borderColor: "var(--chart-value)" }} />5-min mean</span>
        {seasonal && <span><i style={{ borderColor: "var(--chart-expected)", borderTopStyle: "dashed" }} />expected</span>}
        {seasonal && <span><i style={{ borderColor: "var(--chart-band)", borderTopWidth: 8 }} />wobble band (expected +/- 1 wobble)</span>}
        {seasonal && <span><i style={{ borderColor: "var(--chart-hot)", borderTopStyle: "dotted" }} />hot line (expected {down ? "-" : "+"} {S.hot_z} x wobble)</span>}
        <span><i style={{ borderColor: "var(--chart-hot)", borderTopStyle: "dashed" }} />alert</span>
        <span><i style={{ borderColor: "var(--chart-hot)", borderTopWidth: 3 }} />page fired</span>
        <span><i style={{ borderColor: "var(--chart-change)" }} />change</span>
        {reveal && <span><i style={{ borderColor: "var(--key)", borderTopWidth: 8 }} />injection (answer key)</span>}
      </div>
      <div className="sl-chart" data-testid="chart" data-series={sid}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={rows} margin={{ top: 22, right: 28, bottom: 4, left: 8 }}>
            <CartesianGrid stroke="var(--line)" strokeOpacity={0.5} />
            <XAxis dataKey="ts" type="number" scale="time" domain={[from, t]} allowDataOverflow ticks={hourTicks(from, t, 1)}
                   tickFormatter={hhmm} tick={tick} stroke="var(--line-strong)" />
            <YAxis tick={tick} tickFormatter={fmtNum} stroke="var(--line-strong)" width={64} domain={["auto", "auto"]} />
            <Tooltip isAnimationActive={false} labelFormatter={(x) => `${day(x)} ${hhmm(x)} UTC`} formatter={(v) => (Array.isArray(v) ? v.map(fmtNum).join(" to ") : fmtNum(v))}
                     contentStyle={{ background: "var(--bg-raise)", border: "1px solid var(--line-strong)", fontFamily: "var(--font-mono)", fontSize: 12 }} />
            {areas.map((i) => (
              <ReferenceArea key={i.id} x1={Math.max(i.start, from)} x2={Math.min(i.stop, t)} className="sl-m-inject" ifOverflow="hidden"
                             fill="var(--key-soft)" stroke="var(--key)" strokeDasharray="4 3"
                             label={{ value: i.id, position: "insideTopLeft", fill: "var(--key)", fontSize: 11, fontFamily: "var(--font-mono)" }} />
            ))}
            {seasonal && <Area dataKey="band" name="wobble band" stroke="none" fill="var(--chart-band)" fillOpacity={1} isAnimationActive={false} connectNulls={false} />}
            {seasonal && <Line dataKey="expected" name="expected" dot={false} stroke="var(--chart-expected)" strokeDasharray="5 4" strokeWidth={1.5} isAnimationActive={false} />}
            {seasonal && <Line dataKey="hot" name="hot line" dot={false} stroke="var(--chart-hot)" strokeDasharray="2 3" strokeWidth={1.5} isAnimationActive={false} />}
            <Line dataKey="value" name="5-min mean" dot={false} stroke="var(--chart-value)" strokeWidth={1.8} isAnimationActive={false} />
            {alerts.map((a, i) => <ReferenceLine key={"a" + i} x={a.time} className="sl-m-alert" stroke="var(--chart-hot)" strokeDasharray="4 3" strokeOpacity={0.75} ifOverflow="hidden" />)}
            {page.paged_at >= from && <ReferenceLine x={page.paged_at} className="sl-m-page" stroke="var(--chart-hot)" strokeWidth={3} ifOverflow="hidden"
                     label={{ value: `page ${page.page}`, position: "insideTopRight", fill: "var(--amber)", fontSize: 11, fontFamily: "var(--font-mono)" }} />}
            {changes.map((c) => <ReferenceLine key={c.change_id} x={c.ts_ms} className="sl-m-change" stroke="var(--chart-change)" strokeWidth={1.5} ifOverflow="hidden"
                     label={{ value: c.change_id, position: "insideBottomLeft", fill: "var(--teal)", fontSize: 11, fontFamily: "var(--font-mono)" }} />)}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
