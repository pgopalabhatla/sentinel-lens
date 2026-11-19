// Variant C: React + Vite + Recharts. What may be shown at the clock comes only from
// ../../shared/visibility.js (the tested port of dashboard/data.py); components only draw it.
import { useEffect, useMemo, useRef, useState } from "react";
import { uiState, attackerView, nextPageOut } from "../../shared/visibility.js";
import { hhmm, hhmmss, day, isoMinute, verdictText, verdictClass, speedLabel, readUrl } from "../../shared/format.js";
import SeriesChart from "./SeriesChart.jsx";

export default function App({ demo, series }) {
  const S = demo.settings, START = demo.meta.start_ms, END = demo.meta.end_ms;
  const init = useMemo(() => readUrl(location.search, START, END, S.default_speed_min), []);
  const [t, setTRaw] = useState(init.t);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(init.speed);
  const [pageSel, setPageSel] = useState(init.page);
  const [seriesSel, setSeriesSel] = useState(init.series);
  const [reveal, setReveal] = useState(init.reveal);
  const setT = (x) => setTRaw(Math.min(END, Math.max(START, x)));

  const ui = useMemo(() => uiState(demo, t), [demo, t]);
  const page = (pageSel != null && ui.pages.find((p) => p.page === pageSel)) || ui.pages[0] || null;

  // play: one tick every tick_ms, `speed` simulated minutes per tick, stop at the end
  useEffect(() => {
    if (!playing) return;
    const id = setInterval(() => setTRaw((x) => {
      const n = Math.min(END, x + speed * 60e3);
      if (n >= END) setPlaying(false);
      return n;
    }), S.tick_ms);
    return () => clearInterval(id);
  }, [playing, speed]);

  useEffect(() => {
    if (playing) return;
    const u = new URLSearchParams({ t: isoMinute(t) });
    if (pageSel != null) u.set("page", pageSel);
    if (seriesSel) u.set("series", seriesSel);
    if (reveal) u.set("reveal", "1");
    history.replaceState(null, "", "?" + u);
  }, [t, playing, pageSel, seriesSel, reveal]);

  // a card that was not there on the previous render slides in (not on the first render)
  const prevPages = useRef(null);
  const fresh = useMemo(() => {
    const prev = prevPages.current;
    return new Set(prev ? ui.pages.filter((p) => !prev.has(p.page)).map((p) => p.page) : []);
  }, [ui]);
  useEffect(() => { prevPages.current = new Set(ui.pages.map((p) => p.page)); }, [ui]);

  useEffect(() => { document.body.dataset.ready = "1"; }, []);

  const next = () => {
    const n = nextPageOut(demo, t);
    setPlaying(false);
    if (n != null) { setPageSel(null); setT(n); }
  };

  return (
    <div className="sl-root">
      <header className="sl-strip">
        <span className="sl-brand">Sentinel Lens</span>
        <span className="sl-clock" data-testid="clock" data-ms={t}>{day(t)} {hhmm(t)}<small>UTC</small></span>
        <div className="sl-controls">
          <button className={"sl-btn" + (playing ? " is-on" : "")} data-testid="play" onClick={() => setPlaying(!playing)}>
            {playing ? "Pause" : "Play"}
          </button>
          <button className="sl-btn" data-testid="next-page" onClick={next}>Next page</button>
          <select className="sl-select" data-testid="speed" aria-label="Speed" value={speed} onChange={(e) => setSpeed(Number(e.target.value))}>
            {S.speeds_min.map((m) => <option key={m} value={m}>{speedLabel(m)}</option>)}
          </select>
        </div>
        <label className="sl-toggle">
          <input type="checkbox" data-testid="reveal" checked={reveal} onChange={(e) => setReveal(e.target.checked)} /> Reveal answer key
        </label>
        <div className="sl-count"><b data-testid="page-count">{ui.page_count}</b><span>page-outs so far</span></div>
      </header>
      <div className="sl-scrubrow">
        <input className="sl-scrub" type="range" aria-label="Simulated time" min={START} max={END} step={demo.meta.bucket_ms}
               value={t} onChange={(e) => { setPlaying(false); setT(Number(e.target.value)); }} />
      </div>
      <main className="sl-main">
        <section className="sl-timeline" data-testid="timeline">
          <p className="sl-eyebrow">Pages, newest first</p>
          {ui.pages.length ? ui.pages.map((p) => (
            <button key={p.page} data-testid="page-card" data-page={p.page} data-paged-at-ms={p.paged_at}
                    className={"sl-card" + (fresh.has(p.page) ? " is-new" : "") + (page && p.page === page.page ? " is-selected" : "")}
                    onClick={() => { setPageSel(p.page); setSeriesSel(null); }}>
              <span className="sl-card-time">{day(p.paged_at)} {hhmm(p.paged_at)} UTC · page {p.page}</span>
              <span className="sl-card-reason" data-testid="card-reason">{p.reason}</span>
              <span className={"sl-pill " + verdictClass(p.verdict)}>{verdictText(p.verdict)}</span>
              <span className="sl-card-meta">{p.alerts.length} alerts{p.merged_into ? ` · merged into page ${p.merged_into}` : ""}</span>
            </button>
          )) : <p className="sl-empty">No page-out yet.</p>}
        </section>
        <section className="sl-detail">
          {/* keyed by page: switching pages mounts a fresh subtree, nothing carries over */}
          {page ? <PageDetail key={page.page} page={page} notes={ui.notes[String(page.page)]} />
                : <><p className="sl-eyebrow">Selected page</p><p className="sl-empty">No page-out yet. Press play or Next page.</p></>}
        </section>
      </main>
      <SeriesChart demo={demo} series={series} ui={ui} page={page} t={t} seriesSel={seriesSel}
                   onSeries={setSeriesSel} reveal={reveal} />
      {reveal && <AnswerKey demo={demo} t={t} />}
    </div>
  );
}

function PageDetail({ page, notes }) {
  const written = notes.filter((r) => r.type !== "link");
  const last = written[written.length - 1];
  const from = last ? (last.type === "follow_up" ? "from the follow-up" : "from the first note") : "no note written yet";
  return (
    <div data-testid="page-detail" data-page={page.page}>
      <p className="sl-eyebrow">
        Page {page.page} · paged {day(page.paged_at)} {hhmm(page.paged_at)} UTC · {page.alerts.length} alerts now
        {page.merged_into ? ` · merged into page ${page.merged_into}` : ""}
      </p>
      <h1 className="sl-headline" data-testid="reason-headline">{page.reason}</h1>
      <div className={"sl-badge " + verdictClass(page.verdict)} data-testid="verdict">{verdictText(page.verdict)}</div>
      <span className="sl-advisory" style={{ marginLeft: 10 }}>{from}</span>
      {notes.length ? notes.map((r) => r.type === "link"
        ? <p key={r.link_id || r.ready_at} className="sl-link">{hhmm(r.ready_at)} UTC: page {(r.absorbed || []).join(", ")} merged into this page after it paged.</p>
        : <NoteCard key={r.type + r.ready_at} r={r} title={r.type === "follow_up" ? "Follow-up" : "Triage note"} />)
        : <p className="sl-empty">The triage agent has not written a note yet.</p>}
    </div>
  );
}

function NoteCard({ r, title }) {
  const ev = r.evidence || [];
  return (
    <article className="sl-note" data-testid="note" data-type={r.type}>
      <h3>{title}<span className="sl-when">written {day(r.ready_at)} {hhmmss(r.ready_at)} UTC</span></h3>
      <p>
        <span className={"sl-pill " + verdictClass(r.needs_human)}>{verdictText(r.needs_human)}</span>
        <span className="sl-advisory">confidence {r.confidence} · advisory only</span>
      </p>
      {r.guard && <p className="sl-advisory">Guard: converted from "{r.guard.converted_from}": {r.guard.reason}</p>}
      <div className="sl-field" data-testid="cause"><b>Cause · {r.cause?.category || "-"}</b><p>{r.cause?.description || "-"}</p></div>
      <div className="sl-field" data-testid="impact"><b>Impact · {(r.impact?.degraded || []).join(", ") || "none"}</b><p>{r.impact?.description || "-"}</p></div>
      <details className="sl-evidence" data-testid="evidence">
        <summary>Evidence ({ev.length})</summary>
        <ul>{ev.map((e, i) => <li key={i}><code>{e.tool}</code>{e.finding}</li>)}</ul>
      </details>
      <div className="sl-field" data-testid="next-step" style={{ marginTop: 10 }}><b>Next step</b><p>{r.next_step || "-"}</p></div>
    </article>
  );
}

function AnswerKey({ demo, t }) {
  const view = attackerView(demo, t), active = new Set(view.active.map((i) => i.id));
  return (
    <div className="sl-key" data-testid="answer-key">
      <h2>Answer key: hidden from the system</h2>
      <p className="sl-advisory">The attacker's injections for the whole week, including after the clock. Nothing here reaches the
        detector, the pages or the triage agent. Rows active at the clock are bright; on the chart, an injection that touched the
        shown series is shaded.</p>
      <table>
        <thead><tr><th>id</th><th>start</th><th>stop</th><th>kind</th><th>severity</th><th>intent</th></tr></thead>
        <tbody>{view.all.map((i) => (
          <tr key={i.id} className={active.has(i.id) ? "is-active" : ""}>
            <td>{i.id}{active.has(i.id) ? " (active)" : ""}</td><td>{day(i.start)} {hhmm(i.start)}</td><td>{hhmm(i.stop)}</td>
            <td>{i.kind === "named" ? i.scenario : "generic"}</td><td>{i.severity}</td><td>{i.intent}</td>
          </tr>))}
        </tbody>
      </table>
    </div>
  );
}
