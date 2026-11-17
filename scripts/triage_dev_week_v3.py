"""Arm D: note schema v3 with follow-ups, on live-week-1 stream v2 (DEVELOPMENT SET), next to arm C.

    python scripts/triage_dev_week_v3.py            # triage every page-out not yet noted (and its follow-up), write the report
    python scripts/triage_dev_week_v3.py --report   # only write the report from the notes already on the topics

Arm C (scripts/triage_dev_week_v2.py, topic `live-week-1-v2-triage`): verdict real | noise | unsure on
stream v2. Arm D: the same stream and page-outs (`live-week-1-v2-*`, already replayed), the agent with
note schema v3 (needs_human, cause, impact; an explanation does not make a page harmless) and one
follow-up of an "unsure" note at paging time + follow_up_minutes. Notes on `live-week-1-v2-triage-v3`.
C's verdicts are scored with the same rule after mapping real -> yes, noise -> no, unsure -> unsure.

Development set: the week, its schedule and the mistakes of arms A-C had all been read before v3 was
written. The answer key is read only after the investigations.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

V2 = "live-week-1-v2-"
TRIAGE = {"C": V2 + "triage", "D": V2 + "triage-v3"}
DATA = ROOT / "data_live"
RESULT = ROOT / "docs/results/triage-dev-week-1-v3.md"


def main(argv=None) -> int:
    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, triage_score
    from sentinel_stream.dispatcher import Dispatcher, KafkaTopics, Topics
    from sentinel_stream.investigator import Config, Investigator
    from sentinel_stream.scenarios_live import SCENARIOS

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--report", action="store_true", help="do not investigate; report on the notes already written")
    args = ap.parse_args(argv)

    bootstrap = broker.DEFAULT_BOOTSTRAP
    if not broker.reachable(bootstrap):
        raise SystemExit(f"no Kafka broker at {bootstrap} (start one: docker compose up -d)")
    ds = DataSource(DATA)
    config = Config.load()
    topics = Topics(V2 + "telemetry", V2 + "alerts", V2 + "pages", TRIAGE["D"])
    ran = None
    if not args.report:
        agent = Investigator(config, run_name="triage-dev-week-1-v3")
        began = time.monotonic()
        d = Dispatcher(KafkaTopics(bootstrap, topics), ds, agent.investigate, topics, concurrency=3, stream_version=2,
                       follow_up_minutes=config.follow_up_minutes)
        d.log(f"{len(d.noted)} notes, {len(d.followed)} follow-ups already on {topics.triage!r}; {config.model}")
        d.run(until_end=True)
        ran = (d.investigated, d.followed_up, time.monotonic() - began)
        d.log(f"{d.investigated} investigated, {d.followed_up} followed up, {d.failed} failed, {ran[2]:.0f} s")

    # ---- evaluation: only from here on is the answer key read
    topo = build_topology(ds.relationships(), ds.series())
    summaries = {name: s.summary for name, s in SCENARIOS.items()}
    key, pages = broker.read_topic(bootstrap, V2 + "answer_key"), broker.read_topic(bootstrap, V2 + "pages")
    scored, raw = {}, {}
    for arm, topic in TRIAGE.items():
        records = broker.read_topic(bootstrap, topic)
        raw[arm] = records
        scored[arm] = triage_score.score_needs_human(key, pages, [r for r in records if r.get("type") == "note"],
                                                     [r for r in records if r.get("type") == "follow_up"], topo, summaries)
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    RESULT.write_text(report(commit, config, scored, raw, ran))
    print(f"saved {RESULT.relative_to(ROOT)}")
    return 0


def report(commit, config, s, raw, ran) -> str:
    from sentinel_stream import triage_score

    C, D = s["C"], s["D"]
    b = config.budget
    L = ["# Triage on live-week-1, stream v2: note schema v3 with follow-ups (DEVELOPMENT SET)", "",
         "**A development-set result, not a final or held-out one.** The week, its schedule and the mistakes of the earlier "
         "arms (docs/results/triage-dev-week-1.md, triage-dev-week-1-v2.md) were read before schema v3 and the follow-up "
         "were written. It shows whether the changes do what they were meant to on the cases that motivated them.", "",
         f"- written by `scripts/triage_dev_week_v3.py` at commit `{commit}`",
         "- page-outs: the 29 of live-week-1 replayed on stream v2 (`live-week-1-v2-*`), the same in both arms",
         f"- agent: `{config.model}`, effort `{config.effort}`; budget per investigation {b.max_tool_calls} tool calls, "
         f"{b.max_seconds:g} s, {b.max_tokens:,} tokens; 3 at a time; follow-up of an \"unsure\" note at paging time + "
         f"{config.follow_up_minutes:g} min, a fresh agent whose tools see nothing after that time",
         *([f"- this invocation: {ran[0]} first investigations and {ran[1]} follow-ups in {ran[2]:.0f} s of wall time"] if ran else []),
         "", "| arm | note | follow-up | topic |", "|---|---|---|---|",
         f"| C | verdict real / noise / unsure (scored as yes / no / unsure) | none | `{TRIAGE['C']}` |",
         f"| D | needs_human yes / no / unsure, cause, impact; an explanation does not make a page harmless | once, if unsure | `{TRIAGE['D']}` |",
         "", "Scoring: a true page answered yes = correct, no = **dangerous**, unsure = deferred; a false page answered no = "
         "correct, yes = wasted, unsure = deferred. The final answer is the follow-up's when there was one.", "",
         "## C against D", "",
         "| | C | D, first answer | D, final answer |", "|---|---|---|---|"]

    def cnt(t, truth, o):
        return t[truth][o]
    for truth, n in (("real", C["real_pages"]), ("noise", C["noise_pages"])):
        for o in (("correct", "dangerous", "deferred") if truth == "real" else ("correct", "wasted", "deferred")):
            L.append(f"| {truth} page-outs ({n}): {o} | {cnt(C['final'], truth, o)} | {cnt(D['first'], truth, o)} | "
                     f"{cnt(D['final'], truth, o)} |")
    fmt = lambda x: "-" if x is None else f"{x:.1f}"
    pair = lambda x: x or (None, None)
    usd = lambda x: "-" if x is None else f"{x:.2f}"
    L += [f"| cause names the injected target (of {C['real_pages']}) | {C['cause_names_target']} | {D['first_cause_names_target']} | "
          f"{D['cause_names_target']} |",
          f"| follow-ups | - | - | {D['follow_ups']} |",
          f"| minutes from paging to the final answer: median / max | {fmt(pair(C['minutes_to_final'])[0])} / "
          f"{fmt(pair(C['minutes_to_final'])[1])} | - | {fmt(pair(D['minutes_to_final'])[0])} / {fmt(pair(D['minutes_to_final'])[1])} |",
          f"| ... for the pages with a follow-up: median / max | - | - | "
          f"{fmt(pair(D['minutes_to_final_followed'])[0])} / {fmt(pair(D['minutes_to_final_followed'])[1])} |",
          f"| cost (USD, estimate; D with its follow-ups) | {usd(C['cost_total'])} | - | {usd(D['cost_total'])} |", ""]

    # ---- dangerous, side by side
    L += ["## Dangerous answers", ""]
    for arm in ("C", "D"):
        rows = s[arm]["dangerous"]
        L.append(f"Arm {arm}: {len(rows)}" + (":" if rows else "."))
        if rows:
            L += ["", "| time (UTC) | page | injection | scenario | cause |", "|---|---|---|---|---|"]
            L += [f"| {r['time']:%b %d %H:%M} | {r['page']} | {', '.join(r['injections'])} | {r['scenario']} | "
                  f"{triage_score._cell(r['cause'])} |" for r in rows]
        L.append("")

    # ---- per page
    c = {r["page_id"]: r for r in C["rows"]}
    L += ["## Per page-out: C, then D's first answer and its follow-up", "",
          "| time (UTC) | page | truth | injection | scenario | C | D first | D follow-up | D final | D outcome | D cause | D impact | D min to final |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in D["rows"]:
        L.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['truth']} | {', '.join(r['injections']) or '-'} | {r['scenario'] or '-'} | "
                 f"{c[r['page_id']]['final'] or '-'} | {r['first'] or '-'} | {r['follow_up'] or '-'} | {r['final'] or '-'} | "
                 f"{r['final_outcome']} | {r['cause_category'] or '-'} | {', '.join(r['impact'] or []) or '-'} | "
                 f"{fmt(r['minutes_to_final_answer'])} |")
    changed = [r for r in D["rows"] if r["follow_up"] is not None and r["follow_up"] != r["first"]]
    L += ["", f"Follow-ups that changed the answer: {len(changed)} of {D['follow_ups']}.", ""]

    cats = Counter(r["cause_category"] for r in D["rows"])
    L += ["Arm D's cause categories (final notes): " + ", ".join(f"{k} {v}" for k, v in cats.most_common()) + ".", ""]

    # ---- D's tables in full, and the causes next to what was injected
    L += ["# Arm D in full", "", triage_score.markdown_needs_human(D), "",
          "## D's cause next to what was injected (to read, not scored)", ""]
    for r in D["rows"]:
        what = (f"**{', '.join(r['injections'])}: {r['scenario']}** on `{r['target']}`. {r['summary'] or ''}"
                if r["truth"] == "real" else "**nothing injected** (a page-out of the quiet world)")
        L += [f"### {r['time']:%a %b %d %H:%M} - page {r['page']} - {r['final']} ({r['final_outcome']})", "",
              f"- Injected: {what}", f"- Cause ({r['cause_category']}): {r['cause'] or '-'}",
              f"- Impact: {', '.join(r['impact'] or []) or '-'}"
              + (f"; answered at paging time: {r['first']}, after the follow-up: {r['follow_up']}" if r["follow_up"] else ""), ""]
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
