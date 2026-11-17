"""Triage agents on live-week-1's schedule replayed on stream v2: a DEVELOPMENT-SET evaluation.

    python scripts/triage_dev_week_v2.py            # replay (once), triage every page-out not yet noted, write the report
    python scripts/triage_dev_week_v2.py --report   # only write the report from what is on the topics

1. Replay. The same 14 injections as live-week-1 (the schedule generated with the same seed, checked
   against the one written in docs/live-protocol.md) are played through Kafka on stream version 2,
   which also carries logs, Kubernetes events and changes, with what each failure writes there
   (sentinel_stream/evidence_live.py). Topics `live-week-1-v2-*`. Done only if those topics do not
   exist yet. live-week-1 itself (stream v1, docs/results/live-week-1.md) is not touched.
2. Triage (arm C): the dispatcher on the v2 topics, the agent as it is now (two verdict rules, the
   baseline_comparison tool), logs / events / changes read from the stream. Notes on `live-week-1-v2-triage`.
3. Triage (arm B, an ablation): the same agent on the ORIGINAL v1 topics, notes on
   `live-week-1-triage-agent-v2`. Arm A is the first run (`live-week-1-triage`, docs/results/triage-dev-week-1.md):
   the agent as it was, stream v1. B against A isolates the prompt and tool change; C against B the
   evidence on the stream.
4. Only then is the answer key read, to score.

Why "development set": the week, its schedule and the first run's mistakes had all been read before the
stream-v2 evidence, the two rules and the tool were written. A held-out number needs a further world.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

V1, V2 = "live-week-1-", "live-week-1-v2-"
ABLATION_TRIAGE = "live-week-1-triage-agent-v2"
DATA = ROOT / "data_live"
RESULT = ROOT / "docs/results/triage-dev-week-1-v2.md"
ARMS = {"A": "agent v1, stream v1 (the first run)", "B": "agent v2, stream v1 (ablation)", "C": "agent v2, stream v2"}


def replay_if_needed(bootstrap, log=print) -> bool:
    from confluent_kafka.admin import AdminClient

    import run_live_week as live
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    existing = [t for t in AdminClient(broker.client_config(bootstrap)).list_topics(timeout=10).topics if t.startswith(V2)]
    if existing:
        log(f"stream v2 replay already on the broker ({len(existing)} topics); not replayed")
        return False
    commands = live.schedule(DataSource(DATA))
    if live.schedule_markdown(commands) not in live.PROTOCOL.read_text():
        raise SystemExit("the schedule generated now is not the one in docs/live-protocol.md")
    began = time.monotonic()
    out = live.run(DATA, live.START, live.END, commands, V2, stream_version=2, log=log)
    log(f"replayed on stream v2 in {time.monotonic() - began:.0f} s: {out['telemetry_messages']} telemetry messages, "
        f"{out['alerts']} alerts, {len(out['pages'])} page events")
    return True


def triage(bootstrap, topics, run_name: str, stream_version: int, ds, config) -> tuple:
    from sentinel_stream.dispatcher import Dispatcher, KafkaTopics
    from sentinel_stream.investigator import Investigator

    agent = Investigator(config, run_name=run_name)
    began = time.monotonic()
    d = Dispatcher(KafkaTopics(bootstrap, topics), ds, agent.investigate, topics, concurrency=3, stream_version=stream_version)
    d.log(f"{len(d.noted)} notes already on {topics.triage!r}; investigating the rest with {config.model} (stream v{stream_version})")
    d.run(until_end=True)
    wall = time.monotonic() - began
    d.log(f"{d.investigated} investigated, {d.failed} failed, {wall:.0f} s")
    return d.investigated, wall


def main(argv=None) -> int:
    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, live_score, triage_score
    from sentinel_stream.dispatcher import Topics
    from sentinel_stream.investigator import Config
    from sentinel_stream.scenarios_live import SCENARIOS

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--report", action="store_true", help="do not replay or investigate; report on what is on the topics")
    ap.add_argument("--no-ablation", action="store_true", help="skip arm B (the new agent on the v1 stream)")
    args = ap.parse_args(argv)

    bootstrap = broker.DEFAULT_BOOTSTRAP
    if not broker.reachable(bootstrap):
        raise SystemExit(f"no Kafka broker at {bootstrap} (start one: docker compose up -d)")
    ds = DataSource(DATA)
    config = Config.load()
    topics = {"A": Topics(V1 + "telemetry", V1 + "alerts", V1 + "pages", V1 + "triage"),
              "B": Topics(V1 + "telemetry", V1 + "alerts", V1 + "pages", ABLATION_TRIAGE),
              "C": Topics(V2 + "telemetry", V2 + "alerts", V2 + "pages", V2 + "triage")}
    ran = {}
    if not args.report:
        replay_if_needed(bootstrap)
        ran["C"] = triage(bootstrap, topics["C"], "triage-dev-week-1-v2", 2, ds, config)
        if not args.no_ablation:
            ran["B"] = triage(bootstrap, topics["B"], "triage-dev-week-1-agent-v2-stream-v1", 1, ds, config)

    # ---- evaluation: only from here on is the answer key read
    topo = build_topology(ds.relationships(), ds.series())
    summaries = {name: s.summary for name, s in SCENARIOS.items()}
    keys = {"v1": broker.read_topic(bootstrap, V1 + "answer_key"), "v2": broker.read_topic(bootstrap, V2 + "answer_key")}
    pages = {"v1": broker.read_topic(bootstrap, V1 + "pages"), "v2": broker.read_topic(bootstrap, V2 + "pages")}
    detection = {v: live_score.score(keys[v], pages[v], topo) for v in ("v1", "v2")}
    results, notes = {}, {}
    for arm, stream in (("A", "v1"), ("B", "v1"), ("C", "v2")):
        triage_records = broker.read_topic(bootstrap, topics[arm].triage)
        notes[arm] = [r for r in triage_records if r.get("type") == "note"]
        if notes[arm]:
            results[arm] = triage_score.score(keys[stream], pages[stream], notes[arm], topo, summaries)
    evidence_of = {r["id"]: r.get("evidence") for r in keys["v2"] if r["event"] == "started"}
    telemetry_v2 = broker.read_topic(bootstrap, V2 + "telemetry")
    same = {"pages": pages["v1"] == pages["v2"],
            "alerts": broker.read_topic(bootstrap, V1 + "alerts") == broker.read_topic(bootstrap, V2 + "alerts")}
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    RESULT.write_text(report(commit, config, detection, results, notes, evidence_of, telemetry_v2, ran, topics, same))
    print(f"saved {RESULT.relative_to(ROOT)}")
    return 0


# ------------------------------------------------------------------------------------------ report
def _verdicts(result) -> dict:
    return {r["page_id"]: r for r in result["rows"]} if result else {}


def evidence_label(kinds) -> str:
    if not kinds:
        return "-"
    return "metrics only" if list(kinds) == ["metrics"] else "metrics + " + ", ".join(k for k in kinds if k != "metrics")


def report(commit, config, detection, results, notes, evidence_of, telemetry_v2, ran, topics, same) -> str:
    from collections import Counter

    from sentinel_stream import live_score, triage_score, wire

    b = config.budget
    C = results.get("C")
    L = ["# Triage agents on live-week-1, stream v2 (DEVELOPMENT SET)", "",
         "**This is a development-set result, not a held-out one, and not a final result.** The week, its schedule and the "
         "first triage run's mistakes (docs/results/triage-dev-week-1.md) were read before the stream-v2 evidence, the two "
         "verdict rules and the `baseline_comparison` tool were written. It shows whether the changes do what they were "
         "meant to on the cases they were written for; a held-out number needs a further world with its own protocol.", "",
         f"- written by `scripts/triage_dev_week_v2.py` at commit `{commit}`",
         f"- the same 14 injections as live-week-1 (schedule seed 1, checked against docs/live-protocol.md), replayed on "
         f"stream v2: topics `{V2}*`. live-week-1 itself (stream v1) and its result file are unchanged",
         f"- agent: `{config.model}`, effort `{config.effort}`; budget per page {b.max_tool_calls} tool calls, {b.max_seconds:g} s, "
         f"{b.max_tokens:,} tokens; one fresh agent per page-out, 3 at a time; notes are advisory",
         *(f"- this invocation: arm {arm} investigated {n} page-outs in {w:.0f} s of wall time" for arm, (n, w) in ran.items()),
         "", "Three arms:", "",
         "| arm | agent | stream | notes topic |", "|---|---|---|---|",
         f"| A | as in the first run (no rules, 8 tools) | v1: metrics and runs only; logs / events / changes from `data_live` | `{topics['A'].triage}` |",
         f"| B | two verdict rules + `baseline_comparison` | v1 (same page-outs as A) | `{topics['B'].triage}` |",
         f"| C | two verdict rules + `baseline_comparison` | v2: logs / events / changes on the stream, with the failures' own | `{topics['C'].triage}` |",
         "", "A to B changes the agent only; B to C changes the evidence only.", ""]

    # ---- detection
    d1, d2 = detection["v1"], detection["v2"]
    L += ["## Detection: stream v1 against stream v2", "",
          "| | stream v1 (live-week-1) | stream v2 |", "|---|---|---|"]
    for label, k, fmt in (("injections detected", "n_detected", "{}"), ("median time to detect (min)", "median_time_to_detect_min", "{:.0f}"),
                          ("max time to detect (min)", "max_time_to_detect_min", "{:.0f}"), ("page-outs", "page_outs", "{}"),
                          ("matching an injection", "matched_page_outs", "{}"), ("false page-outs", "false_page_outs", "{}"),
                          ("false pages per day", "false_pages_per_day", "{:.2f}")):
        L.append(f"| {label} | {fmt.format(d1[k])} | {fmt.format(d2[k])} |")
    rows1 = {r["id"]: r for r in d1["injections"]}
    differ = [r for r in d2["injections"] if (r["detected"], r["time_to_detect_min"], r["paged_by"], r["matching_page_outs"])
              != (rows1[r["id"]]["detected"], rows1[r["id"]]["time_to_detect_min"], rows1[r["id"]]["paged_by"], rows1[r["id"]]["matching_page_outs"])]
    L += ["", ("Per injection: identical (detected, time to detect, first paged by, matching page-outs)." if not differ else
               f"Per injection, {len(differ)} differ: " + ", ".join(r["id"] for r in differ) + "."),
          f"The `alerts` topics of the two runs are identical record for record: {same['alerts']}; the `pages` topics: {same['pages']}.",
          "The engine does not read the new message types, and the readings and runs on stream v2 are byte-for-byte those of "
          "stream v1 (tests/test_live_evidence.py), so no difference is expected.", ""]

    kinds = Counter(m["type"] for m in telemetry_v2)
    records = {k: sum(len(wire.records_of(m)) for m in telemetry_v2 if m["type"] == k) for k in wire.EVIDENCE}
    L += ["What stream v2 carried: " + ", ".join(f"{kinds[k]:,} `{k}` messages" for k in (wire.METRICS, wire.DAG_RUN, wire.LOGS,
                                                                                          wire.K8S_EVENTS, wire.CHANGES, wire.WATERMARK))
          + f"; {records[wire.LOGS]:,} log lines, {records[wire.K8S_EVENTS]:,} Kubernetes events, {records[wire.CHANGES]:,} changes.", ""]

    # ---- arm C, the tables of the first report
    if C:
        L += ["# Arm C: agent v2 on stream v2", "", triage_score.markdown(C), ""]

    # ---- comparison
    L += ["# v1 against v2", "", "## Verdict against truth, per arm", "",
          "| truth | verdict | " + " | ".join(f"{a}: {ARMS[a]}" for a in "ABC") + " |", "|---|---|---|---|---|"]
    for truth in ("real", "noise"):
        for v in ("real", "noise", "unsure", "no note"):
            L.append(f"| {truth} | {v} | " + " | ".join(str(results[a]["table"][truth][v]) if a in results else "-" for a in "ABC") + " |")
    L += ["", "| | " + " | ".join(f"arm {a}" for a in "ABC") + " |", "|---|---|---|---|"]
    for label, fn in (("real called noise", lambda r: f"{len(r['real_called_noise'])} of {r['real_pages']}"),
                      ("noise called real", lambda r: f"{len(r['noise_called_real'])} of {r['page_outs'] - r['real_pages']}"),
                      ("real called real", lambda r: f"{r['table']['real']['real']} of {r['real_pages']}"),
                      ("(a) names an answer-key entity", lambda r: f"{r['answer_key_entity_named']} of {r['real_pages']}"),
                      ("(b) names the target", lambda r: f"{r['target_named']} of {r['real_pages']}"),
                      ("valid notes", lambda r: f"{r['valid']} of {r['notes']}"),
                      ("cost (USD, estimate)", lambda r: f"{r['cost_total']:.2f}" if r["cost_total"] is not None else "-"),
                      ("tool calls per page (mean)", lambda r: f"{r['tool_calls_per_page'][0]:.1f}" if r["tool_calls_per_page"] else "-"),
                      ("seconds per page (mean)", lambda r: f"{r['seconds_per_page'][0]:.1f}" if r["seconds_per_page"] else "-"),
                      ("budget used up (tool calls / time / tokens)",
                       lambda r: "{tool_calls} / {time} / {tokens}".format(**r["budget_exhausted"]))):
        L.append(f"| {label} | " + " | ".join(fn(results[a]) if a in results else "-" for a in "ABC") + " |")

    # ---- by evidence type
    L += ["", "## By evidence type (arm C)", "",
          "From the v2 answer key: what the matched injection writes to besides the readings. Every injection of this schedule "
          "is a named scenario, so the `metrics only` group (generic injections) is empty here; it is reported so that a "
          "schedule with generic injections can be split the same way.", "",
          "| evidence of the injection | real page-outs | real | noise | unsure |", "|---|---|---|---|---|"]
    if C:
        groups: dict = {}
        for r in C["rows"]:
            if r["truth"] == "real":
                g = groups.setdefault(evidence_label(evidence_of.get(r["injections"][0])), Counter())
                g[r["verdict"] or "no note"] += 1
        groups.setdefault("metrics only", Counter())
        for label, g in sorted(groups.items()):
            L.append(f"| {label} | {sum(g.values())} | {g['real']} | {g['noise']} | {g['unsure']} |")

    # ---- per page-out
    a, bb, c = (_verdicts(results.get(k)) for k in "ABC")
    ids = list(dict.fromkeys([*a, *c]))
    rows = sorted((c.get(i) or a.get(i) for i in ids), key=lambda r: (r["time"], r["page"]))
    L += ["", "## Per page-out", "",
          "A page-out is the same in all arms when its page id (paging time and entity) is the same.", "",
          "| time (UTC) | page | truth | injection | scenario | A | B | C | C cites |", "|---|---|---|---|---|---|---|---|---|"]

    def v(d, i):
        r = d.get(i)
        return "-" if r is None or r["verdict"] is None else f"{r['verdict']} ({r['confidence']})"
    for r in rows:
        i = r["page_id"]
        L.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['truth']} | {', '.join(r['injections']) or '-'} | {r['scenario'] or '-'} | "
                 f"{v(a, i)} | {v(bb, i)} | {v(c, i)} | {live_score.cell(', '.join(c[i]['evidence_tools'])) if i in c else '-'} |")
    changed = [r for r in rows if r["truth"] == "real" and a.get(r["page_id"], {}).get("verdict") != c.get(r["page_id"], {}).get("verdict")]
    L += ["", f"Real page-outs whose verdict differs between A and C: {len(changed)}.", "",
          "## The first run's mistakes, again", "",
          "The four real page-outs arm A called noise, and what B and C say:", "",
          "| time (UTC) | page | scenario | A | B | C | C's suspected cause |", "|---|---|---|---|---|---|---|"]
    for r in (results["A"]["real_called_noise"] if "A" in results else []):
        i = r["page_id"]
        L.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['scenario']} | {v(a, i)} | {v(bb, i)} | {v(c, i)} | "
                 f"{triage_score._cell(c[i]['suspected_cause']) if i in c else '-'} |")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
