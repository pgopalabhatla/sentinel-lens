"""Live week 2: the frozen detector, triage agent and attacker on a new quiet world (docs/live-week-2-protocol.md).

    python scripts/run_live_week_2.py --check                           # only the refusal checks; runs nothing
    python scripts/run_live_week_2.py --run-once                        # THE run, once: docs/results/live-week-2.md
    python scripts/run_live_week_2.py --run-once --resume "<reason>"    # after a technical failure only (Rules 3)

Steps, in order (each one's output is kept under runs/live-week-2/, so a resumed run reuses what finished):

    1. world      python -m sentinel_stream.quiet_world --days 35 --seed 13 --out data_live_2; file hashes recorded
    2. campaign   the frozen attacker over the 7 days, recorded once (never re-recorded once finished)
    3. replay     the recording on a fresh control topic; publisher (stream v2) + consumer (frozen engine)
    4. triage     the frozen dispatcher and agent, 3 independent runs, each on its own topic
    5. score      only now is the answer key read: detection, triage, the quiet reference; the report

`--run-once` refuses unless the protocol is committed with its predictions filled in, the code is
committed and unchanged, every frozen file equals its tag, data_live_2/ does not exist, there is no
result and no earlier attempt (or --resume with a reason), and a broker and an API key are available.
The result file is written by this script and not edited afterwards.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from statistics import median

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

NAME = "live-week-2"
DATA = ROOT / "data_live_2"
SEED, DAYS = 13, 35                                # 28 days of warm start and history + the 7-day stream
START, END = "2026-10-26", "2026-11-02"
STREAM_VERSION = 2
TRIAGE_RUNS = 3
PREFIX = NAME + "-"                                # topics: live-week-2-telemetry, ...-triage-r1, ...
PROTOCOL = ROOT / "docs/live-week-2-protocol.md"
RESULT = ROOT / "docs/results/live-week-2.md"
CONTROL = ROOT / "docs/results/live-week-2.control.jsonl"
STATE = ROOT / "runs" / NAME                       # gitignored: every step's output and the attempts
PREDICTION_MARKER = "NOT FILLED IN"

FROZEN = {                                         # tag -> what must be identical to it
    "pre-test-freeze": ["sentinel_lens", "sentinel_sim", "docs/config-frozen.md"],
    "triage-freeze-v1": ["sentinel_stream/investigator.py", "sentinel_stream/context_tools.py", "sentinel_stream/dispatcher.py",
                         "sentinel_stream/triage.yaml", "docs/triage-config-frozen.md"],
    "attacker-freeze-v2": ["sentinel_stream/attacker.py", "sentinel_stream/attacker.yaml", "sentinel_stream/injector.py",
                           "sentinel_stream/scenarios_live.py", "docs/attacker-config-frozen.md"],
}
GUARDED = ["sentinel_lens", "sentinel_sim", "sentinel_stream", "scripts/run_live_week_2.py", "scripts/run_live_week.py",
           "scripts/triage_freeze.py", "scripts/attacker_freeze.py", "docs/live-week-2-protocol.md", "docs/config-frozen.md",
           "docs/triage-config-frozen.md", "docs/attacker-config-frozen.md"]


def log(line: str) -> None:
    print(f"[{pd.Timestamp.now(tz='UTC'):%H:%M:%S}] {line}", flush=True)


def git(*args) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, default=str))


def read_json(path: Path):
    return json.loads(path.read_text()) if path.exists() else None


# ------------------------------------------------------------------------------------------- checks
def problems(resume: str | None) -> list:
    out = []
    if RESULT.exists():
        out.append(f"{RESULT.relative_to(ROOT)} exists: live week 2 has been run. It is run once.")
    if STATE.exists() and not resume:
        out.append(f"{STATE.relative_to(ROOT)} exists: an attempt was made. Only a technical failure may be resumed, "
                   f"with --resume \"<reason>\" (protocol, Rules 3)")
    if resume is not None and not resume.strip():
        out.append("--resume needs a reason")
    if resume and not STATE.exists():
        out.append("--resume: there is no earlier attempt to resume")
    if DATA.exists() and not (STATE / "world.json").exists():
        out.append(f"{DATA.relative_to(ROOT)} exists but was not made by this runner: it must not exist before the run")
    if git("ls-files", "--", str(PROTOCOL.relative_to(ROOT))) == "":
        out.append(f"{PROTOCOL.relative_to(ROOT)} is not committed")
    elif PREDICTION_MARKER in PROTOCOL.read_text():
        out.append(f"fill in the predictions in {PROTOCOL.relative_to(ROOT)} and commit them first")
    dirty = git("status", "--porcelain", "--", *GUARDED)
    if dirty:
        out.append(f"uncommitted changes in the code, the frozen files or the protocol:\n{dirty}")
    for tag, paths in FROZEN.items():
        if not git("rev-parse", "--verify", "--quiet", f"{tag}^{{commit}}"):
            out.append(f"the tag {tag} does not exist")
        elif subprocess.run(["git", "diff", "--quiet", tag, "--", *paths], cwd=ROOT).returncode != 0:
            out.append(f"files frozen by {tag} differ from it: {paths}")
    import attacker_freeze
    import triage_freeze
    for mod in (triage_freeze, attacker_freeze):
        if not mod.OUT.exists() or mod.OUT.read_text() != mod.generate():
            out.append(f"{mod.OUT.relative_to(ROOT)} is not what {Path(mod.__file__).name} generates from the code")
    from sentinel_stream import broker
    if not broker.reachable(broker.DEFAULT_BOOTSTRAP):
        out.append(f"no Kafka broker at {broker.DEFAULT_BOOTSTRAP} (start one: docker compose up -d)")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        out.append("ANTHROPIC_API_KEY is not set")
    return out


# -------------------------------------------------------------------------------------------- 1. world
def hashes(folder: Path) -> dict:
    return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(folder.rglob("*")) if p.is_file()}


def world() -> dict:
    state = STATE / "world.json"
    if state.exists():
        rec = read_json(state)
        if hashes(DATA) != rec["files"]:
            raise SystemExit(f"{DATA.relative_to(ROOT)} no longer matches the hashes recorded when it was generated")
        log("world: reused, hashes match")
        return rec
    from sentinel_stream import quiet_world
    log(f"world: generating {DATA.relative_to(ROOT)} (seed {SEED}, {DAYS} days)")
    quiet_world.generate(DATA, seed=SEED, days=DAYS)
    files = hashes(DATA)
    rec = {"command": f"python -m sentinel_stream.quiet_world --days {DAYS} --seed {SEED} --out {DATA.name}", "files": files,
           "combined": hashlib.sha256("".join(f"{k} {v}\n" for k, v in files.items()).encode()).hexdigest()}
    write_json(state, rec)
    return rec


# ----------------------------------------------------------------------------------------- 2. campaign
def campaign() -> dict:
    """The frozen attacker over the 7 days. A finished recording is never made again."""
    from sentinel_sim.replay import DataSource
    from sentinel_stream import attacker as A

    folder = STATE / "campaign"
    if (folder / "record.json").exists() and (folder / "control.jsonl").exists():
        log("campaign: reused the finished recording")
    else:
        if folder.exists():
            note_attempt("an unfinished campaign was discarded and is recorded again")
            shutil.rmtree(folder)
        attacker = A.Attacker(DataSource(DATA), A.Config.load(), window=(START, END))
        log(f"campaign: {START} .. {END}, frozen attacker ({attacker.config.model})")
        record = A.run_campaign(attacker, A.Agent(attacker), START, END, log=log)
        A.save(record, folder)
    shutil.copyfile(folder / "control.jsonl", CONTROL)
    return read_json(folder / "record.json")


# ------------------------------------------------------------------------------------------- 3. replay
def topic_names() -> dict:
    return {k: PREFIX + k for k in ("telemetry", "control", "answer_key", "alerts", "pages")}


def replay(commands: list) -> dict:
    """The recording through Kafka: publisher (stream v2, injecting) + consumer (frozen engine). Deterministic
    from the recording, so an unfinished replay is deleted and made again."""
    import run_live_week
    from confluent_kafka.admin import AdminClient

    from sentinel_stream import broker

    state = STATE / "replay.json"
    if state.exists():
        log("replay: reused")
        return read_json(state)
    b = broker.DEFAULT_BOOTSTRAP
    left = [t for t in AdminClient(broker.client_config(b)).list_topics(timeout=10).topics if t in topic_names().values()]
    if left:
        note_attempt(f"an unfinished replay's topics were deleted and the replay made again: {sorted(left)}")
        broker.delete_topics(b, left)
    began = time.monotonic()
    out = run_live_week.run(DATA, START, END, commands, PREFIX, log=log, stream_version=STREAM_VERSION)
    rec = {"topics": out["topics"], "telemetry_messages": out["telemetry_messages"], "alerts": out["alerts"],
           "page_records": len(out["pages"]), "seconds": round(time.monotonic() - began, 1)}
    write_json(state, rec)              # the score run() computed is not kept: everything is scored in step 5
    return rec


# ------------------------------------------------------------------------------------------- 4. triage
def triage(k: int, topics: dict) -> dict:
    """One run of the frozen dispatcher and agent on every page-out, on its own topic. A run cut off is
    continued from its topic (the dispatcher's restart rule)."""
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, live_score
    from sentinel_stream.dispatcher import Dispatcher, KafkaTopics, Topics
    from sentinel_stream.investigator import Config, Investigator

    state = STATE / f"triage-r{k}.json"
    if state.exists():
        log(f"triage run {k}: reused")
        return read_json(state)
    b = broker.DEFAULT_BOOTSTRAP
    name = f"{PREFIX}triage-r{k}"
    broker.ensure_topics(b, [name])
    config = Config.load()
    t = Topics(topics["telemetry"], topics["alerts"], topics["pages"], name)
    agent = Investigator(config, run_name=f"{NAME}-triage-r{k}")
    began = time.monotonic()
    d = Dispatcher(KafkaTopics(b, t), DataSource(DATA), agent.investigate, t, concurrency=3, stream_version=STREAM_VERSION,
                   follow_up_minutes=config.follow_up_minutes)
    d.log(f"triage run {k}: {len(d.noted)} notes already on {name!r}; {config.model}")
    d.run(until_end=True)
    outs, _ = live_score.page_outs(broker.read_topic(b, topics["pages"]))
    noted = {r["page_id"] for r in broker.read_topic(b, name) if r.get("type") == "note"}
    missing = [o["page_id"] for o in outs if o["page_id"] not in noted]
    if missing:
        raise SystemExit(f"technical failure: triage run {k} left {len(missing)} page-outs without a note ({missing[:5]}); "
                         f"resume with --run-once --resume \"<reason>\"")
    rec = {"topic": name, "investigated": d.investigated, "followed_up": d.followed_up, "failed": d.failed,
           "follow_ups_past_end": d.follow_ups_past_end, "seconds": round(time.monotonic() - began, 1)}
    write_json(state, rec)
    return rec


# ------------------------------------------------------------------------------------- 5. score, report
def wilson(k: int, n: int) -> str:
    from sentinel_lens.stats import wilson_interval
    lo, hi = wilson_interval(k, n)
    return f"{lo:.2f}-{hi:.2f}"


def garwood(k: int, days: float) -> tuple:
    """Exact Poisson 95% interval for a count k over `days`, per day."""
    from scipy.stats import chi2
    lo = 0.0 if k == 0 else chi2.ppf(0.025, 2 * k) / 2
    return lo / days, chi2.ppf(0.975, 2 * k + 2) / 2 / days


def kind_of(inj: dict) -> str:
    return "generic" if inj["scenario"] == "custom" else "named"


def names_target(row: dict, inj: dict | None) -> bool:
    """Protocol, Triage: a named scenario's cause must name its target; a generic injection's must name
    any entity the answer key lists for it. Both under triage_score's whole-word rule."""
    from sentinel_stream import triage_score
    if inj is None:
        return False
    if kind_of(inj) == "named":
        return row["cause_names_target"]
    return any(triage_score.text_names_target(row["cause"], e.split(":", 1)[-1]) for e in inj["entities"])


def m(x) -> str:
    return "-" if x is None else f"{x:.0f}"


def m1(x) -> str:
    return "-" if x is None else f"{x:.1f}"


def campaign_section(record: dict, started_ids: set) -> list:
    from sentinel_stream.live_score import cell
    inj, b = record["injections"], record["config"]["budget"]
    days = pd.date_range(START, END, freq="D", inclusive="left", tz="UTC")
    per_day = Counter(i["at"][:10] for i in inj)
    calls = Counter(c["tool"] for w in record["wakes"] for c in w["tool_calls"])
    rule = Counter(r["reasons"][0].split(":")[0] if r["reasons"][0].split(":")[0] in ("budget", "quota") else
                   "partly accepted" if r.get("partly_accepted") else "other" for r in record["rejections"])
    L = ["## The attacker campaign", "",
         f"Frozen attacker (`attacker-freeze-v2`): `{record['config']['model']}`, effort `{record['config']['effort']}`, woken "
         f"every {record['config']['wake_every_minutes']:g} simulated minutes ({len(record['wakes'])} wakes). Budget in code: at most "
         f"{b['max_per_day']} a day, {b['max_active']} active, starts {b['min_gap_minutes'] / 60:g} h apart, named scenarios at most "
         f"{b['max_named_share']:.0%} of the campaign's injections. Recorded once; control stream: "
         f"[{CONTROL.name}]({CONTROL.name}).", "",
         f"- injections: **{len(inj)}**; named {sum(i['kind'] == 'scenario' for i in inj)}, generic {sum(i['kind'] == 'generic' for i in inj)}; "
         f"noticeable {sum(i['severity'] == 'noticeable' for i in inj)}, subtle {sum(i['severity'] == 'subtle' for i in inj)}",
         f"- the attacker's own rejections: {len(record['rejections'])}" + (" (" + ", ".join(f"{k} {v}" for k, v in rule.most_common()) + ")"
                                                                           if rule else ""),
         f"- cost: USD {record['cost_usd']:.3f} (estimate); tokens {record['tokens']}",
         f"- tool calls: {sum(calls.values())} ({', '.join(f'{k} {v}' for k, v in calls.most_common())})", "",
         "Injections per simulated day (by start):", "", "| day | injections |", "|---|---|",
         *(f"| {d:%a %b %d} | {per_day.get(f'{d:%Y-%m-%d}', 0)} |" for d in days), "",
         "| id | start (UTC) | stop | kind | scenario / effects | severity | started on the stream | intent | novelty |",
         "|---|---|---|---|---|---|---|---|---|"]
    for i in inj:
        what = (f"{i['scenario']} on {i['target']}" if i["kind"] == "scenario"
                else f"{len(next(c for c in record['commands'] if c.get('id') == i['id'])['effects'])} effects on "
                     + ", ".join(i["entities"]))
        L.append(f"| {i['id']} | {i['at'][5:16]} | {(i['stopped_at'] or i['stop'])[5:16]} | {i['kind']} | {cell(what)} | {i['severity']} | "
                 f"{'yes' if i['id'] in started_ids else 'NO'} | {cell(i['intent'])} | {cell(i.get('novelty') or '-')} |")
    generic = [i for i in inj if i["kind"] == "generic" and "overlap" in i]
    if generic:
        names = list(generic[0]["overlap"])
        L += ["", "Generic incidents: series shared with each named scenario's series, `shared / the incident's (Jaccard)` "
              "(reported, not enforced):", "", "| incident | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        for i in generic:
            o = i["overlap"]
            L.append(f"| {i['id']} | " + " | ".join(f"{o[n]['shared']}/{o[n]['of_incident']} ({o[n]['jaccard']:g})" for n in names) + " |")
    return L + [""]


def detection_section(det: dict, key: list, quiet: list, commands: list) -> list:
    from sentinel_stream import live_score
    rows = det["injections"]
    rejected = [a for a in key if a["event"] == "rejected"]
    partly = [a for a in key if a["event"] == "started" and a.get("rejected")]
    same_as_quiet = [f for f in det["false"] if any(f["time"] == t for t, _ in quiet)]
    lo, hi = garwood(det["false_page_outs"], det["days"])

    def group(label, sel):
        g = [r for r in rows if sel(r)]
        k, times = sum(r["detected"] for r in g), [r["time_to_detect_min"] for r in g if r["detected"]]
        return (f"| {label} | {len(g)} | {k} | {wilson(k, len(g)) if g else '-'} | {m(median(times)) if times else '-'} | "
                f"{m(max(times)) if times else '-'} |")
    kind = {r["id"]: ("generic" if r["scenario"] == "custom" else "named") for r in rows}
    L = ["## Detection", "",
         f"- inject commands in the recording: {sum(c['cmd'] == 'inject' for c in commands)}; started on the stream (counted): "
         f"{len(rows)}; refused entirely by the publisher (not counted): {len(rejected)}"
         + (f" ({'; '.join(a['id'] + ': ' + a['reasons'][0] for a in rejected)})" if rejected else "")
         + f"; started with some effects refused: {len(partly)}",
         f"- false page-outs: **{det['false_page_outs']}** in {det['days']:g} days = **{det['false_pages_per_day']:.2f} a day** "
         f"(exact Poisson 95%: {lo:.2f}-{hi:.2f})",
         f"- the same 7 days with nothing injected: {len(quiet)} page-outs; of the {det['false_page_outs']} false page-outs here, "
         f"{len(same_as_quiet)} are at the time of one of those", "",
         "| injections | n | detected | Wilson 95% | median time to detect (min) | max (min) |", "|---|---|---|---|---|---|",
         group("all", lambda r: True),
         group("named", lambda r: kind[r["id"]] == "named"), group("generic", lambda r: kind[r["id"]] == "generic"),
         group("noticeable", lambda r: r["severity"] == "noticeable"), group("subtle", lambda r: r["severity"] == "subtle"),
         *(group(f"{k} x {s}", lambda r, k=k, s=s: kind[r["id"]] == k and r["severity"] == s)
           for k in ("named", "generic") for s in ("noticeable", "subtle")), "",
         "### The scorer's tables (`sentinel_stream/live_score.py`)", "", live_score.markdown(det).rstrip(), "",
         "### The same 7 days with nothing injected: page-outs", "", "| time (UTC) | first alert of the page |", "|---|---|",
         *[f"| {t:%b %d %H:%M} | {live_score.cell(target)} |" for t, target in quiet], ""]
    return L


def triage_section(scored: list, raw: list, runs: list, inj_by_id: dict) -> list:
    from sentinel_stream import triage_score
    _cell = triage_score._cell
    R = range(len(scored))
    hdr = "| | " + " | ".join(f"run {k + 1}" for k in R) + " | min-max | sum |"
    sep = "|---|" + "---|" * (len(scored) + 2)

    def line(label, vals, total=True, fmt=str):
        lo_hi = f"{fmt(min(vals))}-{fmt(max(vals))}" if all(v is not None for v in vals) else "-"
        return f"| {label} | " + " | ".join(fmt(v) if v is not None else "-" for v in vals) + f" | {lo_hi} | " + \
            (fmt(sum(vals)) if total and all(v is not None for v in vals) else "") + " |"

    def by_kind(s, kind):
        real = [r for r in s["rows"] if r["truth"] == "real" and kind_of(inj_by_id[r["injections"][0]]) == kind]
        return sum(names_target(r, inj_by_id[r["injections"][0]]) for r in real), len(real)
    n_true, n_false = scored[0]["real_pages"], scored[0]["noise_pages"]
    L = ["## Triage", "",
         f"The frozen agent (`triage-freeze-v1`), {len(scored)} independent runs on the same {n_true + n_false} page-outs "
         f"({n_true} true, {n_false} false under the live rule). Final answer = the follow-up's when there was one.", "",
         hdr, sep]
    for truth, n, outcomes in (("real", n_true, ("correct", "dangerous", "deferred", "no note")),
                               ("noise", n_false, ("correct", "wasted", "deferred", "no note"))):
        for o in outcomes:
            label = f"{'true' if truth == 'real' else 'false'} pages ({n}): {'**DANGEROUS**' if o == 'dangerous' else o}"
            L.append(line(label, [s["final"][truth][o] for s in scored]))
    L += ["| true pages: share correct (Wilson 95%) | " + " | ".join(
              f"{s['final']['real']['correct'] / n_true:.2f} ({wilson(s['final']['real']['correct'], n_true)})" if n_true else "-"
              for s in scored) + " | | |"]
    for kind in ("named", "generic"):
        vals = [by_kind(s, kind) for s in scored]
        L.append(f"| cause names the target, {kind} (of {vals[0][1]} true pages) | " + " | ".join(str(v[0]) for v in vals) +
                 f" | {min(v[0] for v in vals)}-{max(v[0] for v in vals)} | |")
    L += [line("follow-ups", [s["follow_ups"] for s in scored]),
          line("notes the guard converted", [sum(1 for r in rw if r.get("type") in ("note", "follow_up") and r.get("guard")) for rw in raw]),
          line("minutes to the final answer, median", [(s["minutes_to_final"] or (None, None))[0] for s in scored], False, m1),
          line("minutes to the final answer, max", [(s["minutes_to_final"] or (None, None))[1] for s in scored], False, m1),
          line("cost, USD (estimate, follow-ups included)", [s["cost_total"] for s in scored], True, lambda x: f"{x:.2f}"),
          line("wall time, s", [r["seconds"] for r in runs], True, lambda x: f"{x:.0f}"), ""]

    finals = {}
    for s in scored:
        for r in s["rows"]:
            finals.setdefault(r["page_id"], []).append(r["final"])
    same = sum(len(set(v)) == 1 and len(v) == len(scored) for v in finals.values())
    L += [f"Agreement: {same} of {len(finals)} page-outs got the same final answer in all {len(scored)} runs.", "",
          "### Every dangerous final answer", ""]
    dangerous = [(k + 1, r) for k, s in enumerate(scored) for r in s["dangerous"]]
    if dangerous:
        L += ["| run | time (UTC) | page | injection | kind | scenario | severity | first | follow-up | cause category | cause |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
        for k, r in dangerous:
            inj = inj_by_id[r["injections"][0]]
            L.append(f"| {k} | {r['time']:%b %d %H:%M} | {r['page']} | {', '.join(r['injections'])} | {kind_of(inj)} | "
                     f"{r['scenario'] if kind_of(inj) == 'named' else 'generic'} | {inj.get('severity') or '-'} | {r['first'] or '-'} | "
                     f"{r['follow_up'] or '-'} | {r['cause_category'] or '-'} | {_cell(r['cause'])} |")
    else:
        L.append("None, in any run.")
    L += ["", "### Per page-out", "",
          "| time (UTC) | page | truth | injection | kind | " + " | ".join(f"run {k + 1} final (outcome)" for k in R) + " |",
          "|---|---|---|---|---|" + "---|" * len(scored)]
    rows = {k: {r["page_id"]: r for r in s["rows"]} for k, s in enumerate(scored)}
    for r in scored[0]["rows"]:
        inj = inj_by_id[r["injections"][0]] if r["injections"] else None
        L.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['truth']} | {', '.join(r['injections']) or '-'} | "
                 f"{kind_of(inj) if inj else '-'} | " + " | ".join(
                     f"{rows[k][r['page_id']]['final'] or '-'} ({rows[k][r['page_id']]['final_outcome']})" for k in R) + " |")
    for k, s in enumerate(scored):
        L += ["", f"### Run {k + 1} in full", "", triage_score.markdown_needs_human(s, title=f"Run {k + 1}: ").rstrip()]
    return L + [""]


def score_and_report(world_rec: dict, record: dict, replay_rec: dict, runs: list) -> str:
    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, live_score, triage_score
    from sentinel_stream.scenarios_live import SCENARIOS
    import run_live_week

    b, topics = broker.DEFAULT_BOOTSTRAP, replay_rec["topics"]
    ds = DataSource(DATA)
    topo = build_topology(ds.relationships(), ds.series())
    # ---- only from here on is the answer key read
    key, pages = broker.read_topic(b, topics["answer_key"]), broker.read_topic(b, topics["pages"])
    det = live_score.score(key, pages, topo)
    injs, _ = live_score.injections(key)
    inj_by_id = {i["id"]: i for i in injs}
    summaries = {n: s.summary for n, s in SCENARIOS.items()}
    raw, scored = [], []
    for r in runs:
        records = broker.read_topic(b, r["topic"])
        raw.append(records)
        scored.append(triage_score.score_needs_human(key, pages, [x for x in records if x.get("type") == "note"],
                                                     [x for x in records if x.get("type") == "follow_up"], topo, summaries))
    quiet = run_live_week.quiet_page_outs(DATA, START, END)
    commands = record["commands"]

    tags = {t: git("rev-parse", "--short", f"{t}^{{commit}}") for t in FROZEN}
    attempts = read_json(STATE / "attempts.json") or []
    total = record["cost_usd"] + sum(s["cost_total"] or 0 for s in scored)
    L = [f"# {NAME}: frozen detector, triage agent and attacker on a new world", "",
         "Written by `scripts/run_live_week_2.py --run-once` and not edited afterwards. "
         "Protocol: [docs/live-week-2-protocol.md](../live-week-2-protocol.md).", "",
         f"- code: commit `{git('rev-parse', 'HEAD')}`; protocol last committed in `{git('log', '-1', '--format=%h', '--', str(PROTOCOL.relative_to(ROOT)))}`",
         "- frozen: " + ", ".join(f"`{t}` ({c})" for t, c in tags.items()) + "; every frozen file identical to its tag",
         f"- world: `{world_rec['command']}`, {len(world_rec['files'])} files, combined SHA-256 `{world_rec['combined']}`; "
         f"stream {START} to {END} (UTC), version {STREAM_VERSION}; engine warm-started on the 28 days before",
         f"- topics: `{PREFIX}*`; telemetry messages {replay_rec['telemetry_messages']:,}; alerts {replay_rec['alerts']}; "
         f"page-outs {det['page_outs']}",
         "- attempts: " + "; ".join(f"{a['at']}: {a['reason']}" for a in attempts),
         f"- **total cost: USD {total:.2f}** (campaign {record['cost_usd']:.2f} + triage "
         + " + ".join(f"{s['cost_total'] or 0:.2f}" for s in scored) + ")", "",
         *campaign_section(record, {i["id"] for i in injs}),
         *detection_section(det, key, quiet, commands),
         *triage_section(scored, raw, runs, inj_by_id),
         "## Answer key, as written by the publisher", "", "```json", *[json.dumps(a, sort_keys=True) for a in key], "```", ""]
    return "\n".join(L)


# ---------------------------------------------------------------------------------------------- main
def note_attempt(reason: str) -> None:
    path = STATE / "attempts.json"
    attempts = read_json(path) or []
    attempts.append({"at": f"{pd.Timestamp.now(tz='UTC'):%Y-%m-%d %H:%M} UTC", "commit": git("rev-parse", "--short", "HEAD"),
                     "reason": reason})
    write_json(path, attempts)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="only the refusal checks; runs nothing")
    mode.add_argument("--run-once", action="store_true")
    ap.add_argument("--resume", metavar="REASON", default=None, help="continue after a technical failure (protocol, Rules 3)")
    args = ap.parse_args(argv)

    found = problems(args.resume)
    if args.check:
        print("ready to run" if not found else "would refuse:\n  - " + "\n  - ".join(found))
        return 0 if not found else 1
    if found:
        raise SystemExit("Refusing to run live week 2:\n  - " + "\n  - ".join(found))

    note_attempt("first attempt" if args.resume is None else f"resumed: {args.resume}")
    world_rec = world()
    record = campaign()
    from sentinel_stream import attacker as A
    commands = A.load_commands(CONTROL)
    replay_rec = replay(commands)
    runs = [triage(k, replay_rec["topics"]) for k in range(1, TRIAGE_RUNS + 1)]
    text = score_and_report(world_rec, record, replay_rec, runs)
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(text)
    log(f"saved {RESULT.relative_to(ROOT)} and {CONTROL.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
