"""The scripted live week: a pre-registered "second world" (docs/live-protocol.md).

    python scripts/run_live_week.py --show-schedule       # the schedule, as the protocol lists it (no broker needed)
    python scripts/run_live_week.py --rehearsal           # the whole pipeline with a dummy injection; touches no result
    python scripts/run_live_week.py --run-once            # THE run: once, saved to docs/results/live-week-1.md

The world is the quiet data_live (no incidents) plus a schedule of injections drawn from the declared
scenario ranges with a fixed seed. The publisher plays the week at max speed and injects; the consumer
runs the frozen engine on it; the live scorer reads the answer key and the pages.

`--run-once` refuses to run when a result already exists, when the protocol is not committed, when
the code differs from what is committed, or when the schedule this code generates is not the one
written in the protocol. Its output file is written by this script and not edited afterwards.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sentinel_stream.events import utc  # noqa: E402
from sentinel_stream.scenarios_live import REHEARSAL, SCENARIOS  # noqa: E402

DATA = ROOT / "data_live"
START, END = "2026-10-26", "2026-11-02"            # the last 7 days of data_live; the 28 days before are history
SEED = 1
PER_SCENARIO = 2                                   # 7 scenarios x 2 = 14 injections in 7 days
NAME = "live-week-1"
RESULT = ROOT / "docs/results/live-week-1.md"
PROTOCOL = ROOT / "docs/live-protocol.md"
GUARDED_PATHS = ["sentinel_lens", "sentinel_sim", "sentinel_stream", "scripts/run_live_week.py", "docs/live-protocol.md"]

LEAD_IN_MIN = (60, 240)        # the first injection starts this long after the stream starts
GAP_MIN = 120                  # at least this long between one injection's stop and the next one's start
GAP_EXTRA_MAX = 720            # plus a random extra of up to this much, shrunk if the week would overflow
TAIL_MIN = 30                  # the scorer's window after a stop; the last injection must leave room for it


def minutes(n) -> pd.Timedelta:
    return pd.Timedelta(minutes=float(n))


# ------------------------------------------------------------------------------------ schedule
def schedule(ds, seed: int = SEED, start=START, end=END, scenarios: dict = SCENARIOS, per_scenario: int = PER_SCENARIO) -> list:
    """The inject commands of the week, in time order. Deterministic in `seed`.

    1. Every scenario `per_scenario` times, in a random order.
    2. For each: a target drawn from the scenario's targets, and every parameter drawn uniformly from
       its declared range (durations and lead times in whole minutes, the rest to 3 significant digits).
    3. Times: the first starts LEAD_IN_MIN after the stream starts; each next one starts GAP_MIN plus a
       random extra (up to GAP_EXTRA_MAX) after the previous stop, so injections never overlap. A DAG
       pause starts `lead_min` before the next scheduled run of its DAG that leaves room for that lead.
       If the week would overflow, all extras are shrunk by the same factor (1.0, 0.9, ... 0.0) until
       the last stop plus TAIL_MIN fits.
    """
    start, end = utc(start), utc(end)
    rng = np.random.default_rng(seed)
    names = [n for n in sorted(scenarios) for _ in range(per_scenario)]
    names = [names[i] for i in rng.permutation(len(names))]
    drawn = []
    for name in names:
        spec = scenarios[name]
        target = spec.targets[int(rng.integers(len(spec.targets)))]
        params = {}
        for k, (lo, hi) in spec.params.items():
            params[k] = float(rng.integers(lo, hi + 1)) if k in ("duration_min", "lead_min") else float(f"{rng.uniform(lo, hi):.3g}")
        drawn.append((spec, target, spec.check(target, params)))
    lead_in = int(rng.integers(LEAD_IN_MIN[0], LEAD_IN_MIN[1] + 1))
    extras = rng.uniform(0, GAP_EXTRA_MAX, len(drawn))
    expected = ds.expected_runs(start=start - pd.Timedelta("1D"), end=end)
    runs_of = {dag: sorted(utc(t) for t in g.scheduled_for) for dag, g in expected.groupby("dag_id")}

    for shrink in np.round(np.arange(1.0, -0.05, -0.1), 1):
        commands, t = [], start + minutes(lead_in)
        for n, (spec, target, params) in enumerate(drawn):
            if n:
                t = commands[-1]["_stop"] + minutes(GAP_MIN) + minutes(round(shrink * extras[n]))
            if spec.pauses_dag:
                lead = minutes(params["lead_min"])
                due = next((r for r in runs_of[target] if r - lead >= t), None)
                if due is None:
                    break
                t = due - lead
            commands.append({"cmd": "inject", "id": f"inj-{n + 1:02d}", "scenario": spec.name, "target": target, "params": params,
                             "at": t.isoformat(), "_stop": t + minutes(params["duration_min"])})
        if len(commands) == len(drawn) and commands[-1]["_stop"] + minutes(TAIL_MIN) <= end:
            return [{k: v for k, v in c.items() if k != "_stop"} for c in commands]
    raise RuntimeError("the schedule does not fit into the week even without extra gaps")


def schedule_markdown(commands: list) -> str:
    lines = ["| id | scenario | target | start (UTC) | stop (UTC) | parameters |", "|---|---|---|---|---|---|"]
    for c in commands:
        at = utc(c["at"])
        stop = at + minutes(c["params"]["duration_min"])
        params = ", ".join(f"{k}={v:g}" for k, v in c["params"].items())
        lines.append(f"| {c['id']} | {c['scenario']} | {c['target']} | {at:%a %b %d %H:%M} | {stop:%a %b %d %H:%M} | {params} |")
    return "\n".join(lines)


# ----------------------------------------------------------------------------------------- run
def run(data, start, end, commands: list, prefix: str, scenarios: dict = SCENARIOS, history_start=None, log=print,
        stream_version: int = 1) -> dict:
    """Play `data` from start to end through Kafka at max speed with `commands` on the control topic,
    run the consumer on it, score. Uses its own topics, all named `prefix` + ..., which must not exist.
    stream_version 2: the stream also carries logs, events and changes (scripts/triage_dev_week_v2.py)."""
    from confluent_kafka.admin import AdminClient

    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, injector, live_score
    from sentinel_stream.publisher import publish

    bootstrap = broker.DEFAULT_BOOTSTRAP
    if not broker.reachable(bootstrap):
        raise SystemExit(f"no Kafka broker at {bootstrap} (start one: docker compose up -d)")
    topic = {k: prefix + k for k in ("telemetry", "control", "answer_key", "alerts", "pages")}
    existing = [t for t in AdminClient(broker.client_config(bootstrap)).list_topics(timeout=10).topics if t.startswith(prefix)]
    if existing:
        raise SystemExit(f"topics with the prefix {prefix!r} already exist: {existing}")
    broker.ensure_topics(bootstrap, list(topic.values()))
    injector.send(bootstrap, topic["control"], commands)
    log(f"{len(commands)} commands on {topic['control']!r}")

    ds = DataSource(data)
    consumer_log = Path(tempfile.mkdtemp(prefix="live-run-")) / "consumer.log"
    args = ["--data", str(data), "--start", str(start), "--bootstrap", bootstrap, "--telemetry-topic", topic["telemetry"],
            "--alerts-topic", topic["alerts"], "--pages-topic", topic["pages"], "--group", prefix + "consumer", "--exit-at-watermark"]
    if history_start:
        args += ["--history-start", str(history_start)]
    with open(consumer_log, "w") as out:
        consumer = subprocess.Popen([sys.executable, "-W", "ignore", "-m", "sentinel_stream.consumer", *args], cwd=ROOT,
                                    env={**os.environ, "PYTHONPATH": str(ROOT)}, stdout=out, stderr=out)
        try:
            sent = publish(ds, start, end, speed=None, bootstrap=bootstrap, topic=topic["telemetry"], history_start=history_start,
                           control=topic["control"], answer_key=topic["answer_key"], scenarios=scenarios,
                           stream_version=stream_version)
            log(f"published {sent} telemetry messages; waiting for the consumer")
            code = consumer.wait(timeout=1800)
        finally:
            if consumer.poll() is None:
                consumer.kill()
    if code != 0:
        raise SystemExit(f"the consumer exited with {code}; its log: {consumer_log}")

    answer_key = broker.read_topic(bootstrap, topic["answer_key"])
    pages = broker.read_topic(bootstrap, topic["pages"])
    result = live_score.score(answer_key, pages, build_topology(ds.relationships(), ds.series()))
    return {"result": result, "answer_key": answer_key, "pages": pages, "topics": topic, "telemetry_messages": sent,
            "alerts": broker.end_offset(bootstrap, topic["alerts"]), "consumer_log": str(consumer_log)}


def quiet_page_outs(data, start, end, history_start=None) -> list:
    """The page-outs of the same week with nothing injected (in-process engine): (time, first alert's target)."""
    from sentinel_sim.replay import DataSource
    from sentinel_stream import PageEvent, StreamEngine, replay

    ds = DataSource(data).at(end)
    engine = StreamEngine.warm_start(ds, start, history_start=history_start)
    return [(e.emitted_at, e.alert.target) for e in replay(engine, ds, start, end, history_start=history_start) if isinstance(e, PageEvent) and e.kind == "paged"]


def report(out: dict, commands: list, quiet: list, header: list) -> str:
    from sentinel_stream import live_score

    r = out["result"]
    started = [a for a in out["answer_key"] if a["event"] == "started"]
    stopped = {a["id"]: a for a in out["answer_key"] if a["event"] == "stopped"}
    same_as_quiet = [f for f in r["false"] if any(f["time"] == t for t, _ in quiet)]
    lines = [f"# {NAME}: the scripted live week", "", *header, "",
             f"- telemetry messages published: {out['telemetry_messages']}",
             f"- alerts written by the consumer: {out['alerts']}; page events: {len(out['pages'])}; page-outs: {r['page_outs']}",
             f"- injections commanded: {len(commands)}; started according to the answer key: {len(started)}; "
             f"stopped: {len(stopped)} ({sum(s['reason'] == 'duration' for s in stopped.values())} by duration)",
             f"- the same week with nothing injected: {len(quiet)} page-outs (all false by construction)",
             f"- of the {r['false_page_outs']} false page-outs here, {len(same_as_quiet)} are at the time of one of those "
             f"{len(quiet)}; {r['false_page_outs'] - len(same_as_quiet)} are not",
             "", live_score.markdown(r).rstrip(), "",
             "## The same week with nothing injected: page-outs", "",
             "| time (UTC) | first alert of the page |", "|---|---|",
             *[f"| {t:%b %d %H:%M} | {live_score.cell(target)} |" for t, target in quiet], "",
             "## Answer key, as written by the publisher", "", "```json",
             *[__import__("json").dumps(a, sort_keys=True) for a in out["answer_key"]], "```", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------- main
def git(*args) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def refuse_unless_ready(commands: list) -> None:
    if RESULT.exists():
        raise SystemExit(f"{RESULT.relative_to(ROOT)} exists: the live week has been run. It is run once.")
    if not PROTOCOL.exists() or git("ls-files", "--error-unmatch", str(PROTOCOL.relative_to(ROOT))) == "":
        raise SystemExit("docs/live-protocol.md is not committed")
    dirty = git("status", "--porcelain", "--", *GUARDED_PATHS)
    if dirty:
        raise SystemExit(f"uncommitted changes in the code or the protocol:\n{dirty}")
    if schedule_markdown(commands) not in PROTOCOL.read_text():
        raise SystemExit("the schedule this code generates is not the one written in docs/live-protocol.md")


def main(argv=None) -> int:
    from sentinel_sim.replay import DataSource

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--show-schedule", action="store_true")
    mode.add_argument("--rehearsal", action="store_true")
    mode.add_argument("--run-once", action="store_true")
    args = ap.parse_args(argv)

    if args.rehearsal:
        # a dummy injection on half a day of the TRAIN split of the evaluated dataset: nothing of the
        # seven scenarios and nothing of data_live's injected week is seen
        from sentinel_stream import broker
        start, end = "2026-10-07 00:00", "2026-10-07 12:00"
        commands = [{"cmd": "inject", "id": "rehearsal-01", "scenario": REHEARSAL.name, "target": REHEARSAL.targets[0],
                     "params": {"duration_min": 20.0}, "at": utc("2026-10-07 06:00").isoformat()}]
        prefix = f"rehearsal-{uuid.uuid4().hex[:8]}-"
        try:
            out = run(ROOT / "data", start, end, commands, prefix, scenarios={REHEARSAL.name: REHEARSAL}, history_start="2026-09-28")
            print(report(out, commands, quiet_page_outs(ROOT / "data", start, end, "2026-09-28"), ["A rehearsal. Not a result."]))
        finally:
            from confluent_kafka.admin import AdminClient
            topics = AdminClient(broker.client_config(broker.DEFAULT_BOOTSTRAP)).list_topics(timeout=10).topics
            broker.delete_topics(broker.DEFAULT_BOOTSTRAP, [t for t in topics if t.startswith(prefix)])
        return 0

    commands = schedule(DataSource(DATA))
    if args.show_schedule:
        print(schedule_markdown(commands))
        return 0

    refuse_unless_ready(commands)
    commit = git("rev-parse", "HEAD")
    out = run(DATA, START, END, commands, NAME + "-")
    header = [f"Written by `scripts/run_live_week.py --run-once` and not edited afterwards. Protocol: [docs/live-protocol.md](../live-protocol.md).",
              "", f"- code: commit `{commit}`", f"- world: `data_live` (`sentinel_stream.quiet_world --days 35 --seed 11`), stream {START} to {END} (UTC), "
              f"engine warm-started on the 28 days before", f"- schedule seed: {SEED}; topics: `{NAME}-*`"]
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(report(out, commands, quiet_page_outs(DATA, START, END), header))
    print(f"saved {RESULT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
