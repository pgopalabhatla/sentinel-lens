"""Triage agents on the scripted live week (live-week-1): a DEVELOPMENT-SET evaluation.

    python scripts/triage_dev_week.py            # investigate every page-out not yet noted, then write the report
    python scripts/triage_dev_week.py --report   # only write the report from the notes already on the topic

The page-outs, readings and alerts are the ones the frozen system produced in the single scripted run
(topics `live-week-1-*`, still on the local broker). The dispatcher runs on them exactly as it would
live: it reads `live-week-1-pages` from the start, investigates each page-out as it was at paging time
(three at a time) and writes the notes to `live-week-1-triage`. Notes already on that topic are not
redone, so running this again does not pay twice.

Only after that does this script read the answer key, to score the notes (sentinel_stream/triage_score.py).

Why "development set": the live week has been run, scored and read (docs/results/live-week-1.md), and
the agent's prompt and tools were built in the same session that looked at it. A held-out result
needs a further world (another schedule seed with its own protocol), as for the detectors.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PREFIX = "live-week-1-"
DATA = ROOT / "data_live"
RESULT = ROOT / "docs/results/triage-dev-week-1.md"


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
    topics = Topics(PREFIX + "telemetry", PREFIX + "alerts", PREFIX + "pages", PREFIX + "triage")
    ds = DataSource(DATA)
    config = Config.load()
    wall, ran = None, 0
    if not args.report:
        agent = Investigator(config, run_name="triage-dev-week-1")
        began = time.monotonic()
        d = Dispatcher(KafkaTopics(bootstrap, topics), ds, agent.investigate, topics, concurrency=3)
        d.log(f"{len(d.noted)} notes already on {topics.triage!r}; investigating the rest with {config.model}")
        d.run(until_end=True)
        wall, ran = time.monotonic() - began, d.investigated
        d.log(f"{d.investigated} investigated, {d.failed} failed, {wall:.0f} s")

    # ---- evaluation: only from here on is the answer key read
    triage = broker.read_topic(bootstrap, topics.triage)
    notes = [r for r in triage if r.get("type") == "note"]
    link_records = [r for r in triage if r.get("type") == "link"]
    result = triage_score.score(broker.read_topic(bootstrap, PREFIX + "answer_key"), broker.read_topic(bootstrap, topics.pages),
                                notes, build_topology(ds.relationships(), ds.series()),
                                {name: s.summary for name, s in SCENARIOS.items()})
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    b = config.budget
    header = [
        "# Triage agents on live-week-1 (DEVELOPMENT SET)", "",
        "**This is a development-set result, not a held-out one.** The live week had been run, scored and read before "
        "the agent existed, and the agent's prompt and tools were written in the same session. It says whether the "
        "pipeline works and what kind of mistakes it makes; a held-out number needs a further world.", "",
        f"- written by `scripts/triage_dev_week.py` at commit `{commit}`; notes from the topic `{topics.triage}`",
        f"- agent: `{config.model}`, effort `{config.effort}`, server-side fallback `{config.fallbacks}`; budget per page "
        f"{b.max_tool_calls} tool calls, {b.max_seconds:g} s, {b.max_tokens:,} tokens; one fresh agent per page-out, 3 at a time",
        f"- {result['page_outs']} page-outs; notes on the topic: {len(notes)}; merge links: {len(link_records)}"
        + (f"; this invocation investigated {ran} page-outs in {wall:.0f} s of wall time" if wall is not None else ""),
        "- the notes are advisory: nothing in the system reads them", "",
        "What the agent could see (see docs/log.md, Nov 17, the evidence check): the injected failures change "
        "**readings only** (and, for a paused DAG, remove its run messages). Logs, Kubernetes events and changes are "
        "not on the stream; the tools read them from `data_live`, the quiet world, where they show normal operation "
        "during every injection (no OOMKilled, BackOff or CrashLoopBackOff event, no ERROR line, no deploy record; "
        "the paused `customer_agg` run's Spark pods even appear as scheduled and started). So for injected failures "
        "these three tools can only return nothing or contradicting evidence. The column `evidence from` shows which "
        "tools a note cites.", ""]
    RESULT.write_text("\n".join(header) + "\n" + triage_score.markdown(result) + "\n")
    print(f"saved {RESULT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
