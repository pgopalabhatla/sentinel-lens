"""Live scorer: were the injections paged for, and how many pages were for nothing?

    python -m sentinel_stream.live_score --data data_live

It reads two topics and nothing else from the stream: `answer_key` (what the publisher injected) and
`pages` (what the consumer said about pages). The entity graph for "1 hop" is the correlator's own
(`build_topology` on the data dir's catalog: configuration, not telemetry).

The rule (fixed before the first scored run, docs/live-protocol.md):

    A PAGE-OUT is a `paged` event: the moment an engineer is paged. What the page holds at that
    moment is what they are paged with.

    A page-out MATCHES an injection when
        its time is inside [start, stop + 30 min] of the injection, and
        the page holds, at that moment, an alert on one of the injection's target entities or on an
        entity 1 hop from one of them.
    The target entities are the entities that own the series the injection changes (and the DAG, for
    a pause), as the publisher wrote them to the answer key.

    An injection is DETECTED when at least one page-out matches it.
    TIME TO DETECT = time of its first matching page-out - its start.
    A page-out that matches no injection is FALSE.

Consequences worth knowing: an alert that joins a page AFTER that page has paged does not make a
detection (nobody is paged again), and a page that paged before the injection started does not count
however much it grows. The column `alerts near target` shows such cases: it counts the alerts on or
1 hop from the targets that fired inside the window, paged for or not.
"""
from __future__ import annotations

import argparse
from statistics import median

import pandas as pd

from sentinel_stream.events import utc

TAIL = pd.Timedelta("30min")        # a page this long after the stop still counts
MAX_HOPS = 1


def injections(answer_key: list) -> tuple:
    """([injection], (stream start, stream end) or None) from the answer-key records. An injection's
    stop is the actual one if a `stopped` record exists, else the planned one."""
    out, span = {}, None
    for r in answer_key:
        if r["event"] == "started":
            out[r["id"]] = {**r, "start": utc(r["start"]), "stop": utc(r["stop"])}
        elif r["event"] == "stopped" and r["id"] in out:
            out[r["id"]]["stop"] = utc(r["stop"])
        elif r["event"] == "published":
            start, end = utc(r["start"]), utc(r["end"])
            span = (start, end) if span is None else (min(span[0], start), max(span[1], end))
    return sorted(out.values(), key=lambda i: (i["start"], i["id"])), span


def page_outs(pages: list) -> tuple:
    """([page-out], [every alert with the time it was emitted]) from the `pages` records. A page-out
    carries the alerts its page held at the moment it paged."""
    held, outs, alerts = {}, [], []
    for r in pages:
        key = r["page"]
        if r["kind"] == "paged":
            outs.append({"time": utc(r["emitted_at"]), "page": key, "page_id": r["page_id"], "alerts": list(held.get(key, []))})
            continue
        held.setdefault(key, []).append(r["alert"])
        alerts.append((utc(r["emitted_at"]), r["alert"]))
        for absorbed in r.get("absorbed", []):                  # a merge: the other pages' alerts move here
            held[key] += held.pop(absorbed, [])
    return outs, alerts


def score(answer_key: list, pages: list, topology, days: float | None = None) -> dict:
    """topology: sentinel_lens.correlator.Topology. days: length of the run, if the answer key has
    no `published` record."""
    injs, span = injections(answer_key)
    outs, alerts = page_outs(pages)
    if days is None:
        if span is None:
            raise ValueError("the answer key has no 'published' record; give the length of the run in days")
        days = (span[1] - span[0]) / pd.Timedelta("1D")

    for inj in injs:
        inj["near"] = frozenset().union(*(topology.within(e, MAX_HOPS) for e in inj["entities"]))
        inj["matches"] = []
        inj["alerts_near_target"] = sum(inj["start"] <= t <= inj["stop"] + TAIL and topology.entity(a["target"]) in inj["near"]
                                        for t, a in alerts)
    for out in outs:
        out["matched"] = []
        for inj in injs:
            hits = [a for a in out["alerts"] if topology.entity(a["target"]) in inj["near"]]
            if hits and inj["start"] <= out["time"] <= inj["stop"] + TAIL:
                out["matched"].append(inj["id"])
                inj["matches"].append({"time": out["time"], "page": out["page"], "alert": hits[0]})

    rows = []
    for inj in injs:
        first = min(inj["matches"], key=lambda m: m["time"]) if inj["matches"] else None
        rows.append({"id": inj["id"], "scenario": inj["scenario"], "target": inj["target"], "params": inj["params"],
                     "start": inj["start"], "stop": inj["stop"], "detected": first is not None,
                     "time_to_detect_min": None if first is None else (first["time"] - inj["start"]) / pd.Timedelta("1min"),
                     "paged_by": None if first is None else f"{first['alert']['target']} ({first['alert']['detector']})",
                     "matching_page_outs": len(inj["matches"]), "alerts_near_target": inj["alerts_near_target"]})
    false = [o for o in outs if not o["matched"]]
    times = [r["time_to_detect_min"] for r in rows if r["detected"]]
    by_scenario = {}
    for r in rows:
        s = by_scenario.setdefault(r["scenario"], {"injected": 0, "detected": 0, "times": []})
        s["injected"] += 1
        s["detected"] += r["detected"]
        s["times"] += [r["time_to_detect_min"]] if r["detected"] else []
    return {"injections": rows, "by_scenario": by_scenario, "days": days,
            "n_injections": len(rows), "n_detected": sum(r["detected"] for r in rows),
            "median_time_to_detect_min": median(times) if times else None, "max_time_to_detect_min": max(times) if times else None,
            "page_outs": len(outs), "matched_page_outs": len(outs) - len(false), "false_page_outs": len(false),
            "false_pages_per_day": len(false) / days if days else None,
            "false": [{"time": o["time"], "page": o["page"],
                       "alerts": sorted({f"{a['target']} ({a['detector']})" for a in o["alerts"]})} for o in false]}


# ------------------------------------------------------------------------------------- report
def _minutes(x) -> str:
    return "-" if x is None else f"{x:.0f}"


def _params(p: dict) -> str:
    return ", ".join(f"{k}={v:g}" for k, v in p.items())


def cell(text) -> str:
    """A series id has a | in it, which would split a Markdown table cell."""
    return str(text).replace("|", "\\|")


def markdown(result: dict) -> str:
    r = result
    lines = ["## Overall", "",
             "| injections | detected | median time to detect (min) | max time to detect (min) | page-outs | "
             "matching an injection | FALSE | false pages per day |",
             "|---|---|---|---|---|---|---|---|",
             f"| {r['n_injections']} | {r['n_detected']} | {_minutes(r['median_time_to_detect_min'])} | "
             f"{_minutes(r['max_time_to_detect_min'])} | {r['page_outs']} | {r['matched_page_outs']} | {r['false_page_outs']} | "
             f"{r['false_pages_per_day']:.2f} |",
             "", f"Length of the run: {r['days']:g} days.", "",
             "## By scenario", "", "| scenario | injected | detected | times to detect (min) |", "|---|---|---|---|"]
    for name, s in sorted(r["by_scenario"].items()):
        lines.append(f"| {name} | {s['injected']} | {s['detected']} | {', '.join(_minutes(t) for t in s['times']) or '-'} |")
    lines += ["", "## Per injection", "",
              "| id | scenario | target | start (UTC) | stop (UTC) | parameters | detected | time to detect (min) | "
              "first paged by | matching page-outs | alerts near target |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    for i in r["injections"]:
        lines.append(f"| {i['id']} | {i['scenario']} | {i['target']} | {i['start']:%b %d %H:%M} | {i['stop']:%b %d %H:%M} | "
                     f"{_params(i['params'])} | {'yes' if i['detected'] else 'NO'} | {_minutes(i['time_to_detect_min'])} | "
                     f"{cell(i['paged_by'] or '-')} | {i['matching_page_outs']} | {i['alerts_near_target']} |")
    lines += ["", "## False page-outs", ""]
    if r["false"]:
        lines += ["| time (UTC) | page | alerts in the page when it paged |", "|---|---|---|"]
        lines += [f"| {f['time']:%b %d %H:%M} | {f['page']} | {cell('; '.join(f['alerts']))} |" for f in r["false"]]
    else:
        lines.append("None.")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    ap = argparse.ArgumentParser(description="Score the live injections from the answer_key and pages topics.")
    ap.add_argument("--data", required=True, help="data directory, for the entity graph (catalog only)")
    ap.add_argument("--bootstrap", default=broker.DEFAULT_BOOTSTRAP)
    ap.add_argument("--answer-key-topic", default="answer_key")
    ap.add_argument("--pages-topic", default="pages")
    ap.add_argument("--days", type=float, default=None, help="length of the run, if the publisher did not finish")
    args = ap.parse_args(argv)
    ds = DataSource(args.data)
    result = score(broker.read_topic(args.bootstrap, args.answer_key_topic), broker.read_topic(args.bootstrap, args.pages_topic),
                   build_topology(ds.relationships(), ds.series()), args.days)
    print(markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
