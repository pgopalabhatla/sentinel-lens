"""Report on one attacker campaign: what it injected, what was rejected, what it cost. No scoring.

    python scripts/attacker_report.py dry-run-1        # reads runs/attacker/dry-run-1/, writes docs/results/attacker-dry-run-1.md

Also copies the recorded control stream to docs/results/attacker-<name>.control.jsonl (the record with
the full transcripts stays in runs/, gitignored) and replays it through the injector without the LLM, to
show every command is accepted there too. Nothing here runs the engine or the triage agent, and nothing
is scored against detection: the campaign is a description of what was injected.
"""
from __future__ import annotations

import json
import shutil
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DATA = ROOT / "data_live"


def cell(x) -> str:
    return str(x).replace("|", "\\|").replace("\n", " ")


def main(argv=None) -> int:
    from sentinel_sim.replay import DataSource
    from sentinel_stream import attacker as A

    name = (argv or sys.argv[1:])[0]
    folder = ROOT / "runs/attacker" / name
    record = json.loads((folder / "record.json").read_text())
    control = ROOT / f"docs/results/attacker-{name}.control.jsonl"
    shutil.copyfile(folder / "control.jsonl", control)
    start, end = record["window"]
    sent, key = A.replay(DataSource(DATA), start, end, A.load_commands(control))
    started = {k["id"]: k for k in key if k["event"] == "started"}
    refused = [k for k in key if k["event"] == "rejected"]
    held = {k["id"]: k.get("minutes_held_at_a_limit") for k in key if k["event"] == "stopped" and k.get("minutes_held_at_a_limit")}

    inj = record["injections"]
    wakes = record["wakes"]
    calls = Counter(c["tool"] for w in wakes for c in w["tool_calls"])
    cfg, b = record["config"], record["config"]["budget"]
    by_day = Counter(i["at"][:10] for i in inj)
    L = [f"# Attacker campaign `{name}`: a dry run (not scored)", "",
         "**Not scored, on purpose.** This is one short real run of the attacker agent, to see what it injects. Its "
         "injections were not run through the engine or the triage agent, and nothing here measures detection.", "",
         f"- window: {start} .. {end} of `data_live` (the quiet world), campaign mode (the live loop at max speed, no broker)",
         f"- agent: `{cfg['model']}`, effort `{cfg['effort']}`, woken every {cfg['wake_every_minutes']:g} simulated minutes "
         f"({len(wakes)} wakes); per wake at most {b['max_tool_calls_per_wake']} tool calls",
         f"- budget enforced in code: at most {b['max_per_day']} injections a day, at most {b['max_active']} active at once"
         + (f", starts at least {b['min_gap_minutes'] / 60:g} h apart, only series described (describe_normal) in the same wake"
            if "min_gap_minutes" in b else "")
         + (f", named scenarios at most {b['max_named_share']:.0%} of the campaign's injections" if "max_named_share" in b else "")
         + f"; asked for at least {b['min_per_day']} a day",
         f"- cost: **USD {record['cost_usd']:.3f}** (estimate), tokens {record['tokens']}",
         f"- tool calls: {sum(calls.values())} ({', '.join(f'{k} {v}' for k, v in calls.most_common())})",
         f"- recorded control stream: [attacker-{name}.control.jsonl](attacker-{name}.control.jsonl) ({len(record['commands'])} "
         f"commands); full record with every message: `runs/attacker/{name}/record.json` (gitignored)", "",
         "## What it injected", "",
         f"{len(inj)} injections ({', '.join(f'{d}: {n}' for d, n in sorted(by_day.items()))}).", "",
         "| | count |", "|---|---|",
         *(f"| {k} | {v} |" for k, v in Counter(i["kind"] for i in inj).most_common()),
         *(f"| {k} | {v} |" for k, v in Counter(i["severity"] for i in inj).most_common()),
         *(f"| touches layer {l} | {sum(l in i['layers'] for i in inj)} |" for l in (1, 2, 3)), "",
         *([f"Named share over the campaign, after each injection: "
            + ", ".join(f"{sum(j['kind'] == 'scenario' for j in inj[:n])}/{n}" for n in range(1, len(inj) + 1))
            + f" (cap {b['max_named_share']:.0%}).", ""] if "max_named_share" in b and inj else []),
         "| id | start (UTC) | stop | kind | what | entities | severity | intent |", "|---|---|---|---|---|---|---|---|"]
    for i in inj:
        if i["kind"] == "scenario":
            what = f"{i['scenario']} on {i['target']} ({', '.join(f'{k}={v:g}' for k, v in started.get(i['id'], {}).get('params', {}).items())})"
        else:
            what = "; ".join(f"{e['op']} {e['shape']} {e['magnitude']:g} on {e['series_id']} for {e['duration_min']:g} min"
                             for e in next(c for c in record["commands"] if c.get("id") == i["id"])["effects"])
        stop = i["stopped_at"] or i["stop"]
        L.append(f"| {i['id']} | {i['at'][5:16]} | {stop[5:16]}{' (stopped)' if i['stopped_at'] else ''} | {i['kind']} | {cell(what)} | "
                 f"{cell(', '.join(i['entities']))} | {i['severity']} | {cell(i['intent'])} |")
    generic = [i for i in inj if i["kind"] == "generic"]
    if not generic and "min_gap_minutes" in b:
        L += ["", "## Generic incidents: novelty, and overlap with the named scenarios", "",
              "No generic incident in this run, so there is no novelty text or overlap to report."]
    if generic and "overlap" in generic[0]:
        names = list(generic[0]["overlap"])
        L += ["", "## Generic incidents: novelty, and overlap with the named scenarios (reported, not enforced)", "",
              "Overlap: the incident's series shared with each scenario's series (over all its targets), as "
              "`shared / the incident's series (Jaccard)`.", ""]
        for i in generic:
            L += [f"- **{i['id']}** novelty: {cell(i['novelty'])}"]
        L += ["", "| incident | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        for i in generic:
            o = i["overlap"]
            L.append(f"| {i['id']} | " + " | ".join(f"{o[n]['shared']}/{o[n]['of_incident']} ({o[n]['jaccard']:g})" for n in names) + " |")
    L += ["", "## Rejections (the attacker's own check: the injector's rules, the budget, described series, the gap"
          + (", the named-scenario quota" if "max_named_share" in b else "") + ")", ""]
    if record["rejections"]:
        rule = Counter(r["reasons"][0].split(":")[0] if r["reasons"][0].split(":")[0] in ("budget", "quota") else
                       "partly accepted" if r.get("partly_accepted") else "other" for r in record["rejections"])
        L += [f"{len(record['rejections'])} in all: " + ", ".join(f"{k} {v}" for k, v in rule.most_common()) + ".", ""]
        L += ["| decided at | tool | partly accepted | reasons |", "|---|---|---|---|"]
        L += [f"| {r['at'][5:16]} | {r['tool']} | {'yes' if r.get('partly_accepted') else 'no'} | {cell('; '.join(r['reasons']))} |"
              for r in record["rejections"]]
    else:
        L.append("None.")
    L += ["", "## Replayed through the injector, without the LLM", "",
          f"{len(started)} of {len(inj)} injections started; {len(refused)} commands refused by the injector at the publisher's "
          f"check" + (f" ({'; '.join(r['reasons'][0] for r in refused)})" if refused else "") + "; readings held at a physical "
          f"limit: " + (", ".join(f"{k}: {v}" for k, v in held.items()) or "none") + f". Telemetry messages: {len(sent):,}.", "",
          "## The agent's notes, wake by wake", "",
          "| wake (UTC) | tool calls | commands | note |", "|---|---|---|---|"]
    for w in wakes:
        L.append(f"| {w['now'][5:16]} | {len(w['tool_calls'])} | {', '.join(c['id'] + ('' if c['cmd'] == 'inject' else ' stop') for c in w['commands']) or '-'} | "
                 f"{cell(w['note'] or ('(' + (w['stop'] or 'no note') + ')'))} |")
    out = ROOT / f"docs/results/attacker-{name}.md"
    out.write_text("\n".join(L) + "\n")
    print(f"saved {out.relative_to(ROOT)} and {control.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
