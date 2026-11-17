"""Scoring of the triage notes against the answer key (evaluation only).

It reads the answer key, so it is on the injection side (tests/test_live_isolation.py): nothing the
triage agent or the dispatcher loads may import it.

Truth for a page-out comes from the live scorer's own rule (live_score.py, docs/live-protocol.md):
a page-out that matches an injection (inside [start, stop + 30 min], an alert on or 1 hop from a target
entity) is REAL; any other page-out is NOISE. The same functions are used, so the split is exactly the
one in docs/results/live-week-1.md.

For every page-out:
    verdict vs truth                    a 2 x 3 table (truth real | noise  x  verdict real | noise | unsure)
    real called noise                   listed on their own: the costly mistake
    affected_entities vs the target     (a) any entity the answer key lists for the matched injection,
                                        (b) the scenario's named target: an entity with that name, or a pod
                                            whose name is the target followed by "-<n>"
    suspected_cause vs scenario         side by side, for a human to read; not scored
"""
from __future__ import annotations

from statistics import mean, median

from sentinel_stream import live_score


def matches(answer_key: list, pages: list, topology) -> list:
    """Every page-out with the ids of the injections it matches (live_score's rule)."""
    injs, _ = live_score.injections(answer_key)
    outs, _ = live_score.page_outs(pages)
    for inj in injs:
        inj["near"] = frozenset().union(*(topology.within(e, live_score.MAX_HOPS) for e in inj["entities"]))
    for out in outs:
        out["matched"] = [inj for inj in injs
                          if inj["start"] <= out["time"] <= inj["stop"] + live_score.TAIL
                          and any(topology.entity(a["target"]) in inj["near"] for a in out["alerts"])]
    return outs


def names_target(entities: list, target: str) -> bool:
    for e in entities:
        name = e.split(":", 1)[-1]
        if name == target or (e.startswith("pod:") and name.startswith(target + "-") and name[len(target) + 1:].isdigit()):
            return True
    return False


def score(answer_key: list, pages: list, notes: list, topology, summaries: dict | None = None) -> dict:
    """notes: the `note` records of the triage topic. summaries: {scenario: one-line summary}."""
    by_id = {n["page_id"]: n for n in notes}
    outs = matches(answer_key, pages, topology)
    false_by_rule = {(f["time"], f["page"]) for f in live_score.score(answer_key, pages, topology)["false"]}
    rows = []
    for out in outs:
        truth = "real" if out["matched"] else "noise"
        assert (truth == "noise") == ((out["time"], out["page"]) in false_by_rule), "disagrees with live_score"
        note = by_id.get(out["page_id"])
        inj = out["matched"][0] if out["matched"] else None
        ents = note["affected_entities"] if note else []
        rows.append({
            "time": out["time"], "page": out["page"], "page_id": out["page_id"], "truth": truth,
            "injections": [i["id"] for i in out["matched"]],
            "scenario": inj["scenario"] if inj else None, "target": inj["target"] if inj else None,
            "params": inj["params"] if inj else None, "summary": (summaries or {}).get(inj["scenario"]) if inj else None,
            "first_alert": out["alerts"][0]["target"] if out["alerts"] else None,
            "note": note is not None, "verdict": note["verdict"] if note else None,
            "confidence": note["confidence"] if note else None, "valid": note.get("valid") if note else None,
            "suspected_cause": note["suspected_cause"] if note else None, "affected_entities": ents,
            "evidence_tools": sorted({e.get("tool") for e in note["evidence"] if e.get("tool")}) if note else [],
            "answer_key_entity_named": bool(inj) and any(e in set(inj["entities"]) for e in ents),
            "target_named": bool(inj) and names_target(ents, inj["target"]),
            "cost": note.get("cost_usd_estimate") if note else None, "seconds": note.get("seconds") if note else None,
            "tool_calls": note.get("tool_calls") if note else None, "tokens": note["tokens"]["total"] if note else None,
            "budget_exhausted": note.get("budget_exhausted") if note else None})

    table = {t: {v: 0 for v in ("real", "noise", "unsure", "no note")} for t in ("real", "noise")}
    for r in rows:
        table[r["truth"]][r["verdict"] or "no note"] += 1
    real = [r for r in rows if r["truth"] == "real"]
    noted = [r for r in rows if r["note"]]
    costs = [r["cost"] for r in noted if r["cost"] is not None]
    secs = [r["seconds"] for r in noted if r["seconds"] is not None]
    return {"rows": rows, "table": table, "page_outs": len(rows), "notes": len(noted),
            "valid": sum(bool(r["valid"]) for r in noted),
            "real_called_noise": [r for r in real if r["verdict"] == "noise"],
            "noise_called_real": [r for r in rows if r["truth"] == "noise" and r["verdict"] == "real"],
            "real_pages": len(real), "answer_key_entity_named": sum(r["answer_key_entity_named"] for r in real),
            "target_named": sum(r["target_named"] for r in real),
            "cost_total": sum(costs) if costs else None, "cost_per_page": (mean(costs), median(costs), max(costs)) if costs else None,
            "seconds_per_page": (mean(secs), median(secs), max(secs)) if secs else None,
            "tool_calls_per_page": (mean(r["tool_calls"] for r in noted), max(r["tool_calls"] for r in noted)) if noted else None,
            "tokens_total": sum(r["tokens"] for r in noted),
            "budget_exhausted": {k: sum(r["budget_exhausted"] == k for r in noted) for k in ("tool_calls", "time", "tokens")},
            "span": (min(r["time"] for r in rows), max(r["time"] for r in rows)) if rows else None}


def _cell(x) -> str:
    return live_score.cell("-" if x is None else x).replace("\n", " ")


def markdown(s: dict) -> str:
    t = s["table"]
    lines = ["## Verdict against truth", "",
             "Truth: a page-out is real when it matches an injection under the live-week rule, noise otherwise.", "",
             "| truth \\ verdict | real | noise | unsure | no note |", "|---|---|---|---|---|"]
    for truth in ("real", "noise"):
        lines.append(f"| {truth} ({sum(t[truth].values())}) | {t[truth]['real']} | {t[truth]['noise']} | {t[truth]['unsure']} | "
                     f"{t[truth]['no note']} |")
    rcn = s["real_called_noise"]
    lines += ["", f"**Real called noise: {len(rcn)} of {s['real_pages']} real page-outs.**", ""]
    if rcn:
        lines += ["| time (UTC) | page | injection | scenario | confidence | suspected cause | evidence from |", "|---|---|---|---|---|---|---|"]
        lines += [f"| {r['time']:%b %d %H:%M} | {r['page']} | {', '.join(r['injections'])} | {r['scenario']} | {r['confidence']} | "
                  f"{_cell(r['suspected_cause'])} | {_cell(', '.join(r['evidence_tools']))} |" for r in rcn]
    nr = s["noise_called_real"]
    lines += ["", f"Noise called real: {len(nr)} of {s['page_outs'] - s['real_pages']} noise page-outs.", "",
              "## Does the note name the injected target?", "",
              "Over the real page-outs. (a) any of the entities the answer key lists for the matched injection "
              "(the owners of the series it changed, plus the DAG for a pause); (b) the scenario's named target itself "
              "(an entity with that name, or a pod named `<target>-<n>`).", "",
              "| real page-outs | (a) names an answer-key entity | (b) names the target |", "|---|---|---|",
              f"| {s['real_pages']} | {s['answer_key_entity_named']} | {s['target_named']} |", "",
              "## Cost and time", ""]
    if s["cost_per_page"]:
        c, sec, tc = s["cost_per_page"], s["seconds_per_page"], s["tool_calls_per_page"]
        lines += ["| | total | per page: mean | median | max |", "|---|---|---|---|---|",
                  f"| cost (USD, estimate) | {s['cost_total']:.3f} | {c[0]:.4f} | {c[1]:.4f} | {c[2]:.4f} |",
                  f"| time (s, one investigation) | {sum(r['seconds'] or 0 for r in s['rows']):.0f} | {sec[0]:.1f} | {sec[1]:.1f} | {sec[2]:.1f} |",
                  f"| tool calls | {sum(r['tool_calls'] or 0 for r in s['rows'])} | {tc[0]:.1f} | - | {tc[1]} |", "",
                  f"Tokens over all investigations: {s['tokens_total']:,}. Notes: {s['notes']} of {s['page_outs']} page-outs, "
                  f"{s['valid']} valid. Budget used up: {s['budget_exhausted']['tool_calls']} by tool calls, "
                  f"{s['budget_exhausted']['time']} by time, {s['budget_exhausted']['tokens']} by tokens."]
    lines += ["", "## Per page-out", "",
              "| time (UTC) | page | truth | injection | verdict | conf. | affected entities | (a) | (b) | tool calls | s | USD | evidence from |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["rows"]:
        lines.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['truth']} | {_cell(', '.join(r['injections']) or '-')} | "
                     f"{_cell(r['verdict'])} | {_cell(r['confidence'])} | {_cell(', '.join(r['affected_entities']))} | "
                     f"{'yes' if r['answer_key_entity_named'] else ('no' if r['truth'] == 'real' else '-')} | "
                     f"{'yes' if r['target_named'] else ('no' if r['truth'] == 'real' else '-')} | {_cell(r['tool_calls'])} | "
                     f"{_cell(r['seconds'])} | {_cell(r['cost'])} | {_cell(', '.join(r['evidence_tools']))} |")
    lines += ["", "## Suspected cause next to what was injected (to read, not scored)", ""]
    for r in s["rows"]:
        what = (f"**{', '.join(r['injections'])}: {r['scenario']}** on `{r['target']}` "
                f"({', '.join(f'{k}={v:g}' for k, v in (r['params'] or {}).items())}). {r['summary'] or ''}"
                if r["truth"] == "real" else "**nothing injected** (a page-out of the quiet world)")
        lines += [f"### {r['time']:%a %b %d %H:%M} - page {r['page']} - verdict {r['verdict']} ({r['confidence']})", "",
                  f"- Injected: {what}", f"- First alert of the page: `{r['first_alert']}`",
                  f"- Note's suspected cause: {r['suspected_cause'] or '(no note)'}",
                  f"- Note's affected entities: {', '.join(f'`{e}`' for e in r['affected_entities']) or '-'}", ""]
    return "\n".join(lines)
