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

From note schema v3 on (`needs_human` instead of `verdict`): `score_needs_human` scores the first and the
final answer (a follow-up's, if there was one) as correct / dangerous / wasted / deferred, and whether the
cause names the injected target.
"""
from __future__ import annotations

from statistics import mean, median

import pandas as pd

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


# ------------------------------------------------------------------------- needs_human (note schema v3)
# A note of the first two runs has `verdict`; from v3 on it has `needs_human`. The old verdicts map
# onto the new answer so that both can be scored the same way: real -> yes, noise -> no, unsure -> unsure.
ANSWER_OF_VERDICT = {"real": "yes", "noise": "no", "unsure": "unsure"}
OUTCOME = {("real", "yes"): "correct", ("real", "no"): "dangerous", ("real", "unsure"): "deferred",
           ("noise", "no"): "correct", ("noise", "yes"): "wasted", ("noise", "unsure"): "deferred"}
OUTCOMES = ("correct", "dangerous", "wasted", "deferred", "no note")


def answer_of(note) -> str | None:
    if not note:
        return None
    return note.get("needs_human") or ANSWER_OF_VERDICT.get(note.get("verdict"))


def outcome(truth: str, answer: str | None) -> str:
    """A true page: yes = correct, no = dangerous (the engineer is told to stand down), unsure = deferred.
    A false page: no = correct, yes = wasted (an engineer is sent for nothing), unsure = deferred."""
    return "no note" if answer is None else OUTCOME[(truth, answer)]


def cause_text(note) -> str:
    if not note:
        return ""
    c = note.get("cause")
    return c.get("description", "") if isinstance(c, dict) else (note.get("suspected_cause") or "")


def text_names_target(text: str, target: str | None) -> bool:
    """Does free text name the target: the name itself, or a pod `<target>-<n>`, as a whole word
    (so "orders-etl" is not found in "orders-etl-consumer")."""
    import re
    if not target or not text:
        return False
    return re.search(rf"(?<![\w-]){re.escape(target)}(?:-\d+)?(?![\w-])", text) is not None


def score_needs_human(answer_key: list, pages: list, notes: list, follow_ups: list, topology, summaries: dict | None = None) -> dict:
    """notes: `note` records of the triage topic; follow_ups: its `follow_up` records. The FINAL answer
    is the follow-up's when there is one, else the first note's."""
    by_id = {n["page_id"]: n for n in notes}
    later = {f["follow_up_of"]: f for f in follow_ups}
    rows = []
    for out in matches(answer_key, pages, topology):
        truth = "real" if out["matched"] else "noise"
        inj = out["matched"][0] if out["matched"] else None
        first, follow = by_id.get(out["page_id"]), later.get(out["page_id"])
        final = follow or first
        a1, a2 = answer_of(first), answer_of(final)
        t_first = first["seconds"] / 60 if first and first.get("seconds") is not None else None
        if follow is not None:
            wait = (pd.Timestamp(follow["as_of"]) - pd.Timestamp(follow["paged_at"])).total_seconds() / 60
            t_final = wait + (follow.get("seconds") or 0) / 60
        else:
            t_final = t_first
        costs = [n.get("cost_usd_estimate") for n in (first, follow) if n and n.get("cost_usd_estimate") is not None]
        rows.append({
            "time": out["time"], "page": out["page"], "page_id": out["page_id"], "truth": truth,
            "injections": [i["id"] for i in out["matched"]], "scenario": inj["scenario"] if inj else None,
            "target": inj["target"] if inj else None, "summary": (summaries or {}).get(inj["scenario"]) if inj else None,
            "first": a1, "follow_up": answer_of(follow), "final": a2,
            "first_outcome": outcome(truth, a1), "final_outcome": outcome(truth, a2),
            "confidence": final.get("confidence") if final else None,
            "cause_category": (final.get("cause") or {}).get("category") if final and isinstance(final.get("cause"), dict) else None,
            "cause": cause_text(final), "cause_names_target": bool(inj) and text_names_target(cause_text(final), inj["target"]),
            "first_cause_names_target": bool(inj) and text_names_target(cause_text(first), inj["target"]),
            "impact": (final.get("impact") or {}).get("degraded") if final and isinstance(final.get("impact"), dict) else None,
            "minutes_to_first_answer": t_first, "minutes_to_final_answer": t_final,
            "cost": sum(costs) if costs else None, "tool_calls": sum(n.get("tool_calls") or 0 for n in (first, follow) if n),
            "evidence_tools": sorted({e.get("tool") for e in final["evidence"] if e.get("tool")}) if final else []})

    def table(key):
        t = {truth: {o: 0 for o in OUTCOMES} for truth in ("real", "noise")}
        for r in rows:
            t[r["truth"]][r[key]] += 1
        return t

    real = [r for r in rows if r["truth"] == "real"]
    finals = [r["minutes_to_final_answer"] for r in rows if r["minutes_to_final_answer"] is not None]
    followed = [r["minutes_to_final_answer"] for r in rows if r["follow_up"] is not None]
    costs = [r["cost"] for r in rows if r["cost"] is not None]
    return {"rows": rows, "first": table("first_outcome"), "final": table("final_outcome"), "page_outs": len(rows),
            "real_pages": len(real), "noise_pages": len(rows) - len(real),
            "dangerous": [r for r in rows if r["final_outcome"] == "dangerous"],
            "wasted": [r for r in rows if r["final_outcome"] == "wasted"],
            "follow_ups": sum(r["follow_up"] is not None for r in rows),
            "cause_names_target": sum(r["cause_names_target"] for r in real),
            "first_cause_names_target": sum(r["first_cause_names_target"] for r in real),
            "minutes_to_final": (median(finals), max(finals)) if finals else None,
            "minutes_to_final_followed": (median(followed), max(followed)) if followed else None,
            "cost_total": sum(costs) if costs else None}


def markdown_needs_human(s: dict, title: str = "") -> str:
    def outcome_table(t):
        out = ["| truth | correct | DANGEROUS | wasted | deferred | no note |", "|---|---|---|---|---|---|"]
        for truth in ("real", "noise"):
            out.append(f"| {truth} ({sum(t[truth].values())}) | " + " | ".join(
                str(t[truth][o]) if o in ("correct", "deferred", "no note") or (o == "dangerous") == (truth == "real") else "-"
                for o in OUTCOMES) + " |")
        return out

    def mins(x):
        return "-" if x is None else f"{x:.1f}"
    L = [f"## {title}Outcomes", "",
         "A true page: needs_human yes = correct, no = **dangerous**, unsure = deferred. A false page: no = correct, "
         "yes = wasted, unsure = deferred. Final = the follow-up's answer when there was one.", "",
         "First answer (at paging time):", "", *outcome_table(s["first"]), "",
         "Final answer:", "", *outcome_table(s["final"]), "",
         f"Follow-ups: {s['follow_ups']}. Dangerous (final): {len(s['dangerous'])}. Wasted (final): {len(s['wasted'])}.", ""]
    if s["dangerous"]:
        L += ["**Dangerous answers** (a true page answered needs_human no):", "",
              "| time (UTC) | page | injection | scenario | cause |", "|---|---|---|---|---|"]
        L += [f"| {r['time']:%b %d %H:%M} | {r['page']} | {', '.join(r['injections'])} | {r['scenario']} | {_cell(r['cause'])} |"
              for r in s["dangerous"]]
        L.append("")
    m, mf = s["minutes_to_final"], s["minutes_to_final_followed"]
    L += [f"Cause names the injected target: {s['cause_names_target']} of {s['real_pages']} true page-outs (final answer; "
          f"first answer: {s['first_cause_names_target']}).", "",
          f"Minutes from paging to the final answer: median {mins(m[0] if m else None)}, max {mins(m[1] if m else None)}; "
          f"for the pages that had a follow-up: median {mins(mf[0] if mf else None)}, max {mins(mf[1] if mf else None)}.", ""]
    L += ["| time (UTC) | page | truth | injection | first | follow-up | final | outcome | cause | names target | impact | min to final |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["rows"]:
        L.append(f"| {r['time']:%b %d %H:%M} | {r['page']} | {r['truth']} | {', '.join(r['injections']) or '-'} | {r['first'] or '-'} | "
                 f"{r['follow_up'] or '-'} | {r['final'] or '-'} | {r['final_outcome']} | {r['cause_category'] or '-'} | "
                 f"{'yes' if r['cause_names_target'] else ('no' if r['truth'] == 'real' else '-')} | "
                 f"{_cell(', '.join(r['impact'] or [])) if r['impact'] else '-'} | {mins(r['minutes_to_final_answer'])} |")
    return "\n".join(L) + "\n"
