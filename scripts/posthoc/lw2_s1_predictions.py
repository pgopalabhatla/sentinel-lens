"""POST-HOC, section 1 of docs/results/live-week-2-posthoc.md: the seven predictions vs the result.

Recomputes each result number from the exported topics with the scorers the runner used
(live_score.score, triage_score.score_needs_human) and its interval functions, then places it against
the range written in docs/live-week-2-protocol.md before the run.

    python scripts/posthoc/lw2_s1_predictions.py
"""
from __future__ import annotations

from lw2_common import TRIAGE_RUNS, campaign_record, load, topology

# (prediction, low, high), copied from the protocol's Predictions table (commit bc1619e)
PREDICTIONS = {
    "detected, overall": (0.65, 0.55, 0.75),
    "detected, generic": (0.65, 0.60, 0.70),
    "detected, subtle": (0.42, 0.30, 0.55),
    "false pages per day": (0.9, 0.4, 1.4),
    "dangerous final answers, sum of 3 runs": (1, 0, 3),
    "true pages correct, mean of 3 runs": (0.70, 0.60, 0.80),
    "total cost, USD": (9.50, 8.0, 11.0),
}


def where(x, lo, hi) -> str:
    return "below" if x < lo else "above" if x > hi else "inside"


def main() -> None:
    from run_live_week_2 import garwood, wilson
    from sentinel_stream import live_score, triage_score

    key, pages, topo = load("answer_key"), load("pages"), topology()
    det = live_score.score(key, pages, topo)
    rows = det["injections"]

    def share(sel):
        g = [r for r in rows if sel(r)]
        k = sum(r["detected"] for r in g)
        return k, len(g), wilson(k, len(g))

    scored = []
    for k in TRIAGE_RUNS:
        recs = load(f"triage-r{k}")
        scored.append(triage_score.score_needs_human(key, pages, [x for x in recs if x.get("type") == "note"],
                                                     [x for x in recs if x.get("type") == "follow_up"], topo))
    correct = [s["final"]["real"]["correct"] for s in scored]
    n_real = scored[0]["real_pages"]
    dangerous = sum(len(s["dangerous"]) for s in scored)
    camp = campaign_record()["cost_usd"]
    triage_costs = [s["cost_total"] or 0 for s in scored]
    lo, hi = garwood(det["false_page_outs"], det["days"])

    ko, no, wo = share(lambda r: True)
    kg, ng, wg = share(lambda r: r["scenario"] == "custom")
    ks, ns, ws = share(lambda r: r["severity"] == "subtle")
    results = {
        "detected, overall": (ko / no, f"{ko}/{no}", f"Wilson {wo}"),
        "detected, generic": (kg / ng, f"{kg}/{ng}", f"Wilson {wg}"),
        "detected, subtle": (ks / ns, f"{ks}/{ns}", f"Wilson {ws}"),
        "false pages per day": (det["false_pages_per_day"], f"{det['false_page_outs']}/{det['days']:g}",
                                f"Garwood {lo:.2f}-{hi:.2f}"),
        "dangerous final answers, sum of 3 runs": (dangerous, f"{dangerous}", "-"),
        "true pages correct, mean of 3 runs": (sum(correct) / len(correct) / n_real, f"{correct} of {n_real}",
                                               f"Wilson {wilson(correct[0], n_real)} (run 1)"),
        "total cost, USD": (camp + sum(triage_costs), f"campaign {camp:.2f} + triage "
                            + " + ".join(f"{c:.2f}" for c in triage_costs), "-"),
    }
    print("| | prediction (range) | result | | vs range | 95% interval |")
    print("|---|---|---|---|---|---|")
    for name, (p, plo, phi) in PREDICTIONS.items():
        x, raw, ci = results[name]
        print(f"| {name} | {p:g} ({plo:g}-{phi:g}) | {x:.2f} | {raw} | {where(x, plo, phi)} | {ci} |")
    print(f"\nfalse page-outs: {[(str(f['time']), f['alerts']) for f in det['false']]}")


if __name__ == "__main__":
    main()
