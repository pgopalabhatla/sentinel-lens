"""POST-HOC, section 4 of docs/results/live-week-2-posthoc.md: "cause names the target" on named true pages.

For each named true page and each triage run: the answer-key target, the final note's affected_entities,
the pre-registered rule on the cause text (triage_score.text_names_target), the same name rule on the
entities (triage_score.names_target), and the cause text. Also when each change record was published
relative to its timestamp (the atk-007 finding).

    python scripts/posthoc/lw2_s4_target_naming.py
"""
from __future__ import annotations

from lw2_common import TRIAGE_RUNS, load, topology


def main() -> None:
    from sentinel_stream import triage_score
    key, pages, topo = load("answer_key"), load("pages"), topology()
    named_true = {}
    for out in triage_score.matches(key, pages, topo):
        if out["matched"] and out["matched"][0]["scenario"] != "custom":
            named_true[out["page"]] = out["matched"][0]
    in_entities = in_cause = total = 0
    for k in TRIAGE_RUNS:
        recs = load(f"triage-r{k}")
        notes = {r["page"]: r for r in recs if r.get("type") == "note"}
        follow = {r["page"]: r for r in recs if r.get("type") == "follow_up"}
        for page, inj in sorted(named_true.items()):
            final = follow.get(page) or notes[page]
            ents = final.get("affected_entities") or []
            cause = triage_score.cause_text(final)
            by_ents = triage_score.names_target(ents, inj["target"])
            by_cause = triage_score.text_names_target(cause, inj["target"])
            total, in_entities, in_cause = total + 1, in_entities + by_ents, in_cause + by_cause
            print(f"run {k} page {page} {inj['id']} {inj['scenario']} target={inj['target']} "
                  f"final={'follow-up' if page in follow else 'note'} answer={final.get('needs_human')}")
            print(f"    entities: {ents}  -> names target: {by_ents}")
            print(f"    cause rule: {by_cause}; cause: {cause}")
    print(f"\ntarget named: in the cause text {in_cause} of {total}; in affected_entities {in_entities} of {total}")
    print("\nchange records: timestamp vs published on the stream")
    for r in load("telemetry"):
        if r["type"] == "changes":
            for c in r["changes"]:
                print(f"  {c['ts']}  published {r['ts']}  {c['kind']} {c['target']}: {c['description'][:60]}")
    print("\npage-out times:", [(p["page"], p["emitted_at"]) for p in pages if p["kind"] == "paged"])


if __name__ == "__main__":
    main()
