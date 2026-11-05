"""Score detectors and the RCA agent against ground truth.

Detections: a DataFrame with columns
    ts          when the detector raised it (UTC)
    target      a series_id from the catalog, or an entity like "dag:daily_orders"
Optional columns are ignored.

RCA predictions: {incident_id: {"root_cause_category": ..., "trigger_change_id": ... or None}}
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .replay import load_labels


def _entities_of(target: str, catalog: pd.DataFrame) -> set[str]:
    if "|" in target:
        row = catalog[catalog.series_id == target]
        if row.empty:
            return set()
        r = row.iloc[0]
        return {f"{r.entity_kind}:{r.entity}"}
    return {target}


def evaluate_detections(detections: pd.DataFrame, data_dir: str | Path = "data", split: str = "test",
                        grace: str = "5min", dedup: str = "30min") -> dict:
    """Incident-level recall and time-to-detect; alert-level precision.

    * An incident counts as detected when any detection hits one of its key-signal series or one of its
      affected entities between start - grace and end.
    * Detections are deduplicated per target within `dedup`, then each is classified as
      true (inside an actionable incident), expected (inside a non-actionable one, e.g. a planned burst),
      or false positive.
    """
    incidents, _ = load_labels(data_dir)
    catalog = pd.read_csv(Path(data_dir) / "catalog/series.csv")
    lo, hi = {"train": ("2026-10-05", "2026-10-19"), "test": ("2026-10-19", "2026-10-26"),
              "all": ("2026-10-05", "2026-10-26")}[split]
    incs = [i for i in incidents if i["split"] == split or split == "all"]
    det = detections.copy()
    det["ts"] = pd.to_datetime(det["ts"], utc=True)
    det = det[(det.ts >= pd.Timestamp(lo, tz="UTC")) & (det.ts < pd.Timestamp(hi, tz="UTC"))].sort_values("ts")
    # dedup
    keep, last = [], {}
    for idx, r in det.iterrows():
        if r.target in last and r.ts - last[r.target] < pd.Timedelta(dedup):
            continue
        last[r.target] = r.ts
        keep.append(idx)
    det = det.loc[keep]
    g = pd.Timedelta(grace)

    def matches(i, r):
        a, b = pd.Timestamp(i["start"]) - g, pd.Timestamp(i["end"])
        if not (a <= r.ts <= b):
            return False
        sigs = {s["series_id"] for s in i["key_signals"]}
        return r.target in sigs or bool(_entities_of(r.target, catalog) & set(i["affected_entities"]))

    per_inc, klass = [], []
    for i in incs:
        hits = [r.ts for _, r in det.iterrows() if matches(i, r)]
        ttd = (min(hits) - pd.Timestamp(i["start"])).total_seconds() / 60 if hits else None
        per_inc.append({"incident_id": i["incident_id"], "scenario": i["scenario"], "variant": i["variant"],
                        "actionable": i["actionable"], "detected": bool(hits), "ttd_min": ttd})
    for _, r in det.iterrows():
        m = [i for i in incs if matches(i, r)]
        klass.append("true" if any(i["actionable"] for i in m) else ("expected" if m else "false_positive"))
    det["class"] = klass
    pi = pd.DataFrame(per_inc)
    act = pi[pi.actionable]
    tp, fp = (det["class"] == "true").sum(), (det["class"] == "false_positive").sum()
    return {"recall": round(act.detected.mean(), 3) if len(act) else None,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "median_ttd_min": act.ttd_min.median(),
            "alerts": len(det), "false_positives": int(fp), "expected_alerts": int((det["class"] == "expected").sum()),
            "per_incident": pi, "alerts_table": det}


def evaluate_rca(predictions: dict, data_dir: str | Path = "data", split: str = "test") -> dict:
    """Exact-match accuracy on root-cause category and on the triggering change. Grade free-text
    explanations separately (by hand or with an LLM judge against `root_cause` in incidents.json)."""
    incidents, _ = load_labels(data_dir)
    rows = []
    for i in incidents:
        if split != "all" and i["split"] != split:
            continue
        p = predictions.get(i["incident_id"], {})
        rows.append({"incident_id": i["incident_id"], "scenario": i["scenario"], "novel": i["novel_in_test"],
                     "category_ok": p.get("root_cause_category") == i["root_cause_category"],
                     "change_ok": p.get("trigger_change_id") == i["trigger_change_id"]})
    df = pd.DataFrame(rows)
    return {"category_accuracy": round(df.category_ok.mean(), 3), "change_accuracy": round(df.change_ok.mean(), 3),
            "novel_category_accuracy": round(df[df.novel].category_ok.mean(), 3) if df.novel.any() else None,
            "per_incident": df}
