"""Does the generator's --seed change the incident schedule, or only the noise? Read-only.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/compare_seeds.py            # compares seeds 7 and 8
    python scripts/compare_seeds.py 7 11

Generates both datasets into a temporary directory (never into data/), compares them, and deletes
them. Details are printed for the warm-up and train periods only (before 2026-10-19). For the test
week only counts are printed (how many incidents differ and in which fields, never their contents),
so running this does not reveal test labels.
"""
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TRAIN_END = pd.Timestamp("2026-10-19", tz="UTC")
INCIDENT_FIELDS = ["scenario", "variant", "split", "start", "end", "actionable", "severity", "affected_entities",
                   "key_signals", "trigger_change_id", "root_cause_category", "dag_run_ids", "spark_app_ids"]


def generate(seed: int, out: Path) -> str:
    r = subprocess.run([sys.executable, "-m", "sentinel_sim.generate", "--out", str(out), "--seed", str(seed)],
                       cwd=ROOT, capture_output=True, text=True, check=True)
    return r.stdout.strip().splitlines()[-1]


def jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def before_test(ts) -> bool:
    return pd.Timestamp(ts) < TRAIN_END


def compare_incidents(a: Path, b: Path):
    ia = {i["incident_id"]: i for i in json.loads((a / "labels/incidents.json").read_text())}
    ib = {i["incident_id"]: i for i in json.loads((b / "labels/incidents.json").read_text())}
    print(f"incidents: {len(ia)} vs {len(ib)}; same ids: {sorted(ia) == sorted(ib)}")
    rows, test_diff_fields, test_differing = [], set(), 0
    for iid in sorted(set(ia) & set(ib)):
        x, y = ia[iid], ib[iid]
        diff = [f for f in INCIDENT_FIELDS if x.get(f) != y.get(f)]
        if x["split"] != "train" or y["split"] != "train":
            test_differing += bool(diff)
            test_diff_fields |= set(diff)
            continue
        shift = (pd.Timestamp(y["start"]) - pd.Timestamp(x["start"])).total_seconds() / 60
        dur = lambda i: (pd.Timestamp(i["end"]) - pd.Timestamp(i["start"])).total_seconds() / 60   # noqa: E731
        same_series = ({k["series_id"] for k in x["key_signals"]} == {k["series_id"] for k in y["key_signals"]})
        rows.append({"incident": iid, "scenario": x["scenario"], "same_scenario": x["scenario"] == y["scenario"],
                     "same_day": x["start"][:10] == y["start"][:10], "start_a": x["start"][5:16],
                     "start_shift_min": round(shift, 1), "duration_a_min": round(dur(x), 1),
                     "duration_b_min": round(dur(y), 1),
                     "same_entities": x["affected_entities"] == y["affected_entities"],
                     "same_key_series": same_series, "fields_that_differ": ", ".join(diff) or "none"})
    print("\nTrain incidents, seed A vs seed B:")
    print(pd.DataFrame(rows).to_string(index=False))
    n_test = sum(1 for i in ia.values() if i["split"] != "train")
    print(f"\nTest-week incidents (contents not shown): {n_test} incidents, {test_differing} differ in at least "
          f"one field; fields that differ: {', '.join(sorted(test_diff_fields)) or 'none'}")


def compare_changes(a: Path, b: Path):
    ca = [c for c in jsonl(a / "changes/changes.jsonl") if before_test(c["ts"])]
    cb = [c for c in jsonl(b / "changes/changes.jsonl") if before_test(c["ts"])]
    key = lambda c: (c["ts"], c["kind"], c["target"])   # noqa: E731
    same_what = [(c["kind"], c["target"]) for c in ca] == [(c["kind"], c["target"]) for c in cb]
    print(f"\nchanges before the test week: {len(ca)} vs {len(cb)}; same kind+target sequence: {same_what}; "
          f"identical timestamps: {[key(c) for c in ca] == [key(c) for c in cb]}; "
          f"fully identical: {ca == cb}")


def compare_runs(a: Path, b: Path):
    ra = pd.DataFrame([r for r in jsonl(a / "jobs/dag_runs.jsonl") if before_test(r["scheduled_for"])])
    rb = pd.DataFrame([r for r in jsonl(b / "jobs/dag_runs.jsonl") if before_test(r["scheduled_for"])])
    k = ["dag_id", "run_id"]
    m = ra.merge(rb, on=k, suffixes=("_a", "_b"))
    start_shift = (pd.to_datetime(m.start_b) - pd.to_datetime(m.start_a)).dt.total_seconds().abs()
    dur_a = (pd.to_datetime(m.end_a) - pd.to_datetime(m.start_a)).dt.total_seconds() / 60
    dur_b = (pd.to_datetime(m.end_b) - pd.to_datetime(m.start_b)).dt.total_seconds() / 60
    print(f"DAG runs before the test week: {len(ra)} vs {len(rb)}; same run ids: {len(m) == len(ra) == len(rb)}; "
          f"same state: {int((m.state_a == m.state_b).sum())} of {len(m)}; "
          f"same sla_missed: {int((m.sla_missed_a == m.sla_missed_b).sum())} of {len(m)}")
    print(f"  start times identical: {int((start_shift == 0).sum())} of {len(m)} (median shift "
          f"{start_shift.median():.0f} s, max {start_shift.max() / 60:.1f} min); "
          f"durations identical: {int(np.isclose(dur_a, dur_b).sum())} of {len(m)} "
          f"(median |difference| {(dur_a - dur_b).abs().median():.2f} min, max {(dur_a - dur_b).abs().max():.1f} min)")


def compare_metrics(a: Path, b: Path):
    def load(d):
        df = pd.read_csv(d / "metrics/metrics.csv.gz", index_col=0)
        df.index = pd.to_datetime(df.index, utc=True)
        return df[df.index < TRAIN_END]
    ma, mb = load(a), load(b)
    print(f"\nmetrics before the test week: same series: {list(ma.columns) == list(mb.columns)} ({ma.shape[1]}); "
          f"same timestamps: {ma.index.equals(mb.index)}")
    both = ma.notna() & mb.notna()
    same_nan = (ma.isna() == mb.isna()).to_numpy().mean()
    equal = (np.isclose(ma, mb, equal_nan=True)).mean()
    print(f"  cells where both or neither have a value: {same_nan:.1%}; cells with the same value: {equal:.1%}")
    identical = [c for c in ma.columns if np.allclose(ma[c], mb[c], equal_nan=True)]
    print(f"  series identical in both seeds: {len(identical)} of {ma.shape[1]}")
    if identical:
        print("    " + ", ".join(sorted({c.split('|')[0] for c in identical})))
    corr = pd.Series({c: ma[c][both[c]].corr(mb[c][both[c]]) for c in ma.columns if both[c].sum() > 10})
    rel = pd.Series({c: ((ma[c] - mb[c]).abs().median() / (ma[c].abs().median() or np.nan)) for c in ma.columns})
    print(f"  correlation between seeds per series: median {corr.median():.3f}, lowest {corr.min():.3f}")
    print(f"  median |difference| relative to the series' median level: median over series {rel.median():.1%}")


def compare_counts(a: Path, b: Path, line_a: str, line_b: str):
    print(f"\ngenerator summary A: {line_a}\ngenerator summary B: {line_b}")
    for rel in ["catalog/series.csv", "catalog/topology.json", "labels/incidents.json", "changes/changes.jsonl",
                "jobs/dag_runs.jsonl", "metrics/metrics.csv.gz", "logs/logs.jsonl.gz", "k8s/events.jsonl"]:
        print(f"  {rel:28s} identical files: {sha(a / rel) == sha(b / rel)}")


if __name__ == "__main__":
    seed_a, seed_b = (int(x) for x in sys.argv[1:3]) if len(sys.argv) >= 3 else (7, 8)
    with tempfile.TemporaryDirectory() as tmp:
        a, b = Path(tmp) / f"seed{seed_a}", Path(tmp) / f"seed{seed_b}"
        line_a, line_b = generate(seed_a, a), generate(seed_b, b)
        print(f"Seed A = {seed_a}, seed B = {seed_b} (generated into a temporary directory; data/ is not touched)\n")
        current = ROOT / "data"
        if (current / "metrics/metrics.csv.gz").exists():
            files = ["labels/incidents.json", "changes/changes.jsonl", "jobs/dag_runs.jsonl", "k8s/events.jsonl"]
            same = all(sha(current / f) == sha(a / f) for f in files)
            print(f"data/ equals a fresh seed-{seed_a} run (labels, changes, runs, events by hash): {same}\n")
        compare_incidents(a, b)
        compare_changes(a, b)
        compare_runs(a, b)
        compare_metrics(a, b)
        compare_counts(a, b, line_a, line_b)
