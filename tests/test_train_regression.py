"""Regression check on the generated dataset: the routed system's TRAIN result must not move by accident.

Needs the dataset in data/ (not in git; regenerate with `python -m sentinel_sim.generate --out data --seed 7`).
Skipped when it is missing. Train split only; the test week is never read.
"""
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parents[1] / "data"
pytestmark = pytest.mark.skipif(not (DATA / "metrics").exists(), reason="dataset not generated")

# Routed system as of docs/log.md "Nov 9, seasonal noise fixes": day_type history, count-like series
# need a change of more than 6 units, no per-metric overrides.
BASELINE = {"recall": 0.8, "precision": 0.427, "alerts": 378, "false_positives": 208}


def train_result(**route_settings) -> dict:
    import warnings

    from scripts.score_router import routed_detections
    from sentinel_sim.evaluate import evaluate_detections

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        dets, _ = routed_detections(**route_settings)
    res = evaluate_detections(dets.reset_index(drop=True), DATA, split="train")
    return {k: res[k] for k in BASELINE}


# Router defaults since docs/log.md "Nov 10, router settings": the baseline plus
# METRIC_MIN_CHANGE = {"container_cpu_throttled_ratio": 0.18}. Update only with a log entry saying why.
DEFAULTS = {"recall": 0.8, "precision": 0.608, "alerts": 270, "false_positives": 100}


def test_routed_train_result_with_router_defaults():
    assert train_result() == DEFAULTS


def test_routed_train_result_without_metric_overrides_is_unchanged():
    assert train_result(metric_min_change={}) == BASELINE


# --- correlator: pages on train with the default topology (the given relationships + the tracked extra edges)
def train_pages(**topology_settings) -> int:
    import warnings

    import pandas as pd

    from scripts.score_formulas import current_alerts
    from scripts.score_seasonal import ds
    from sentinel_lens.correlator import build_topology, correlate

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        alerts, _ = current_alerts()
    lo, hi = pd.Timestamp("2026-10-05", tz="UTC"), pd.Timestamp("2026-10-19", tz="UTC")
    alerts = [a for a in alerts if lo <= a.time < hi]
    topology = build_topology(ds.relationships(), ds.series(), **topology_settings)
    return len(correlate(alerts, topology))


def test_default_pages_on_train_equal_the_experiments_real_edges_row():
    # scripts/diagnose_fragmentation.py, row "A1-A6 together": 46 pages; the topology as given: 49
    assert train_pages() == 46
    assert train_pages(extra_edges=()) == 49


def test_no_alerting_or_catalog_entity_is_left_without_a_relationship():
    import warnings

    from scripts.score_formulas import current_alerts
    from scripts.score_seasonal import ds
    from sentinel_lens.correlator import build_topology

    catalog = ds.series()
    topology = build_topology(ds.relationships(), catalog)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        alerts, _ = current_alerts()
    entities = set(catalog.entity_kind + ":" + catalog.entity) | {topology.entity(a.target) for a in alerts}
    assert sorted(e for e in entities if e not in topology.neighbours) == []


def test_extra_edges_do_not_repeat_given_relationships():
    from scripts.score_seasonal import ds
    from sentinel_lens.topology_extra import EXTRA_EDGES

    rel = ds.relationships()
    given = {frozenset([f"{a}:{b}", f"{c}:{d}"]) for a, b, c, d in zip(rel.child_kind, rel.child, rel.parent_kind, rel.parent)}
    assert not [e for e in EXTRA_EDGES if frozenset([f"{e[0]}:{e[1]}", f"{e[3]}:{e[4]}"]) in given]
