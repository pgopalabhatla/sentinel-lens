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


def test_routed_train_result_is_unchanged():
    assert train_result() == BASELINE
