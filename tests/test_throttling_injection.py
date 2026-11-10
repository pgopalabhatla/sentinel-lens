"""Injection test on a synthetic throttling series: the router's minimum change for
container_cpu_throttled_ratio must still let a real throttling jump through.

Synthetic data only (no files): a ratio that sits at 0.003 with small noise, as on a healthy
long-running pod, at 5-minute steps. The detector is run the way the routed system runs it:
seasonal_scores with the router's history and absolute floor, then 3-of-5 persistence.
"""
import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import route_series

METRIC = "container_cpu_throttled_ratio"
JUMP_START = pd.Timestamp("2026-01-21T12:00:00Z")          # a Wednesday, in the third week of data
JUMP_END = JUMP_START + pd.Timedelta(hours=3)


def router_settings() -> pd.Series:
    cat = pd.DataFrame([{"series_id": f"{METRIC}|some-pod", "metric": METRIC, "unit": "ratio",
                         "default_detector": "seasonal_baseline"}])
    return route_series(cat).iloc[0]


def throttling_series(seed: int = 0) -> pd.Series:
    """Three weeks from Mon 2026-01-05: 0.003 with noise of about a tenth of that, never below 0."""
    idx = pd.date_range("2026-01-05", periods=21 * 288, freq="5min", tz="UTC")
    rng = np.random.default_rng(seed)
    return pd.Series(np.clip(0.003 + rng.normal(0, 0.0003, len(idx)), 0, None), index=idx,
                     name=f"{METRIC}|some-pod")


def alerts(s: pd.Series, abs_floor=None) -> pd.DatetimeIndex:
    r = router_settings()
    floor = r.abs_floor if abs_floor is None else abs_floor
    out = seasonal_scores(s, history=r.history, abs_floor=floor).set_index("ts")
    return persistent_alerts(out.hot)


def with_level(s: pd.Series, level: float, start=JUMP_START, end=JUMP_END) -> pd.Series:
    s = s.copy()
    s[(s.index >= start) & (s.index < end)] = level
    return s


def test_router_gives_throttling_a_min_change_of_0_18():
    r = router_settings()
    assert r.min_change == 0.18 and r.abs_floor == pytest.approx(0.03)


def test_healthy_series_never_alerts():
    assert len(alerts(throttling_series())) == 0


@pytest.mark.parametrize("level", [0.45, 0.25])
def test_throttling_jump_for_three_hours_alerts(level):
    found = alerts(with_level(throttling_series(), level))
    assert len(found) > 0
    assert found[0] == JUMP_START + pd.Timedelta("10min")       # third hot 5-minute reading
    assert ((found >= JUMP_START) & (found < JUMP_END + pd.Timedelta("20min"))).all()   # and nothing else


def test_blip_to_0_01_does_not_alert():
    s = throttling_series()
    one_reading = with_level(s, 0.01, end=JUMP_START + pd.Timedelta("5min"))
    three_hours = with_level(s, 0.01)
    assert len(alerts(one_reading)) == 0
    assert len(alerts(three_hours)) == 0
    # the minimum change is what keeps it quiet: without an absolute floor the same 0.007 rise alerts
    assert len(alerts(three_hours, abs_floor=0.0)) > 0


def test_rise_just_under_and_over_the_min_change():
    s = throttling_series()
    assert len(alerts(with_level(s, 0.003 + 0.15))) == 0        # under 0.18: ignored
    assert len(alerts(with_level(s, 0.003 + 0.21))) > 0         # over 0.18: alerts
