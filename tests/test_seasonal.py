import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.seasonal import seasonal_scores
from tests.helpers import daily_series, last_day


def test_flat_series_has_no_hot_readings():
    s = daily_series(amplitude=0.0, noise=0.0)  # perfectly constant: MAD is 0, only the floor applies
    out = seasonal_scores(s)
    scored = out.dropna(subset=["z"])
    assert len(scored) > 0
    assert (scored.z == 0).all()
    assert not out.hot.any()


def test_noisy_daily_cycle_has_no_hot_readings():
    out = seasonal_scores(daily_series())
    assert not out.hot.any()


def test_median_5_mad_1_boundary():
    # Seven previous days at the same time of day: median 5, deviations [2,1,1,0,1,1,2] -> MAD 1.
    history = [3, 4, 4, 5, 6, 6, 7]
    idx = pd.date_range("2026-01-01", periods=8, freq="1D", tz="UTC")
    s = pd.Series(history + [9.45], index=idx, name="m|e")
    day8 = seasonal_scores(s, threshold=3, floor=0).iloc[-1]  # sees exactly the seven values above
    assert day8.expected == 5 and day8.wobble == pytest.approx(1.4826)
    assert day8.z == pytest.approx(4.45 / 1.4826)  # 3.0015: just over 3
    assert day8.hot and day8.direction == "up"


def test_low_side_and_just_below_threshold():
    history = [3, 4, 4, 5, 6, 6, 7]
    idx = pd.date_range("2026-01-01", periods=8, freq="1D", tz="UTC")
    low = seasonal_scores(pd.Series(history + [0.55], index=idx), threshold=3, floor=0).iloc[-1]
    assert low.z == pytest.approx(-4.45 / 1.4826)  # -3.0015: just under -3
    assert low.hot and low.direction == "down"

    near = seasonal_scores(pd.Series(history + [9.4], index=idx), threshold=3, floor=0).iloc[-1]
    assert near.z == pytest.approx(4.4 / 1.4826)   # 2.968: not hot
    assert not near.hot


def test_floor_limits_score_on_quiet_history():
    # History never moves, so MAD = 0; the 5% floor makes wobble 5 and a +10 move only z = 2.
    idx = pd.date_range("2026-01-01", periods=8, freq="1D", tz="UTC")
    out = seasonal_scores(pd.Series([100] * 7 + [110], index=idx)).iloc[-1]
    assert out.wobble == 5 and out.z == 2 and not out.hot


def test_needs_min_days_of_history():
    out = seasonal_scores(daily_series(days=8), min_days=3)
    assert out.z.iloc[: 3 * 288].isna().all()   # days 1-3 have 0-2 days of history
    assert out.z.iloc[3 * 288:].notna().all()


def test_explanation_is_plain_english():
    s = daily_series(noise=0.0)
    s.iloc[-1] = s.iloc[-1] * 9.4
    row = seasonal_scores(s).iloc[-1]
    assert row.explanation.startswith("test_metric (test-entity) is 9.4x expected for this time of day")


def test_no_leakage_future_changes_do_not_affect_past_scores():
    s = daily_series()
    cut = last_day(s)[100]
    changed = s.copy()
    changed[changed.index > cut] = 1e6
    before = seasonal_scores(s).set_index("ts").loc[:cut]
    after = seasonal_scores(changed).set_index("ts").loc[:cut]
    pd.testing.assert_frame_equal(before, after)
