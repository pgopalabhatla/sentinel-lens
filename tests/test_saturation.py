import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.saturation import saturation_alerts, saturation_scores

GIB = 1024 ** 3
LIMIT = 4 * GIB


def series(values, name="container_memory_working_set_bytes|some-pod"):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="5min", tz="UTC")
    return pd.Series(np.asarray(values, dtype=float), index=idx, name=name)


def noise(n, scale=0.002, seed=0):
    return np.random.default_rng(seed).normal(0, scale * LIMIT, n)


def test_steady_rise_to_the_limit_alerts():
    # one day at 40% of the limit, then a leak of 10% of the limit per hour
    flat = np.full(288, 0.4 * LIMIT)
    climb = 0.4 * LIMIT + 0.1 * LIMIT * np.arange(1, 73) / 12           # 6 hours, reaches 100%
    s = series(np.concatenate([flat, climb]) + noise(360))
    alerts = saturation_alerts(s, LIMIT)
    assert len(alerts) > 0
    first = alerts.iloc[0]
    # hot needs time_to_full < 4 h, i.e. usage above 60%: two hours into the leak, plus persistence
    hours_into_leak = (first.ts - s.index[288]).total_seconds() / 3600
    assert 2.0 <= hours_into_leak <= 2.5
    assert first.direction == "up" and first.time_to_full_h < 4 and first.r2 >= 0.9
    assert first.explanation.startswith("memory rising ~4")             # ~410 MB/h
    assert "projected to reach its 4.0 GiB limit in ~3." in first.explanation and "(R2=" in first.explanation


def test_flat_series_does_not_alert():
    s = series(np.full(600, 0.7 * LIMIT) + noise(600))
    assert len(saturation_alerts(s, LIMIT)) == 0
    assert not saturation_scores(s, LIMIT).hot.any()


def test_perfectly_flat_series_has_r2_zero_not_nan():
    scores = saturation_scores(series(np.full(50, 0.9 * LIMIT)), LIMIT)
    assert (scores.r2.iloc[12:] == 0).all() and not scores.hot.any()


def test_nightly_climb_and_fall_sawtooth_does_not_alert():
    # every night: 20 minutes up from 55% to 95% of the limit, then straight back down
    day = np.full(288, 0.55 * LIMIT)
    day[24:28] = LIMIT * np.array([0.65, 0.75, 0.85, 0.95])
    s = series(np.tile(day, 7) + noise(7 * 288))
    scores = saturation_scores(s, LIMIT)
    assert len(saturation_alerts(s, LIMIT)) == 0
    # the climb is steep and close to the limit; it is the poor straight-line fit that keeps it quiet
    climbing = scores[scores.ts.dt.hour.eq(2) & scores.ts.dt.minute.isin([5, 10, 15])]
    assert (climbing.slope_per_h > 0).all() and (climbing.r2 < 0.9).all()


def test_missing_limit_raises():
    s = series(np.full(50, 1.0))
    for bad in (None, float("nan"), 0, -1):
        with pytest.raises(ValueError, match="limit"):
            saturation_alerts(s, bad)


def test_time_to_full_math_on_a_known_line():
    # exactly 10 units per hour, limit 100: at value 70 the limit is 3 hours away
    values = 10 + 10 * np.arange(0, 100) / 12
    scores = saturation_scores(series(values, name="disk|vol"), limit=100, unit="GB", label="disk").set_index("ts")
    row = scores[np.isclose(scores.value, 70)].iloc[0]
    assert row.fitted == pytest.approx(70) and row.slope_per_h == pytest.approx(10)
    assert row.r2 == pytest.approx(1.0) and row.usage == pytest.approx(0.7)
    assert row.time_to_full_h == pytest.approx(3.0) and row.hot and row.points == 12
    assert row.explanation == "disk rising ~10 GB/h; projected to reach its 100 GB limit in ~3.0 h (R2=1.00)"
    # the same line 1.5 hours earlier: 4.5 hours away, outside the 4 h horizon
    earlier = scores[np.isclose(scores.value, 55)].iloc[0]
    assert earlier.time_to_full_h == pytest.approx(4.5) and not earlier.hot
    # and below min_usage it is never hot, however steep
    assert not scores[scores.value < 50].hot.any()


def test_uses_the_fitted_value_not_the_last_noisy_reading():
    values = 50 + 5 * np.arange(0, 60) / 12
    values[-1] += 30                                              # a spike on the current reading
    last = saturation_scores(series(values, name="disk|vol"), limit=100, unit="GB").iloc[-1]
    assert last.value == pytest.approx(values[-1])
    assert last.fitted < last.value - 15                          # the line is pulled only partly


def test_needs_a_mostly_full_window():
    # a pod that exists for 30 minutes: a perfect steep climb, but only 6 of 12 readings in the window
    values = np.full(60, np.nan)
    values[30:36] = LIMIT * np.linspace(0.5, 0.95, 6)
    s = series(values)
    scores = saturation_scores(s, LIMIT)
    assert scores.fitted.isna().all() and not scores.hot.any()
    assert len(saturation_alerts(s, LIMIT)) == 0
    # with a 30-minute window the same climb can be fitted
    assert saturation_scores(s, LIMIT, window_min=30).hot.any()


def test_falling_series_never_alerts():
    s = series(LIMIT * np.linspace(0.99, 0.6, 100))
    assert not saturation_scores(s, LIMIT).hot.any()


def test_no_future_values_are_used():
    rng_values = 0.5 * LIMIT + 0.05 * LIMIT * np.arange(200) / 12 + noise(200)
    s = series(rng_values)
    changed = s.copy()
    changed.iloc[120:] = LIMIT * 5
    before = saturation_scores(s, LIMIT).iloc[:120]
    after = saturation_scores(changed, LIMIT).iloc[:120]
    pd.testing.assert_frame_equal(before, after)


def test_uneven_series_raises():
    s = series(np.arange(20.0))
    with pytest.raises(ValueError, match="evenly spaced"):
        saturation_scores(s.drop(s.index[5]), limit=100)
