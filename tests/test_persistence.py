import pandas as pd

from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from tests.helpers import daily_series, last_day


def hot_series(flags):
    idx = pd.date_range("2026-01-01", periods=len(flags), freq="5min", tz="UTC")
    return pd.Series(flags, index=idx)


def detect(s):
    out = seasonal_scores(s).set_index("ts")
    return persistent_alerts(out.hot)


# --- persistence on its own

def test_k_of_n_rule():
    h = hot_series([1, 0, 1, 0, 1, 0, 0, 0, 0])            # 3 hot in readings 0-4
    assert list(persistent_alerts(h.astype(bool))) == [h.index[4]]


def test_two_of_five_is_not_enough():
    h = hot_series([1, 0, 0, 1, 0, 0, 0]).astype(bool)
    assert len(persistent_alerts(h)) == 0


def test_missing_readings_count_as_not_hot():
    h = hot_series([True, None, True, None, None])
    assert len(persistent_alerts(h)) == 0


# --- seasonal + persistence on synthetic series

def test_flat_series_no_alerts():
    assert len(detect(daily_series(amplitude=0.0, noise=0.0))) == 0


def test_single_reading_blip_no_alert():
    s = daily_series()
    s[last_day(s)[150]] *= 10
    assert seasonal_scores(s).hot.sum() == 1   # the blip is hot...
    assert len(detect(s)) == 0                 # ...but does not persist


def test_sustained_step_alerts_at_third_hot_reading():
    s = daily_series()
    start = last_day(s)[150]
    s[s.index >= start] *= 2
    alerts = detect(s)
    assert alerts[0] == start + pd.Timedelta("10min")   # readings 0, 1, 2 of the step are hot


def test_slow_drift_alerts_eventually_but_later_than_step():
    s = daily_series()
    day = last_day(s)
    s[day] += pd.Series(range(len(day)), index=day) * 0.5   # +0.5 per reading, +144 by end of day
    alerts = detect(s)
    assert len(alerts) > 0
    # early morning the series is ~100-130, so the 5% floor sets wobble ~5-6.5 and |diff| must pass
    # ~30-39 (z > 6); the ramp gets there after roughly 60-80 readings

    assert alerts[0] >= day[55]
