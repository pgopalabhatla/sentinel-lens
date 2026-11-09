import numpy as np
import pandas as pd

from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts

NAN = np.nan


def minute_series(values, name="restarts_total|orders-producer"):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="1min", tz="UTC")
    return pd.Series(values, index=idx, name=name, dtype=float)


def test_flat_counter_no_alerts():
    out = counter_increase_alerts(minute_series([3, 3, 3, 3, 3]))
    assert len(out) == 0
    assert list(out.columns) == ["ts", "value", "previous", "increase", "explanation"]


def test_single_step_alerts_once_at_the_step():
    s = minute_series([3, 3, 4, 4, 4])
    out = counter_increase_alerts(s)
    assert list(out.ts) == [s.index[2]]
    row = out.iloc[0]
    assert (row.previous, row.value, row.increase) == (3, 4, 1)
    assert row.explanation == "restarts_total for orders-producer rose from 3 to 4"


def test_several_increases_each_alert():
    s = minute_series([0, 1, 1, 3, 3, 4])
    out = counter_increase_alerts(s)
    assert list(out.ts) == [s.index[1], s.index[3], s.index[5]]
    assert list(out.increase) == [1, 2, 1]


def test_reset_to_zero_is_not_an_alert():
    s = minute_series([5, 5, 0, 0, 1])
    out = counter_increase_alerts(s)
    assert list(out.ts) == [s.index[4]]        # only the rise after the reset, not the reset itself


def test_nan_gap_is_never_compared_across():
    # pod exists, disappears, comes back with a higher count: 0 -> NaN -> 2 is not "rose from 0 to 2",
    # and the leading NaN -> 0 is not an increase either
    s = minute_series([NAN, 0, 0, NAN, NAN, 2, 2, 3])
    out = counter_increase_alerts(s)
    assert list(out.ts) == [s.index[7]]
    assert out.iloc[0].previous == 2


def test_above_zero_blip():
    s = minute_series([0, 0, 2, 0, NAN, 0], name="kafka_under_replicated_partitions|kafka")
    out = above_zero_alerts(s)
    assert list(out.ts) == [s.index[2]]
    assert out.iloc[0].explanation == "kafka_under_replicated_partitions for kafka is 2, should be 0"


def test_above_zero_all_healthy():
    assert len(above_zero_alerts(minute_series([0, 0, NAN, 0]))) == 0
