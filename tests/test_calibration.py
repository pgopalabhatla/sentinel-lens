import numpy as np
import pandas as pd
import pytest

from sentinel_lens.calibration import abs_floor_from_scale, abs_floor_from_unit


def series(values):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="5min", tz="UTC")
    return pd.Series(values, index=idx, dtype=float)


def test_count_like_units_get_a_floor_of_one():
    for unit in ["queries", "tasks", "queries/min", "count"]:
        assert abs_floor_from_unit(unit) == 1.0
    assert abs_floor_from_unit("queries", count_floor=2.0) == 2.0


def test_other_units_get_no_absolute_floor():
    for unit in ["cores", "bytes", "ratio", "msg/s", "ms", "", "something-new"]:
        assert abs_floor_from_unit(unit) == 0.0


def test_scale_floor_is_a_fraction_of_the_95th_percentile():
    values = np.arange(1, 101)                                   # p95 = 95.05
    assert abs_floor_from_scale(series(values), fraction=0.1) == pytest.approx(9.505)


def test_scale_floor_falls_back_to_the_max_when_the_percentile_is_zero():
    values = [0.0] * 99 + [0.4]                                  # p95 is 0, max is 0.4
    assert abs_floor_from_scale(series(values), fraction=0.5) == pytest.approx(0.2)


def test_scale_floor_is_zero_when_there_is_nothing_to_calibrate_from():
    assert abs_floor_from_scale(series([0.0] * 50)) == 0.0
    assert abs_floor_from_scale(series([np.nan] * 50)) == 0.0


def test_scale_floor_skips_missing_readings():
    assert abs_floor_from_scale(series([np.nan, 10, 10, np.nan, 10]), fraction=0.1) == pytest.approx(1.0)
