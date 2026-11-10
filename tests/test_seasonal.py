import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.seasonal import seasonal_scores
from tests.helpers import daily_series, day_of, last_day, weekly_series


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


# --- history modes (5 weeks starting Mon 2026-01-05; the last week is Mon Feb 2 - Sun Feb 8)

MODES = ["previous_days", "same_weekday", "day_type"]
LAST_SATURDAY, LAST_WEDNESDAY = "2026-02-07", "2026-02-04"


def scores_on(s, date, **kwargs):
    return seasonal_scores(s, **kwargs).set_index("ts").loc[day_of(s, date)]


def test_normal_saturday_is_hot_with_previous_days_history():
    # reproduces the bug: 5 of the previous 7 days are weekdays, so "expected" is the weekday level
    sat = scores_on(weekly_series(), LAST_SATURDAY, history="previous_days")
    assert sat.hot.mean() > 0.9
    assert (sat.direction[sat.hot] == "down").all()


@pytest.mark.parametrize("mode", ["same_weekday", "day_type"])
def test_normal_saturday_is_not_hot_with_weekend_aware_history(mode):
    sat = scores_on(weekly_series(), LAST_SATURDAY, history=mode)
    assert not sat.hot.any()
    assert sat.expected.mean() == pytest.approx(40, abs=2)      # the weekend level, not the weekday 100


@pytest.mark.parametrize("mode", MODES)
def test_real_weekday_drop_is_hot_in_every_mode(mode):
    s = weekly_series()
    s[day_of(s, LAST_WEDNESDAY)] *= 0.4
    wed = scores_on(s, LAST_WEDNESDAY, history=mode)
    assert wed.hot.all() and (wed.direction == "down").all()


def test_history_mode_baseline_days():
    s = weekly_series()
    assert (scores_on(s, LAST_SATURDAY, history="previous_days").baseline_days == 7).all()
    assert (scores_on(s, LAST_SATURDAY, history="same_weekday").baseline_days == 4).all()
    assert (scores_on(s, LAST_SATURDAY, history="day_type").baseline_days == 5).all()
    # first Saturday of the data: no earlier weekend day exists
    first_sat = scores_on(s, "2026-01-10", history="day_type")
    assert (first_sat.baseline_days == 0).all() and first_sat.z.isna().all()


def test_day_type_uses_the_most_recent_days_of_the_same_type():
    # Monday's history is the previous Tue-Fri and Mon, never the weekend in between
    s = weekly_series(noise=0.0)
    s[day_of(s, "2026-01-31")] = 1e6                            # Saturday before the last Monday
    s[day_of(s, "2026-02-01")] = 1e6                            # Sunday before the last Monday
    mon = scores_on(s, "2026-02-02", history="day_type")
    assert not mon.hot.any() and (mon.baseline_days == 5).all()


def test_same_weekday_with_one_or_no_prior_week():
    s = weekly_series(weeks=2)
    out = seasonal_scores(s, history="same_weekday")
    week1, week2 = out.iloc[: 7 * 288], out.iloc[7 * 288:]
    assert (week1.baseline_days == 0).all() and week1.z.isna().all() and not week1.hot.any()
    assert (week2.baseline_days == 1).all() and week2.z.notna().all()


@pytest.mark.parametrize("mode", MODES)
def test_explanation_names_the_history_and_its_size(mode):
    row = seasonal_scores(weekly_series(), history=mode).iloc[-1]
    words = {"previous_days": "baseline: 7 values from the previous 7 days",
             "same_weekday": "baseline: 4 values from the same weekday in the previous 4 weeks",
             "day_type": "baseline: 5 values from the last 5 days of the same type (weekday/weekend)"}
    assert words[mode] in row.explanation


@pytest.mark.parametrize("mode", MODES)
def test_no_leakage_in_every_history_mode(mode):
    s = weekly_series()
    cut = day_of(s, LAST_SATURDAY)[100]
    changed = s.copy()
    changed[changed.index > cut] = 1e6
    before = seasonal_scores(s, history=mode).set_index("ts").loc[:cut]
    after = seasonal_scores(changed, history=mode).set_index("ts").loc[:cut]
    pd.testing.assert_frame_equal(before, after)


@pytest.mark.parametrize("mode,full,with_gap", [("same_weekday", 4, 3), ("day_type", 5, 5)])
def test_nan_gap_is_skipped_not_treated_as_zero(mode, full, with_gap):
    s = weekly_series()
    s[day_of(s, "2026-01-31")] = np.nan                         # the Saturday one week earlier is missing
    sat = scores_on(s, LAST_SATURDAY, history=mode)
    # same_weekday loses one value; day_type reaches one day further back to keep 5
    assert (sat.baseline_days == with_gap).all()
    assert sat.expected.mean() == pytest.approx(40, abs=2)      # a gap read as 0 would pull this down
    assert not sat.hot.any()


def test_unknown_history_mode_raises():
    with pytest.raises(ValueError, match="history"):
        seasonal_scores(weekly_series(weeks=1), history="lunar")
