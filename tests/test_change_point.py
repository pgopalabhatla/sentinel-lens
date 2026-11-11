import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.change_point import change_point_alerts, change_point_scores

INF = float("inf")


def z_series(values, name="trino_query_p95_sec|trino-lakehouse"):
    idx = pd.date_range("2026-01-01", periods=len(values), freq="5min", tz="UTC")
    return pd.Series(np.asarray(values, dtype=float), index=idx, name=name)


def noise(n, seed):
    return np.random.default_rng(seed).normal(0, 1, n)


def shifted(shift, seed, quiet=300, after=200):
    """`quiet` readings of N(0,1) noise, then the same noise around `shift` wobbles."""
    values = noise(quiet + after, seed)
    values[quiet:] += shift
    return z_series(values), quiet


# ------------------------------------------------------------------ the requested behaviours (seeded)
@pytest.mark.parametrize("seed", range(10))
def test_shift_of_2_5_wobbles_alerts_within_about_5_to_6_readings(seed):
    z, start = shifted(2.5, seed)
    alerts = change_point_alerts(z)
    assert len(alerts) > 0 and alerts.ts.iloc[0] >= z.index[start]          # nothing before the shift
    readings_to_alert = z.index.get_loc(alerts.ts.iloc[0]) - start + 1
    assert 4 <= readings_to_alert <= 9                                      # gain of about 2 per reading, h = 10
    assert alerts.direction.iloc[0] == "up"


def test_shift_of_2_5_wobbles_typical_delay():
    delays = []
    for seed in range(200):
        z, start = shifted(2.5, seed)
        delays.append(z.index.get_loc(change_point_alerts(z).ts.iloc[0]) - start + 1)
    assert 5 <= np.median(delays) <= 6


def test_shift_of_1_wobble_alerts_within_about_20_readings():
    delays = []
    for seed in range(200):
        z, start = shifted(1.0, seed)
        alerts = change_point_alerts(z)
        assert len(alerts) > 0 and alerts.ts.iloc[0] >= z.index[start]
        delays.append(z.index.get_loc(alerts.ts.iloc[0]) - start + 1)
    assert 15 <= np.median(delays) <= 25                                    # gain of about 0.5 per reading
    assert max(delays) < 80


@pytest.mark.parametrize("spike", [10.0, 1e6, INF, -INF])
def test_single_large_spike_alone_does_not_alert(spike):
    values = np.zeros(200)
    values[100] = spike
    assert len(change_point_alerts(z_series(values))) == 0
    # with noise around it too
    values = noise(2000, seed=3)
    values[1000] = spike
    assert len(change_point_alerts(z_series(values))) == 0


@pytest.mark.parametrize("seed", range(5))
def test_pure_noise_for_2000_readings_gives_no_alert(seed):
    assert len(change_point_alerts(z_series(noise(2000, seed)))) == 0


def test_reset_after_alert():
    # a constant z of 2.5: gain 2.0 per reading, passes 10 at the 6th reading (12 > 10), then starts again
    z = z_series([2.5] * 18)
    scores = change_point_scores(z)
    assert list(scores.s_up.round(6)[:6]) == [2.0, 4.0, 6.0, 8.0, 10.0, 0.0]        # 10 is not above h; 12 is, then reset
    alerts = change_point_alerts(z)
    assert list(alerts.ts) == [z.index[5], z.index[11], z.index[17]]               # every 6 readings, not every reading
    first = alerts.iloc[0]
    assert (first.total, first.readings, first.direction) == (12.0, 6, "up")
    assert first["shift"] == pytest.approx(2.5)


def test_two_sided():
    up = change_point_alerts(z_series([0.0] * 50 + [2.5] * 20))
    down = change_point_alerts(z_series([0.0] * 50 + [-2.5] * 20))
    assert set(up.direction) == {"up"} and set(down.direction) == {"down"}
    assert list(up.ts) == list(down.ts) and list(up.total) == list(down.total)     # mirror images
    assert down["shift"].iloc[0] == pytest.approx(2.5)                               # size of the move, direction says which way
    # up then down: one alert each way, and the down side is not held back by the earlier up shift
    both = change_point_alerts(z_series([0.0] * 20 + [2.5] * 6 + [0.0] * 20 + [-2.5] * 6))
    assert list(both.direction) == ["up", "down"]


# ------------------------------------------------------------------ other properties
def test_readings_without_z_are_skipped_and_keep_the_totals():
    with_gap = z_series([2.5, 2.5, 2.5, np.nan, np.nan, np.nan, 2.5, 2.5, 2.5])
    scores = change_point_scores(with_gap)
    assert list(scores.s_up[2:6]) == [6.0, 6.0, 6.0, 6.0]                           # unchanged through the gap
    alerts = change_point_alerts(with_gap)
    assert list(alerts.ts) == [with_gap.index[8]] and alerts.readings.iloc[0] == 6  # 6 scored readings, gap not counted
    assert len(change_point_alerts(z_series([np.nan] * 50))) == 0


def test_small_deviations_are_absorbed_by_the_slack():
    assert len(change_point_alerts(z_series([0.4] * 5000))) == 0                    # below k = 0.5: total stays 0
    assert len(change_point_alerts(z_series([0.6] * 150))) == 1                     # just above: 0.1 per reading, 101 readings


def test_clip_limits_what_one_reading_can_add():
    scores = change_point_scores(z_series([100.0, 100.0]), clip=3.0)
    assert list(scores.s_up) == [2.5, 5.0]
    # four huge readings in a row are a level shift, not a spike: they alert (4 x 2.5 = 10 is not above 10, the 5th is)
    assert list(change_point_alerts(z_series([0.0] * 5 + [100.0] * 5)).readings) == [5]


def test_explanation_and_columns():
    alerts = change_point_alerts(z_series([2.5] * 6))
    assert list(alerts.columns) == ["ts", "direction", "total", "shift", "readings", "z", "explanation"]
    assert alerts.explanation.iloc[0] == ("trino_query_p95_sec (trino-lakehouse): level shifted up ~2.5 wobbles "
                                          "over 6 readings; running total passed 10")
    unnamed = change_point_alerts(pd.Series([-2.5] * 6, index=pd.date_range("2026-01-01", periods=6, freq="5min", tz="UTC")))
    assert unnamed.explanation.iloc[0].startswith("level shifted down ~2.5 wobbles")


def test_no_future_values_are_used():
    values = noise(600, seed=1)
    values[300:] += 2.5
    z = z_series(values)
    changed = z.copy()
    changed.iloc[400:] = -50.0
    pd.testing.assert_frame_equal(change_point_scores(z).iloc[:400], change_point_scores(changed).iloc[:400])


def test_parameters_change_the_delay_in_the_expected_direction():
    z = z_series([0.0] * 10 + [2.0] * 60)
    first = lambda **kw: change_point_alerts(z, **kw).readings.iloc[0]              # noqa: E731
    assert first(h=5.0) < first(h=10.0) < first(h=12.0)
    assert first(k=0.25) < first(k=0.5) < first(k=1.0)
    with pytest.raises(ValueError):
        change_point_alerts(z, h=0)
