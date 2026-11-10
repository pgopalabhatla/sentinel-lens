import numpy as np
import pandas as pd
import pytest

from sentinel_lens.detectors.job_runs import job_run_alerts, job_run_scores

DAY0 = pd.Timestamp("2026-01-01T03:30:00Z")


def run(day, minutes, dag="customer_agg", state="success", finished=True):
    """A daily run starting 03:30 on day `day` and taking `minutes`. Times are strings or None, the
    way DataSource.job_runs returns them."""
    start = DAY0 + pd.Timedelta(days=day)
    end = start + pd.Timedelta(minutes=minutes)
    return {"dag_id": dag, "run_type": "scheduled", "start": start.isoformat(),
            "end": end.isoformat() if finished else None, "state": state}


def frame(*rows):
    return pd.DataFrame(list(rows), columns=["dag_id", "run_type", "start", "end", "state"])


STABLE = [14.0, 15.0, 14.5, 14.2, 14.8, 14.5, 14.4]             # median 14.5


def stable_then(*later, **kwargs):
    rows = [run(i, m) for i, m in enumerate(STABLE)]
    rows += [run(len(STABLE) + i, m, **kwargs) for i, m in enumerate(later)]
    return frame(*rows)


def test_stable_durations_plus_one_slow_run_alerts_once():
    alerts = job_run_alerts(stable_then(28.6))
    assert len(alerts) == 1
    a = alerts.iloc[0]
    assert a.ts == DAY0 + pd.Timedelta(days=7, minutes=28.6)   # the slow run's end time
    assert (a.target, a.rule, a.direction) == ("dag:customer_agg", "job_run_baseline", "slower")
    assert a.expected == 14.5 and a.baseline_runs == 7
    assert a.wobble == pytest.approx(max(1.4826 * 0.3, 0.05 * 14.5))
    assert a.z == pytest.approx((28.6 - 14.5) / a.wobble)
    assert a.explanation == f"customer_agg took 28.6 min; its earlier runs took ~14.5 min (z={a.z:.1f})"


def test_stable_durations_alone_do_not_alert():
    assert len(job_run_alerts(stable_then(14.6))) == 0


def test_fewer_than_min_runs_earlier_runs_gives_no_alert_and_is_counted():
    runs = frame(*[run(i, 14.5) for i in range(4)], run(4, 60.0))     # only 4 earlier runs
    alerts = job_run_alerts(runs, min_runs=5)
    assert len(alerts) == 0
    assert alerts.attrs["skipped_cold_start"] == 5                    # all five runs were unscored
    # with exactly min_runs earlier runs the same slow run is scored and alerts
    runs = frame(*[run(i, 14.5) for i in range(5)], run(5, 60.0))
    alerts = job_run_alerts(runs, min_runs=5)
    assert len(alerts) == 1 and alerts.attrs["skipped_cold_start"] == 5


def test_failed_and_in_progress_runs_are_ignored():
    # a failed 90-minute run and a run still going: neither is scored, neither enters a baseline
    runs = frame(*[run(i, m) for i, m in enumerate(STABLE)],
                 run(7, 90.0, state="failed"), run(8, 90.0, state="running", finished=False), run(9, 14.6))
    scores = job_run_scores(runs)
    assert len(scores) == len(STABLE) + 1
    last = scores.iloc[-1]
    assert last.baseline_runs == 7 and last.expected == 14.5 and not last.hot
    assert len(job_run_alerts(runs)) == 0


def test_baseline_never_includes_the_run_itself_or_later_runs():
    base = stable_then(14.6, 14.4, 14.7)
    changed = base.copy()
    cut = 8                                                            # change runs 8 and 9 only
    for i in range(cut, len(changed)):
        changed.loc[i, "end"] = (pd.Timestamp(changed.start[i]) + pd.Timedelta(minutes=500)).isoformat()
    before, after = job_run_scores(base), job_run_scores(changed)
    pd.testing.assert_frame_equal(before.iloc[:cut], after.iloc[:cut])
    # the run itself: a lone extreme run is scored against the others, not against itself
    slow = job_run_scores(stable_then(100.0)).iloc[-1]
    assert slow.expected == 14.5 and slow.baseline_runs == 7
    assert list(job_run_scores(base).baseline_runs) == list(range(10))


def test_mad_of_zero_falls_back_to_the_floor():
    flat = [run(i, 10.0) for i in range(6)]
    slow = job_run_scores(frame(*flat, run(6, 14.0))).iloc[-1]         # wobble = 5% of 10 = 0.5
    assert slow.wobble == pytest.approx(0.5) and slow.z == pytest.approx(8.0) and slow.hot
    near = job_run_scores(frame(*flat, run(6, 12.0))).iloc[-1]
    assert near.z == pytest.approx(4.0) and not near.hot


def test_much_faster_run_has_direction_faster():
    alerts = job_run_alerts(stable_then(2.0))
    assert len(alerts) == 1
    assert alerts.iloc[0].direction == "faster" and alerts.iloc[0].z < -6


def test_each_dag_has_its_own_baseline():
    rows = [run(i, m) for i, m in enumerate(STABLE)]
    rows += [run(i, 1.5, dag="hourly_check") for i in range(7)]
    rows += [run(8, 14.5, dag="hourly_check")]                         # normal for customer_agg, not here
    alerts = job_run_alerts(frame(*rows))
    assert list(alerts.target) == ["dag:hourly_check"]


def test_empty_input():
    alerts = job_run_alerts(frame())
    assert len(alerts) == 0 and alerts.attrs["skipped_cold_start"] == 0
