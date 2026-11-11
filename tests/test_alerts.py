import math

import numpy as np
import pandas as pd
import pytest

from sentinel_lens.alerts import (Alert, alerts_from_above_zero, alerts_from_change_point, alerts_from_counter,
                                  alerts_from_job_runs,
                                  alerts_from_saturation, alerts_from_schedule, alerts_from_seasonal,
                                  exponential_score, linear_score, log_score, percentile_formula, ratio_formula,
                                  rescore, severity_score, to_frame, DEFAULT_FORMULA, GUARDRAIL_DETECTORS)
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.job_runs import job_run_alerts
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.saturation import saturation_alerts, saturation_scores
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from tests.helpers import daily_series, last_day

INF = float("inf")


# ------------------------------------------------------------------ formula boundaries
# r = measure / alert line. Each row: formula, score at r=1, at r=2, at a very large r.
@pytest.mark.parametrize("formula,at_line,at_twice,far_out", [
    (linear_score, 0.5, 1.0, 1.0),                      # (1) min(1, r/2)
    (exponential_score, 0.0, 1 - math.exp(-1), 1.0),    # (2) 1 - exp(-(r-1))
    (log_score, 0.0, 0.5, 1.0),                         # (3) min(1, ln r / ln 4)
])
def test_ratio_formula_boundaries(formula, at_line, at_twice, far_out):
    assert formula(6.0, 6.0, "seasonal") == pytest.approx(at_line)        # r = 1
    assert formula(12.0, 6.0, "seasonal") == pytest.approx(at_twice)      # r = 2
    assert formula(6e9, 6.0, "seasonal") == pytest.approx(far_out)        # very large r
    assert formula(INF, 6.0, "seasonal") == 1.0                           # z can be infinite when wobble is 0


@pytest.mark.parametrize("formula", [linear_score, exponential_score, log_score])
def test_ratio_formulas_stay_in_range_and_never_decrease(formula):
    rs = [0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 10.0, 1e6, INF]
    scores = [formula(r, 1.0, "any") for r in rs]
    assert all(0.0 <= s <= 1.0 for s in scores)
    assert scores == sorted(scores)


def test_log_formula_reaches_one_at_four_times_the_line():
    assert log_score(3.99, 1.0, "x") < 1.0 and log_score(4.0, 1.0, "x") == pytest.approx(1.0)


def test_below_the_line_formulas_two_and_three_give_zero():
    assert exponential_score(3.0, 6.0, "x") == 0.0 and log_score(3.0, 6.0, "x") == 0.0
    assert linear_score(3.0, 6.0, "x") == 0.25


def test_ratio_formula_rejects_bad_input():
    with pytest.raises(ValueError, match="line"):
        linear_score(1.0, 0.0, "x")
    with pytest.raises(ValueError, match="missing"):
        linear_score(float("nan"), 6.0, "x")


def test_percentile_formula_boundaries():
    # reference: 100 quiet readings with measures 0.00 .. 0.99 (as if r = measure / 1)
    formula = percentile_formula({"seasonal": np.arange(100) / 100})
    assert formula(0.0, 1.0, "seasonal") == 0.0                 # nothing in the reference is smaller
    assert formula(0.5, 1.0, "seasonal") == pytest.approx(0.5)
    assert formula(1.0, 1.0, "seasonal") == 1.0                 # r = 1: beyond everything seen
    assert formula(2.0, 1.0, "seasonal") == 1.0                 # r = 2
    assert formula(1e9, 1.0, "seasonal") == 1.0 and formula(INF, 1.0, "seasonal") == 1.0


def test_percentile_formula_handles_infinite_and_missing_reference_values():
    formula = percentile_formula({"seasonal": [1.0, 2.0, INF, float("nan")]})    # NaN is dropped
    assert formula(1.5, 6.0, "seasonal") == pytest.approx(1 / 3)
    assert formula(INF, 6.0, "seasonal") == pytest.approx(2 / 3)                 # ties are not "smaller"


def test_percentile_formula_needs_a_reference_for_the_detector():
    formula = percentile_formula({"seasonal": [1.0, 2.0]})
    with pytest.raises(KeyError, match="counter_rule"):
        formula(1.0, 1.0, "counter_rule")


def test_yes_no_rule_that_never_fired_in_the_reference_scores_one():
    formula = percentile_formula({"failed_run": [0.0] * 50})
    assert formula(1.0, 1.0, "failed_run") == 1.0


# ------------------------------------------------------------------ the default: severity
def test_severity_is_the_default_formula():
    assert DEFAULT_FORMULA is severity_score


@pytest.mark.parametrize("detector", ["seasonal", "seasonal fallback", "job_run_baseline", "saturation"])
def test_severity_boundaries_for_non_guardrail_detectors(detector):
    assert severity_score(6.0, 6.0, detector) == 0.5                                        # r = 1: at the line
    assert severity_score(12.0, 6.0, detector) == pytest.approx(0.5 + 0.5 * (1 - math.exp(-1)))   # r = 2: 0.816
    assert severity_score(18.0, 6.0, detector) == pytest.approx(0.5 + 0.5 * (1 - math.exp(-2)))   # r = 3: 0.932
    for huge in (6e3, 6e9, INF):                                                            # very large r
        score = severity_score(huge, 6.0, detector)
        assert 0.999999 < score < 1.0                                                       # never exactly 1.0
    assert severity_score(3.0, 6.0, detector) == 0.5                                        # below the line: clamped


def test_severity_never_decreases_and_stays_in_half_open_range():
    rs = [0.0, 1.0, 1.01, 1.5, 2.0, 3.0, 5.0, 10.0, 40.0, 1e6, INF]
    scores = [severity_score(r, 1.0, "seasonal") for r in rs]
    assert scores == sorted(scores) and all(0.5 <= s < 1.0 for s in scores)
    assert scores[2] > 0.5                                                                  # rises right after the line


@pytest.mark.parametrize("detector", ["counter_rule", "above_zero", "start_late", "sla_breach", "failed_run"])
def test_guardrail_detectors_score_exactly_one(detector):
    assert detector in GUARDRAIL_DETECTORS
    for measure in (1.0, 2.0, 16.0, 1e9):
        assert severity_score(measure, 1.0, detector) == 1.0


def test_only_guardrails_can_reach_exactly_one():
    assert severity_score(INF, 6.0, "seasonal") < 1.0 == severity_score(1.0, 1.0, "failed_run")


def test_severity_rejects_bad_input_for_non_guardrails():
    with pytest.raises(ValueError, match="line"):
        severity_score(1.0, 0.0, "seasonal")
    with pytest.raises(ValueError, match="missing"):
        severity_score(float("nan"), 6.0, "seasonal")


# ------------------------------------------------------------------ the record
def an_alert(**changes):
    fields = dict(time="2026-01-01T00:00:00Z", target="m|e", detector="seasonal", score=0.5, direction="up",
                  explanation="something happened", measure=6.0, line=6.0)
    return Alert(**{**fields, **changes})


def test_alert_validates_its_fields():
    assert an_alert().time == pd.Timestamp("2026-01-01", tz="UTC")
    assert an_alert(time="2026-01-01 00:00").time.tzinfo is not None      # naive times are taken as UTC
    for bad in (-0.1, 1.1, float("nan")):
        with pytest.raises(ValueError, match="score"):
            an_alert(score=bad)
    with pytest.raises(ValueError, match="direction"):
        an_alert(direction="slower")
    with pytest.raises(ValueError, match="explanation"):
        an_alert(explanation="")


def test_to_frame_has_what_the_scorer_needs():
    df = to_frame([an_alert(), an_alert(target="dag:x", detector="failed_run")])
    assert list(df.columns)[:3] == ["ts", "target", "rule"]
    assert list(df.rule) == ["seasonal", "failed_run"] and str(df.ts.dtype).startswith("datetime64")
    assert list(to_frame([]).columns) == list(df.columns)


def test_rescore_changes_only_the_score():
    a = an_alert(measure=12.0, score=1.0)
    b = rescore([a], log_score)[0]
    assert b.score == pytest.approx(0.5)
    assert (b.time, b.target, b.detector, b.direction, b.explanation, b.measure, b.line) == \
           (a.time, a.target, a.detector, a.direction, a.explanation, a.measure, a.line)


def test_formula_is_pluggable():
    always_one = ratio_formula(lambda r: 1.0)
    s = pd.Series([3.0, 3, 4, 4], index=pd.date_range("2026-01-01", periods=4, freq="1min", tz="UTC"), name="c|p")
    assert alerts_from_counter(counter_increase_alerts(s), "c|p", formula=always_one)[0].score == 1.0
    assert alerts_from_counter(counter_increase_alerts(s), "c|p", formula=log_score)[0].score == 0.0


# ------------------------------------------------------------------ adapters
def test_seasonal_adapter_fires_exactly_when_persistence_does():
    s = daily_series()
    start = last_day(s)[150]
    s[s.index >= start] *= 2
    scores = seasonal_scores(s)
    alerts = alerts_from_seasonal(scores, target=s.name)
    assert [a.time for a in alerts] == list(persistent_alerts(scores.set_index("ts").hot))
    first = alerts[0]
    assert first.time == start + pd.Timedelta("10min")
    assert (first.target, first.detector, first.direction, first.line) == (s.name, "seasonal", "up", 6.0)
    assert first.measure > 6.0 and 0.5 < first.score < 1.0
    assert first.explanation.startswith("test_metric (test-entity) is 2.0x expected")


def test_seasonal_adapter_uses_the_worst_hot_reading_in_the_persistence_window():
    idx = pd.date_range("2026-01-01", periods=8, freq="5min", tz="UTC")
    z = [0.0, 7.0, -20.0, 1.0, 8.0, 0.5, 0.5, 0.5]            # three hot readings: 7, -20, 8
    scores = pd.DataFrame({"ts": idx, "z": z, "hot": [abs(v) > 6 for v in z],
                           "direction": ["up" if v > 0 else "down" for v in z],
                           "explanation": [f"reading {i}" for i in range(8)]})
    alerts = alerts_from_seasonal(scores, target="m|e", detector="seasonal fallback")
    # persistence fires at readings 4 and 5 (3 of the last 5 hot); reading 6's window has only 2
    assert [a.time for a in alerts] == list(idx[4:6])
    # both windows still contain the -20 reading: its measure, direction and explanation are used
    assert [a.measure for a in alerts] == [20.0, 20.0]
    assert {a.direction for a in alerts} == {"down"} and {a.explanation for a in alerts} == {"reading 2"}
    assert alerts[1].detector == "seasonal fallback"
    # the alert at reading 5 is not hot itself and still carries a measure above the line
    assert not scores.hot[5] and alerts[1].measure / alerts[1].line > 1


def test_seasonal_adapter_ignores_large_z_on_readings_that_are_not_hot():
    idx = pd.date_range("2026-01-01", periods=5, freq="5min", tz="UTC")
    scores = pd.DataFrame({"ts": idx, "z": [7.0, 8.0, np.nan, 9.0, 0.0], "hot": [True, True, False, True, False],
                           "direction": ["up"] * 5, "explanation": ["e"] * 5})
    assert [a.measure for a in alerts_from_seasonal(scores, "m|e")] == [9.0, 9.0]


def test_seasonal_adapter_handles_infinite_z():
    idx = pd.date_range("2026-01-01", periods=3, freq="5min", tz="UTC")
    scores = pd.DataFrame({"ts": idx, "z": [INF, INF, INF], "hot": [True] * 3, "direction": ["up"] * 3,
                           "explanation": ["e"] * 3})
    for formula in (linear_score, exponential_score, log_score):
        assert alerts_from_seasonal(scores, "m|e", formula=formula)[0].score == 1.0
    assert 0.999999 < alerts_from_seasonal(scores, "m|e")[0].score < 1.0      # default: just below 1


def test_counter_adapter():
    s = pd.Series([3.0, 3, 4, 4, 6], index=pd.date_range("2026-01-01", periods=5, freq="1min", tz="UTC"),
                  name="restarts_total|orders-producer")
    alerts = alerts_from_counter(counter_increase_alerts(s), target=s.name)
    assert [(a.time, a.measure, a.line) for a in alerts] == [(s.index[2], 1.0, 1.0), (s.index[4], 2.0, 1.0)]
    assert [a.score for a in alerts] == [1.0, 1.0]                   # guardrail: always 1.0
    assert [a.score for a in alerts_from_counter(counter_increase_alerts(s), s.name, formula=linear_score)] == [0.5, 1.0]
    assert alerts[0].detector == "counter_rule" and alerts[0].direction == "up"
    assert alerts[0].explanation == "restarts_total for orders-producer rose from 3 to 4"


def test_above_zero_adapter():
    s = pd.Series([0.0, 4, 0], index=pd.date_range("2026-01-01", periods=3, freq="1min", tz="UTC"),
                  name="kafka_under_replicated_partitions|kafka")
    (a,) = alerts_from_above_zero(above_zero_alerts(s), target=s.name)
    assert (a.time, a.detector, a.measure, a.line, a.direction, a.score) == (s.index[1], "above_zero", 4.0, 1.0, "up", 1.0)


def test_schedule_adapter_covers_all_three_rules():
    expected = pd.DataFrame([{"dag_id": "a", "scheduled_for": pd.Timestamp("2026-01-01T02:00Z"), "sla_minutes": 90},
                             {"dag_id": "b", "scheduled_for": pd.Timestamp("2026-01-01T02:00Z"), "sla_minutes": 30}])
    runs = pd.DataFrame([
        {"dag_id": "b", "run_type": "scheduled", "scheduled_for": "2026-01-01T02:00:00Z",
         "start": "2026-01-01T02:00:10Z", "end": "2026-01-01T03:00:00Z", "state": "failed"}])
    frame = pd.concat([schedule_alerts(expected, runs, "2026-01-02T00:00:00Z"), failed_run_alerts(runs)])
    alerts = alerts_from_schedule(frame)
    assert sorted((a.detector, a.target) for a in alerts) == [("failed_run", "dag:b"), ("sla_breach", "dag:b"),
                                                              ("start_late", "dag:a")]
    assert all((a.measure, a.line, a.direction) == (1.0, 1.0, "up") for a in alerts)
    # all three schedule rules are guardrails
    assert {a.detector: a.score for a in alerts} == {"failed_run": 1.0, "sla_breach": 1.0, "start_late": 1.0}
    late = next(a for a in alerts if a.detector == "start_late")
    assert late.time == pd.Timestamp("2026-01-01T02:10Z") and "no run had started by 02:10" in late.explanation


def test_job_run_adapter_maps_slower_and_faster():
    def run(day, minutes):
        start = pd.Timestamp("2026-01-01T03:30Z") + pd.Timedelta(days=day)
        return {"dag_id": "customer_agg", "run_type": "scheduled", "start": start.isoformat(),
                "end": (start + pd.Timedelta(minutes=minutes)).isoformat(), "state": "success"}
    durations = [14.0, 15.0, 14.5, 14.2, 14.8, 14.5, 14.4, 28.6, 14.5, 2.0]
    frame = job_run_alerts(pd.DataFrame([run(i, m) for i, m in enumerate(durations)]))
    slow, fast = alerts_from_job_runs(frame)
    assert (slow.detector, slow.target, slow.direction, slow.line) == ("job_run_baseline", "dag:customer_agg", "up", 6.0)
    assert slow.measure == pytest.approx(abs(frame.z[0]))
    assert slow.score == pytest.approx(0.5 + 0.5 * (1 - math.exp(-(slow.measure / 6.0 - 1)))) and 0.8 < slow.score < 1.0
    assert "took 28.6 min" in slow.explanation
    assert fast.direction == "down" and fast.measure > 6


def test_saturation_adapter():
    limit = 4 * 1024 ** 3
    values = np.concatenate([np.full(100, 0.4 * limit), 0.4 * limit + 0.1 * limit * np.arange(1, 73) / 12])
    s = pd.Series(values, index=pd.date_range("2026-01-01", periods=len(values), freq="5min", tz="UTC"),
                  name="container_memory_working_set_bytes|some-pod")
    scores = saturation_scores(s, limit)
    alerts = alerts_from_saturation(scores, target=s.name)
    assert [a.time for a in alerts] == list(saturation_alerts(s, limit).ts)       # same firing times
    first, last = alerts[0], alerts[-1]
    assert (first.detector, first.direction, first.line) == ("saturation", "up", 1.0)
    # just inside the 4 h horizon at first (r a little over 1), far more urgent at the end
    assert 1.0 < first.measure < 1.2 and 0.5 < first.score < 0.6
    assert last.measure > 4 and 0.97 < last.score < 1.0
    assert first.explanation.startswith("memory rising") and "limit in ~3." in first.explanation


def test_change_point_adapter():
    from sentinel_lens.detectors.change_point import change_point_alerts
    idx = pd.date_range("2026-01-01", periods=30, freq="5min", tz="UTC")
    z = pd.Series([0.0] * 10 + [2.5] * 6 + [0.0] * 8 + [-2.5] * 6, index=idx, name="trino_query_p95_sec|trino")
    up, down = alerts_from_change_point(change_point_alerts(z), target=z.name)
    assert (up.time, up.detector, up.direction, up.measure, up.line) == (idx[15], "change_point", "up", 12.0, 10.0)
    assert (down.time, down.direction) == (idx[29], "down")
    assert up.score == pytest.approx(0.5 + 0.5 * (1 - math.exp(-(1.2 - 1))))       # severity at r = 1.2, not a guardrail
    assert "level shifted up ~2.5 wobbles" in up.explanation
    custom = alerts_from_change_point(change_point_alerts(z, h=5.0), target=z.name, h=5.0)
    assert all(a.line == 5.0 and a.measure > 5.0 for a in custom)
