import pandas as pd

from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts

DUE = "2026-01-01T02:00:00Z"
LATER = "2026-01-02T00:00:00Z"      # a `now` long after every deadline


def ts(x):
    return pd.Timestamp(x)


def expected(dag="daily_orders", due=DUE, sla=90):
    return pd.DataFrame([{"dag_id": dag, "scheduled_for": pd.Timestamp(due), "sla_minutes": sla}])


def run(start, end, dag="daily_orders", due=DUE, run_type="scheduled", state="success"):
    """Times are strings or None, the way DataSource.job_runs returns them."""
    return {"dag_id": dag, "run_type": run_type, "scheduled_for": due, "start": start, "end": end,
            "state": state}


def runs(*rows):
    return pd.DataFrame(list(rows), columns=["dag_id", "run_type", "scheduled_for", "start", "end", "state"])


def test_on_time_run_no_alert():
    out = schedule_alerts(expected(), runs(run("2026-01-01T02:00:20Z", "2026-01-01T02:28:00Z")), LATER)
    assert len(out) == 0
    assert list(out.columns) == ["ts", "target", "rule", "explanation"]


def test_run_never_starts_one_alert_at_scheduled_plus_grace():
    out = schedule_alerts(expected(), runs(), LATER)
    assert len(out) == 1
    row = out.iloc[0]
    assert (row.ts, row.target, row.rule) == (ts("2026-01-01T02:10:00Z"), "dag:daily_orders", "start_late")
    assert row.explanation == "daily_orders was due at 02:00, no run had started by 02:10 (grace 10 min)"


def test_very_late_start_alerts_at_grace_not_at_its_end():
    late = runs(run("2026-01-01T09:40:00Z", "2026-01-01T10:06:00Z"))      # 460 min late
    out = schedule_alerts(expected(), late, LATER)
    assert list(out.ts) == [ts("2026-01-01T02:10:00Z")]
    assert list(out.rule) == ["start_late"]


def test_result_at_a_deadline_does_not_depend_on_what_happened_later():
    # asked at 03:00 (run not started yet, so not in the frame) or the next day (run known):
    # the alert for the 02:10 deadline is identical
    early = schedule_alerts(expected(), runs(), "2026-01-01T03:00:00Z")
    late = schedule_alerts(expected(), runs(run("2026-01-01T09:40:00Z", "2026-01-01T10:06:00Z")), LATER)
    pd.testing.assert_frame_equal(early, late)


def test_on_time_start_but_ends_after_sla():
    slow = runs(run("2026-01-01T02:00:20Z", "2026-01-01T04:15:00Z"))
    out = schedule_alerts(expected(), slow, LATER)
    assert list(out.rule) == ["sla_breach"]
    assert out.iloc[0].ts == ts("2026-01-01T03:30:00Z")                   # due + 90, not the 04:15 end
    assert "had not finished by 03:30 (SLA 90 min)" in out.iloc[0].explanation


def test_manual_run_does_not_satisfy_the_schedule():
    manual = runs(run("2026-01-01T02:00:20Z", "2026-01-01T02:28:00Z", run_type="manual"))
    out = schedule_alerts(expected(), manual, LATER)
    assert list(out.rule) == ["start_late"]


def test_per_dag_grace_override():
    exp = pd.concat([expected("slow_dag"), expected("other_dag")], ignore_index=True)
    both = runs(run("2026-01-01T02:20:00Z", "2026-01-01T02:40:00Z", dag="slow_dag"),
                run("2026-01-01T02:20:00Z", "2026-01-01T02:40:00Z", dag="other_dag"))
    out = schedule_alerts(exp, both, LATER, grace_min={"slow_dag": 30})
    assert list(out.target) == ["dag:other_dag"]                          # default 10 min still applies
    assert len(schedule_alerts(exp, both, LATER, grace_min=30.0)) == 0


def test_in_progress_run_before_its_sla_no_alert_yet():
    running = runs(run("2026-01-01T02:00:20Z", None, state="running"))
    assert len(schedule_alerts(expected(), running, "2026-01-01T03:00:00Z")) == 0
    after = schedule_alerts(expected(), running, "2026-01-01T03:30:00Z")  # same run, SLA now passed
    assert list(after.rule) == ["sla_breach"]


def test_no_alert_before_the_grace_deadline():
    assert len(schedule_alerts(expected(), runs(), "2026-01-01T02:09:00Z")) == 0
    assert len(schedule_alerts(expected(), runs(), "2026-01-01T02:10:00Z")) == 1


def test_failed_run_alerts_at_its_end():
    rs = runs(run("2026-01-01T02:00:20Z", "2026-01-01T03:12:00Z", state="failed"),
              run("2026-01-02T02:00:20Z", "2026-01-02T02:28:00Z", due="2026-01-02T02:00:00Z"),
              run("2026-01-03T02:00:20Z", None, due="2026-01-03T02:00:00Z", state="running"))
    out = failed_run_alerts(rs)
    assert len(out) == 1
    row = out.iloc[0]
    assert (row.ts, row.target, row.rule) == (ts("2026-01-01T03:12:00Z"), "dag:daily_orders", "failed_run")
    assert len(failed_run_alerts(runs())) == 0
