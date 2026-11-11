import random

import pandas as pd
import pytest

from sentinel_lens.alerts import Alert
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.feedback import (FALSE, LABEL_DELAY_MIN, N_MIN, TAU, TRUE, estimate, group_key, is_demoted, pages,
                                    paging_time, replay)

T0 = pd.Timestamp("2026-01-01T00:00:00Z")
EVENTS = {"throttle|pod-a": "cpu_throttling", "throttle|pod-b": "cpu_throttling", "cpu|pod-a": "cpu_usage",
          "lag|group": "consumer_lag"}


def alert(minutes, target="throttle|pod-a", detector="seasonal"):
    return Alert(time=T0 + pd.Timedelta(minutes=minutes), target=target, detector=detector, score=0.7,
                 direction="up", explanation=f"{target} at +{minutes}", measure=12.0, line=6.0)


# ------------------------------------------------------------------ estimate and the demotion rule
def test_defaults():
    assert (N_MIN, TAU, LABEL_DELAY_MIN) == (10, 0.25, 60)


def test_estimate_math():
    assert estimate(0, 0) == 0.5                      # no labels: one real, one false assumed
    assert estimate(2, 0) == 0.75                     # (2 + 1) / (2 + 2)
    assert estimate(0, 2) == 0.25
    assert estimate(1, 9) == pytest.approx(2 / 12)
    assert estimate(0, 10) == pytest.approx(1 / 12)


def test_no_demotion_below_n_min():
    assert not is_demoted(0, 9)                       # estimate 1/11 = 0.09, but only 9 labels
    assert is_demoted(0, 10)
    assert not is_demoted(0, 9, n_min=10) and is_demoted(0, 9, n_min=5)


def test_demotion_needs_a_low_estimate():
    assert is_demoted(1, 9)                           # 2/12 = 0.167
    assert not is_demoted(2, 8)                       # 3/12 = 0.25 is not below 0.25
    assert not is_demoted(5, 5) and not is_demoted(50, 0)
    assert is_demoted(2, 8, tau=0.33)


# ------------------------------------------------------------------ group keys
def test_group_key_for_series_and_dag_alerts():
    assert group_key(alert(0, "throttle|pod-a"), EVENTS) == ("seasonal", "cpu_throttling")
    assert group_key(alert(0, "throttle|pod-b"), EVENTS) == ("seasonal", "cpu_throttling")     # same kind, other pod
    assert group_key(alert(0, "cpu|pod-a"), EVENTS) == ("seasonal", "cpu_usage")
    assert group_key(alert(0, "throttle|pod-a", detector="counter_rule"), EVENTS) == ("counter_rule", "cpu_throttling")
    assert group_key(alert(0, "dag:hourly_check", detector="job_run_baseline"), EVENTS) == \
           ("job_run_baseline", "dag:hourly_check")
    with pytest.raises(KeyError, match="canonical event"):
        group_key(alert(0, "new_metric|x"), EVENTS)


def test_series_without_a_canonical_event_is_grouped_by_its_metric():
    events = {"heartbeat_age|airflow": "", "isr_shrinks|kafka": "", "queued|adhoc": float("nan")}
    assert group_key(alert(0, "heartbeat_age|airflow"), events) == ("seasonal", "heartbeat_age")
    assert group_key(alert(0, "isr_shrinks|kafka"), events) == ("seasonal", "isr_shrinks")     # not one blank group
    assert group_key(alert(0, "queued|adhoc"), events) == ("seasonal", "queued")


# ------------------------------------------------------------------ replay
def false_run(n, start=0, step=5, target="throttle|pod-a"):
    """n alerts, `step` minutes apart, all labeled false."""
    alerts = [alert(start + i * step, target) for i in range(n)]
    return alerts, {a: FALSE for a in alerts}


def test_group_is_demoted_only_once_enough_labels_have_arrived():
    alerts, labels = false_run(30)                    # at 0, 5, ..., 145 min; labels arrive 60 min later
    result = replay(alerts, labels, EVENTS)
    # the 10th label (alert at +45) arrives at +105, so the alert at +105 is the first to be demoted
    assert sorted((a.time - T0).total_seconds() / 60 for a in result.demoted) == list(range(105, 150, 5))
    (g,) = result.groups
    assert (g.key, g.true, g.false, g.labeled, g.alerts, g.demoted_alerts) == \
           (("seasonal", "cpu_throttling"), 0, 30, 30, 30, 9)
    assert g.first_demoted == T0 + pd.Timedelta(minutes=105) and g.estimate == pytest.approx(1 / 32)
    assert result.demoted_now(("seasonal", "cpu_throttling"))


def test_a_label_only_affects_later_alerts():
    alerts, labels = false_run(10)                    # 10 false labels, the last arrives at +105
    just_before, exactly, after = alert(104, "throttle|pod-b"), alert(105, "throttle|pod-b"), alert(300, "throttle|pod-b")
    result = replay(alerts + [just_before, exactly, after], labels, EVENTS)
    assert just_before not in result.demoted          # the 10th label is not in yet
    assert exactly in result.demoted and after in result.demoted
    assert not (set(alerts) & result.demoted)         # none of the labeled alerts is affected by its own label


def test_even_with_no_delay_a_label_cannot_affect_its_own_alert():
    alerts, labels = false_run(11)                    # at 0, 5, ..., 50
    result = replay(alerts, labels, EVENTS, label_delay_min=0)
    # the 10th alert (+45) is judged with 9 labels; only the 11th (+50) sees all 10
    assert result.demoted == {alerts[10]}


def test_label_delay_moves_the_first_demoted_alert():
    alerts, labels = false_run(40)
    first = {d: min(a.time for a in replay(alerts, labels, EVENTS, label_delay_min=d).demoted) for d in (30, 60, 120)}
    assert [(first[d] - T0).total_seconds() / 60 for d in (30, 60, 120)] == [75, 105, 165]


def test_labels_from_one_pod_demote_the_same_kind_of_alert_on_another_pod_only():
    alerts, labels = false_run(10)
    other_pod, other_kind = alert(500, "throttle|pod-b"), alert(500, "cpu|pod-a")
    result = replay(alerts + [other_pod, other_kind], labels, EVENTS)
    assert other_pod in result.demoted and other_kind not in result.demoted


def test_expected_and_missing_labels_do_not_count():
    alerts, labels = false_run(12)
    labels = {a: ("expected" if i < 6 else FALSE) for i, a in enumerate(alerts)}    # only 6 real labels
    del labels[alerts[-1]]                                                          # and one unlabeled
    late = alert(1000)
    result = replay(alerts + [late], labels, EVENTS)
    assert late not in result.demoted
    assert (result.groups[0].true, result.groups[0].false, result.groups[0].alerts) == (0, 5, 13)


def test_true_labels_keep_a_group_alive_and_can_bring_it_back():
    bad, bad_labels = false_run(10)
    good = [alert(200 + i * 5) for i in range(4)]
    labels = {**bad_labels, **{a: TRUE for a in good}}
    during, later = alert(150), alert(1000)
    result = replay(bad + good + [during, later], labels, EVENTS)
    assert during in result.demoted                    # 0 true, 10 false at that time
    assert later not in result.demoted                 # 4 true, 10 false: estimate 5/16 = 0.31
    assert not result.demoted_now(("seasonal", "cpu_throttling"))


def test_replay_is_deterministic_and_order_independent():
    alerts, labels = false_run(25)
    extra = [alert(m, t) for m in (3, 90, 200) for t in ("cpu|pod-a", "throttle|pod-b", "dag:x")]
    everything = alerts + extra
    expected = replay(everything, labels, EVENTS)
    rng = random.Random(1)
    for _ in range(5):
        shuffled = everything[:]
        rng.shuffle(shuffled)
        assert replay(shuffled, labels, EVENTS) == expected
    assert [g.key for g in expected.groups] == sorted(g.key for g in expected.groups)


# ------------------------------------------------------------------ pages
RELATIONSHIPS = pd.DataFrame([("pod", "pod-a", "member_of", "consumer_group", "group")],
                             columns=["child_kind", "child", "relation", "parent_kind", "parent"])
CATALOG = pd.DataFrame([("throttle|pod-a", "pod", "pod-a"), ("cpu|pod-a", "pod", "pod-a"), ("lag|group", "consumer_group", "group"),
                        ("throttle|pod-b", "pod", "pod-b")], columns=["series_id", "entity_kind", "entity"])
TOPOLOGY = build_topology(RELATIONSHIPS, CATALOG, extra_edges=())


def test_demoted_alerts_attach_to_pages_but_cannot_page_alone():
    history, labels = false_run(10)                                    # demotes (seasonal, cpu_throttling) from +105
    alone = alert(600, "throttle|pod-b")                               # a demoted alert with nothing else around
    with_company = [alert(900, "throttle|pod-a"), alert(905, "lag|group")]   # demoted + a normal alert, linked
    result = replay(history + [alone] + with_company, labels, EVENTS)
    assert alone in result.demoted and with_company[0] in result.demoted and with_company[1] not in result.demoted

    incidents = correlate([alone] + with_company, TOPOLOGY)
    lonely_page = next(i for i in incidents if alone in i.alerts)
    shared_page = next(i for i in incidents if with_company[1] in i.alerts)
    assert not pages(lonely_page, result.demoted)                      # only demoted alerts: nobody is paged
    assert paging_time(lonely_page, result.demoted) is None
    assert pages(shared_page, result.demoted)                          # one alert is not demoted
    assert with_company[0] in shared_page.alerts                       # the demoted alert is still attached
    assert paging_time(shared_page, result.demoted) == with_company[1].time   # paged by the non-demoted alert


def test_without_demotions_every_page_pages_at_its_first_alert():
    incidents = correlate([alert(0), alert(5, "lag|group")], TOPOLOGY)
    assert all(pages(i, frozenset()) and paging_time(i, frozenset()) == i.start for i in incidents)
