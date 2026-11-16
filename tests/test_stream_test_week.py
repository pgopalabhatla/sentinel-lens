"""The streaming engine against the batch pipeline on the TEST week.

The test week was evaluated once (docs/results/test-week.md) and nothing is tuned here: these tests
only check that the streaming engine, fed the week one minute at a time, says exactly what the frozen
batch pipeline said. No labels are read. If one of them fails, do not change either side to make it
pass: the difference is the finding.

1. Equivalence  same alerts, same pages, same paging times as scripts/run_test_week.py.
2. Causality    an engine that is only given the stream up to a cut emits exactly what the full run
                had emitted by then.
3. Restart      stop mid-week, rebuild the state from history, continue: same output as never stopping.

Needs the dataset in data/ (not in git); skipped when it is missing. Takes about a minute.
"""
from pathlib import Path

import pandas as pd
import pytest

from sentinel_lens import feedback
from sentinel_lens.correlator import build_topology, correlate
from sentinel_stream import FROZEN_DEMOTED_GROUPS, AlertEvent, PageEvent, StreamEngine, replay

DATA = Path(__file__).resolve().parents[1] / "data"
pytestmark = [pytest.mark.skipif(not (DATA / "metrics").exists(), reason="dataset not generated"),
              pytest.mark.filterwarnings("ignore::RuntimeWarning")]

# The frozen result (docs/results/test-week.md): raw alerts in the window, alerts demoted by the frozen
# feedback, pages of system 4, pages of system 5.
FROZEN_RESULT = {"alerts": 1445, "demoted": 244, "pages": 26, "paging_pages": 24}

# Cut times for the causality test, chosen so that each one tests something:
#   Oct 19 06:52   mid-bucket, 2 minutes into the bucket after the memory leak's first alert (batch time 06:45)
#   Oct 20 02:07   mid-bucket, the leak's page open (321 alerts so far), the nightly Spark job running.
#                  This is also where the restart test stops and restarts.
#   Oct 20 16:20   on a bucket boundary, in the middle of the consumer-lag incident
CUTS = ("2026-10-19 06:52", "2026-10-20 02:07", "2026-10-20 16:20")
MID_BUCKET, RESTART, ON_BOUNDARY = CUTS
SEASONAL_LAG = pd.Timedelta("4min")     # a 5-minute bucket is complete 4 minutes after the time it is stamped with


def utc(x) -> pd.Timestamp:
    return pd.Timestamp(x, tz="UTC")


def key(a):
    return (a.time, a.target, a.detector)


def alerts_of(events) -> list:
    return [e.alert for e in events if isinstance(e, AlertEvent)]


def describe(only_stream, only_batch, n=5) -> str:
    lines = [f"{len(only_stream)} alert(s) only in the stream, {len(only_batch)} only in batch"]
    lines += [f"  stream only: {a}" for a in sorted(only_stream, key=key)[:n]]
    lines += [f"  batch only:  {a}" for a in sorted(only_batch, key=key)[:n]]
    return "\n".join(lines)


# ------------------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def world():
    from scripts.run_test_week import HISTORY_START, SPLITS
    from sentinel_sim.replay import DataSource

    lo, hi = SPLITS["test"]
    ds = DataSource(DATA).at(hi)
    catalog = ds.series()
    return {"ds": ds, "lo": lo, "hi": hi, "history_start": HISTORY_START,
            "topology": build_topology(ds.relationships(), catalog),
            "canonical_event_of": dict(zip(catalog.series_id, catalog.canonical_event))}


@pytest.fixture(scope="module")
def batch(world):
    """What the frozen batch pipeline says about the test week (scripts/run_test_week.py)."""
    from scripts.run_test_week import final_alerts, in_window

    alerts = in_window(final_alerts(world["ds"], world["hi"]), "test")
    demoted = feedback.apply_frozen(alerts, FROZEN_DEMOTED_GROUPS, world["canonical_event_of"])
    return {"alerts": alerts, "pages": correlate(alerts, world["topology"]), "demoted": demoted}


def run_stream(world, start, end, **warm):
    engine = StreamEngine.warm_start(world["ds"], start, history_start=world["history_start"], **warm)
    events = list(replay(engine, world["ds"], start, end, history_start=world["history_start"]))
    return engine, events


@pytest.fixture(scope="module")
def full(world):
    """The whole test week through the engine, warm-started on everything before it."""
    return run_stream(world, world["lo"], world["hi"])


@pytest.fixture(scope="module")
def cut_runs(world):
    """For each cut, a fresh engine that is given the stream only up to the cut."""
    return {cut: run_stream(world, world["lo"], cut) for cut in CUTS}


# ---------------------------------------------------------------------------------- 1. equivalence
def test_batch_side_is_the_frozen_result(batch):
    paging = [p for p in batch["pages"] if feedback.pages(p, batch["demoted"])]
    assert {"alerts": len(batch["alerts"]), "demoted": len(batch["demoted"]), "pages": len(batch["pages"]),
            "paging_pages": len(paging)} == FROZEN_RESULT


def test_stream_alerts_equal_batch_alerts(full, batch):
    _, events = full
    stream = alerts_of(events)

    def fields(a):
        return (a.time, a.target, a.detector, a.direction, a.score)

    got, want = sorted(map(fields, stream)), sorted(map(fields, batch["alerts"]))
    assert got == want, describe(set(stream) - set(batch["alerts"]), set(batch["alerts"]) - set(stream))
    # and the rest of the record too: explanation, measure, line
    assert sorted(stream, key=key) == sorted(batch["alerts"], key=key)


def test_stream_pages_and_paging_times_equal_batch(full, batch):
    engine, _ = full
    assert engine.pages() == batch["pages"]
    assert engine.demoted == batch["demoted"]
    want = {p.id: feedback.paging_time(p, batch["demoted"]) for p in batch["pages"] if feedback.pages(p, batch["demoted"])}
    got = {p.id: feedback.paging_time(p, engine.demoted) for p in engine.paging_pages()}
    assert got == want
    assert len(got) == FROZEN_RESULT["paging_pages"]


def test_alert_times_and_emission_times(full):
    """An alert's time is the batch time. The engine can know a seasonal alert only when its 5-minute
    bucket is complete, 4 minutes after that time; every other alert is known at its own time."""
    _, events = full
    for e in events:
        if isinstance(e, AlertEvent):
            seasonal = e.alert.detector.startswith("seasonal")
            assert e.emitted_at - e.alert.time == (SEASONAL_LAG if seasonal else pd.Timedelta(0)), e
    emitted = [e.emitted_at for e in events]
    assert emitted == sorted(emitted)               # output comes in stream-time order


def test_the_engine_pages_no_later_than_4_minutes_after_the_batch_paging_time(full, batch):
    engine, _ = full
    paged_at = engine.pager.paged_at()
    for page in engine.paging_pages():
        delay = paged_at[page.id] - feedback.paging_time(page, batch["demoted"])
        assert pd.Timedelta(0) <= delay <= SEASONAL_LAG, (page.id, delay)


# ----------------------------------------------------------------------------------- 2. causality
@pytest.mark.parametrize("cut", CUTS)
def test_stream_up_to_a_cut_is_the_prefix_of_the_full_run(cut, cut_runs, full, batch, world):
    engine, events = cut_runs[cut]
    _, full_events = full
    t = utc(cut)
    assert events == [e for e in full_events if e.emitted_at < t]

    # its pages are the batch pages of the alerts it has seen
    seen = alerts_of(events)
    assert engine.pages() == correlate(seen, world["topology"])

    # against batch alert times: everything before the cut, except seasonal alerts whose bucket was
    # still open at the cut (batch stamps them with the bucket's start, before its minutes are in)
    before = {a for a in batch["alerts"] if a.time < t}
    missing = before - set(seen)
    assert set(seen) <= before
    open_bucket = t.floor("5min")
    assert all(a.detector.startswith("seasonal") and a.time == open_bucket and t > open_bucket for a in missing), \
        describe(set(), missing)


def test_on_a_bucket_boundary_the_cut_run_has_exactly_the_batch_alerts_before_it(cut_runs, batch):
    cut = ON_BOUNDARY
    assert utc(cut) == utc(cut).floor("5min")
    _, events = cut_runs[cut]
    assert sorted(alerts_of(events), key=key) == sorted([a for a in batch["alerts"] if a.time < utc(cut)], key=key)


def test_mid_bucket_the_batch_alert_of_the_open_bucket_is_not_known_yet(cut_runs, batch):
    """06:52 on Oct 19: batch has an alert stamped 06:50, made from the minutes 06:50-06:54. An engine
    that has seen up to 06:51 cannot have it; it does have the one stamped 06:45."""
    cut = utc(MID_BUCKET)
    _, events = cut_runs[MID_BUCKET]
    seen = set(alerts_of(events))
    not_yet = [a for a in batch["alerts"] if a.time == cut.floor("5min")]
    assert not_yet and not set(not_yet) & seen
    assert seen == {a for a in batch["alerts"] if a.time < cut.floor("5min")} and seen


# ------------------------------------------------------------------------------------- 3. restart
def test_restart_mid_week_continues_as_if_never_stopped(cut_runs, full, world):
    """Stop at RESTART, throw the engine away, build a new one from history (metrics and runs from
    the DataSource; pages from the alerts already emitted), continue to the end of the week."""
    _, before = cut_runs[RESTART]
    log = [e for e in before if isinstance(e, AlertEvent)]
    engine, after = run_stream(world, RESTART, world["hi"], alert_log=log)

    full_engine, full_events = full
    assert before + after == full_events
    assert engine.pages() == full_engine.pages()
    assert engine.demoted == full_engine.demoted
    assert engine.pager.paged_at() == full_engine.pager.paged_at()
    # the restart is not a quiet moment: pages that were open before it get more alerts after it,
    # and one of them is merged with a page that opens later
    old_pages = {e.page for e in before if isinstance(e, PageEvent)}
    later = [e for e in after if isinstance(e, PageEvent) and e.page in old_pages]
    assert {"alert_added", "merged"} <= {e.kind for e in later}
    assert sum(isinstance(e, AlertEvent) for e in after) > 1000


# ------------------------------------------------------------------------- live paging vs. final pages
def test_live_pages_outnumber_final_pages_by_the_ones_merged_after_paging(full):
    """Batch counts pages at the end of the week (24 that page). Live, a page that has already paged
    can later be joined to another one by a new alert. The engineer was paged for both, so the engine
    pages 31 times for what ends up as 24 pages."""
    engine, events = full
    paged = {}
    absorbed_after_paging = 0
    for e in events:
        if isinstance(e, PageEvent) and e.kind == "paged":
            paged[e.page] = e.emitted_at
        if isinstance(e, PageEvent) and e.kind == "merged":
            absorbed_after_paging += sum(k in paged for k in e.absorbed if e.page in paged)
    assert len(paged) - absorbed_after_paging == len(engine.paging_pages()) == FROZEN_RESULT["paging_pages"]
    assert (len(paged), absorbed_after_paging) == (31, 7)
