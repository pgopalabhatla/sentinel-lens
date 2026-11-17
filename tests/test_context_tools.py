"""The context tools of the triage agent (sentinel_stream/context_tools.py) on a synthetic world:
causality (nothing after the paging time), the page as it was when it paged, errors instead of
exceptions, the entity graph."""
import re
from types import SimpleNamespace

import pandas as pd
import pytest

from sentinel_sim.replay import DataSource
from sentinel_stream import context_tools as ct
from tests import triage_world as world

ISO = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d)?(?:\+00:00|Z)")


@pytest.fixture(scope="module")
def ds(tmp_path_factory):
    return DataSource(world.write_data_dir(tmp_path_factory.mktemp("world")))


@pytest.fixture(scope="module")
def store():
    pages, alerts = world.pages_and_alerts()
    return ct.StreamStore.from_records(world.telemetry(), pages, alerts)


def ctx_of(store, ds, page: int) -> ct.Context:
    index = next(i for i in store.paged() if store.pages[i]["page"] == page)
    return ct.Context.at_page(store, ds, index)


ALL_CALLS = [("get_page", {}), ("query_metric", {"series_id": world.LAG, "window_minutes": 10080, "points": 120}),
             ("query_metric", {"series_id": world.MEM_B, "window_minutes": 60}),
             ("related_entities", {"entity": "grp", "hops": 2}), ("recent_logs", {"pod": "p-a", "window_minutes": 360, "limit": 60}),
             ("k8s_events", {"window_minutes": 1440}), ("recent_changes", {"window_hours": 168}),
             ("dag_runs", {"window_hours": 168}), ("kafka_lag", {"group": "grp", "window_minutes": 1440})]


# ------------------------------------------------------------------------------- context tools
@pytest.mark.parametrize("page", [1, 2, 3])
def test_no_tool_returns_anything_stamped_after_the_paging_time(store, ds, page):
    ctx = ctx_of(store, ds, page)
    for name, args in ALL_CALLS:
        result, is_error = ct.call(ctx, name, args)
        assert not is_error, (name, result)
        late = [s for s in ISO.findall(ct.dumps(result)) if pd.Timestamp(s.replace("Z", "+00:00")) > ctx.now]
        assert not late, (name, late)


def test_the_cut_is_at_the_paging_time_and_not_before_it(store, ds):
    ctx = ctx_of(store, ds, 2)                                     # paged at 02:00
    assert ctx.now == world.at("02:00")
    lag, _ = ct.call(ctx, "query_metric", {"series_id": world.LAG, "window_minutes": 10080})
    assert lag["readings"] == 121 and lag["last"] == 120.0          # 00:00 .. 02:00 inclusive: the reading of 02:00 is seen
    runs, _ = ct.call(ctx, "dag_runs", {"window_hours": 24})
    assert [r["scheduled_for"][11:16] for r in runs["runs"]] == ["00:05", "01:05"]
    logs, _ = ct.call(ctx, "recent_logs", {"pod": "p-a", "window_minutes": 360, "limit": 60})
    assert logs["latest"][-1]["ts"] == "2026-01-05T01:50:07Z"        # the 02:00:07 line is after the page
    events, _ = ct.call(ctx, "k8s_events", {"window_minutes": 1440})
    assert events["by_reason"] == {"Started": 1, "Unhealthy": 1}    # not the BackOff at 02:30
    changes, _ = ct.call(ctx, "recent_changes", {"window_hours": 24})
    assert changes["changes"] == 1


def test_get_page_shows_the_page_as_it_was_when_it_paged(store, ds):
    page1 = ct.call(ctx_of(store, ds, 1), "get_page")[0]
    assert page1["n_alerts"] == 1 and page1["merged_pages"] == []    # the alert at 02:05 and the merge at 02:10 come later
    page3 = ct.call(ctx_of(store, ds, 3), "get_page")[0]
    assert page3["n_alerts"] == 1 and page3["by_target"][0]["target"] == world.MEM_B


def test_a_page_after_a_merge_holds_the_absorbed_alerts():
    pages, alerts = world.pages_and_alerts()
    pages = pages[:6] + [dict(pages[1], emitted_at=pages[5]["emitted_at"], page_id="later")]   # as if page 1 paged after the merge
    folded = ct.get_page(SimpleNamespace(page_records=pages, page=1, page_id="later", alert_records=[],
                                         catalog=_catalog(), now=world.at("02:10")))
    assert folded["n_alerts"] == 4 and folded["merged_pages"] == [2]


def _catalog():
    from sentinel_lens.correlator import Topology
    entity_of = {s: f"{k}:{e}" for s, _, e, k, _ in world.SERIES}
    return SimpleNamespace(topology=Topology(entity_of=entity_of, neighbours={}))


def test_a_missing_reading_is_counted_and_the_dropped_run_shows_as_not_started(store, ds):
    ctx = ctx_of(store, ds, 3)                                       # 03:00
    mem, _ = ct.call(ctx, "query_metric", {"series_id": world.MEM_B, "window_minutes": 30})
    assert mem["readings"] == 20 and mem["minutes_without_reading"] == 10
    runs, _ = ct.call(ctx, "dag_runs", {"dag_id": "d1", "window_hours": 2})
    status = {s["scheduled_for"][11:16]: s["status"] for s in runs["scheduled"]}
    assert status == {"01:05": "started", "02:05": "no start streamed yet"}


def test_bad_calls_come_back_as_errors_not_exceptions(store, ds):
    ctx = ctx_of(store, ds, 1)
    for name, args in [("nope", {}), ("query_metric", {}), ("query_metric", {"series_id": "x|y"}),
                       ("query_metric", {"series_id": world.LAG, "window_minutes": 0}), ("recent_logs", {"pod": "p-z"}),
                       ("recent_logs", {"pod": "p-a", "level": "LOUD"}), ("related_entities", {"entity": "nothing"}),
                       ("dag_runs", {"dag_id": "d9"}), ("kafka_lag", {"group": "tp"}), ("get_page", {"extra": 1})]:
        result, is_error = ct.call(ctx, name, args)
        assert is_error and "error" in result, (name, args, result)


# ------------------------------------------------------------------------ stream v2: evidence on the stream
def v2_store(start="01:00"):
    """The world's stream from `start` on, plus logs / events / changes batches that differ from the
    data directory's (so a test can tell which one a tool read). Each batch stamped T holds [T-1min, T)."""
    from sentinel_stream import wire
    pages, alerts = world.pages_and_alerts()
    telemetry = [m for m in world.telemetry() if wire.event_time(m) >= world.at(start)]
    for t in pd.date_range(world.at(start), periods=6 * 24, freq="10min"):
        line = {"ts": (t + pd.Timedelta("3s")).strftime("%Y-%m-%dT%H:%M:%SZ"), "pod": "p-a", "level": "WARN", "logger": "app",
                "message": f"streamed line {t:%H:%M}"}
        telemetry.append(wire.evidence_message(wire.LOGS, t + pd.Timedelta("1min"), [line]))
    event = {"ts": "2026-01-05T01:20:00Z", "type": "Warning", "reason": "OOMKilled", "message": "streamed OOMKilled p-a", "count": 1,
             "involved_object": {"kind": "Pod", "name": "p-a", "namespace": "ns"}}
    late = dict(event, ts="2026-01-05T02:00:30Z", reason="BackOff")       # in the batch stamped 02:01: after a page at 02:00
    change = {"change_id": "chg-0001", "ts": "2026-01-05T01:30:00Z", "kind": "deploy", "target": "p-a",
              "details": {"image": ["b", "c"]}, "author": "x", "description": "streamed deploy"}
    telemetry += [wire.evidence_message(wire.K8S_EVENTS, world.at("01:21"), [event]),
                  wire.evidence_message(wire.K8S_EVENTS, world.at("02:01"), [late]),
                  wire.evidence_message(wire.CHANGES, world.at("01:31"), [change])]
    return ct.StreamStore.from_records(sorted(telemetry, key=wire.position), pages, alerts)


def test_on_stream_v2_logs_events_and_changes_come_from_the_stream_and_from_history_before_it(ds):
    store = v2_store()
    index = next(i for i in store.paged() if store.pages[i]["page"] == 2)              # paged at 02:00
    ctx = ct.Context.at_page(store, ds, index, stream_version=2)
    logs, _ = ct.call(ctx, "recent_logs", {"pod": "p-a", "window_minutes": 180, "limit": 60})
    assert logs["source"] == ct.STREAM_THEN_HISTORY
    msgs = [r["message"] for r in logs["latest"]]
    before = [m for m in msgs if m.startswith("processed batch")]                      # the data directory's, before 01:00
    streamed = [m for m in msgs if m.startswith("streamed line")]
    assert len(before) == 6 and len(streamed) == 6 and msgs == before + streamed      # 00:00-00:50, then 01:00-01:50
    events, _ = ct.call(ctx, "k8s_events", {"window_minutes": 1440})
    assert events["by_reason"] == {"Started": 1, "OOMKilled": 1}     # history's Started at 00:30; not its Unhealthy at 01:50,
    changes, _ = ct.call(ctx, "recent_changes", {"window_hours": 24})  # nor the streamed BackOff stamped 02:00:30
    assert [c.get("description") for c in changes["latest"]] == [None, "streamed deploy"]
    # the same store read as stream v1: everything from the data directory, as before
    v1 = ct.Context.at_page(store, ds, index)
    logs1, _ = ct.call(v1, "recent_logs", {"pod": "p-a", "window_minutes": 180, "limit": 60})
    assert logs1["source"] == ct.HISTORY and not [r for r in logs1["latest"] if r["message"].startswith("streamed")]


@pytest.mark.parametrize("page", [1, 2, 3])
def test_on_stream_v2_no_tool_returns_anything_stamped_after_the_paging_time(ds, page):
    store = v2_store("00:00")
    index = next(i for i in store.paged() if store.pages[i]["page"] == page)
    ctx = ct.Context.at_page(store, ds, index, stream_version=2)
    for name, args in ALL_CALLS + [("baseline_comparison", {"series_id": world.LAG, "window_minutes": 360})]:
        result, is_error = ct.call(ctx, name, args)
        assert not is_error, (name, result)
        late = [s for s in ISO.findall(ct.dumps(result)) if pd.Timestamp(s.replace("Z", "+00:00")) > ctx.now]
        assert not late, (name, late)


# ------------------------------------------------------------------------------------ baseline_comparison
@pytest.fixture(scope="module")
def seasonal_world(tmp_path_factory):
    """Eight days of a daily pattern on the topic in-rate; the data directory's last day has the plain
    pattern, the STREAM's last day has the in-rate doubled from 02:00 to 02:30."""
    import json
    import numpy as np
    from sentinel_stream import wire
    root = world.write_data_dir(tmp_path_factory.mktemp("seasonal"))
    first = world.DAY - pd.Timedelta("7D")
    minutes = pd.date_range(first, world.DAY + pd.Timedelta("1D"), freq="1min", inclusive="left")
    pattern = 100 + 50 * np.sin(2 * np.pi * (minutes.hour * 60 + minutes.minute) / 1440)
    noise = np.random.default_rng(3).normal(0, 2, len(minutes))
    values = {s: np.arange(len(minutes), dtype=float) for s, *_ in world.SERIES}
    values[world.IN_RATE] = pattern + noise
    pd.DataFrame(values, index=minutes).to_csv(root / "metrics/metrics.csv.gz", compression="gzip")
    topo = json.loads((root / "catalog/topology.json").read_text())
    topo["time_range"]["start"] = first.isoformat()
    (root / "catalog/topology.json").write_text(json.dumps(topo))
    streamed = pd.Series(pattern + noise, index=minutes)[world.DAY:]
    spike = (streamed.index >= world.at("02:00")) & (streamed.index < world.at("02:30"))
    streamed[spike] *= 2
    telemetry = [wire.metrics_message(t, {world.IN_RATE: v}) for t, v in streamed.items()]
    alert = {"time": world.at("02:15").isoformat(), "target": world.IN_RATE, "detector": "seasonal", "score": 0.9, "direction": "up",
             "explanation": "x", "measure": 1.0, "line": 0.5}
    pages = [{"kind": k, "emitted_at": world.at("02:22").isoformat(), "page": 1, "page_id": "p1", "n_alerts": 1, "absorbed": [],
              "alert": alert} for k in ("opened", "paged")]
    store = ct.StreamStore.from_records(telemetry, pages, [])
    return ct.Context.at_page(store, DataSource(root), 1)


def test_baseline_comparison_scores_the_streamed_readings_against_the_same_time_on_earlier_days(seasonal_world):
    r, is_error = ct.call(seasonal_world, "baseline_comparison", {"series_id": world.IN_RATE, "window_minutes": 60})
    assert not is_error, r
    buckets = {b["bucket"][11:16]: b for b in r["buckets"]}
    assert list(buckets) == ["01:20", "01:25", "01:30", "01:35", "01:40", "01:45", "01:50", "01:55", "02:00", "02:05", "02:10", "02:15"]
    for t in ("01:20", "01:40", "01:55"):                           # the normal pattern: expected is the pattern, z small
        assert abs(buckets[t]["z"]) < 3 and buckets[t]["value"] == pytest.approx(buckets[t]["expected"], rel=0.05)
    for t in ("02:00", "02:05", "02:10", "02:15"):                  # the streamed spike (the data directory has none)
        assert buckets[t]["hot"] and buckets[t]["z"] > 6 and buckets[t]["value"] == pytest.approx(2 * buckets[t]["expected"], rel=0.05)
    assert all(b["baseline_days"] == 5 for b in r["buckets"])        # day_type: the five previous weekdays
    assert r["detector"] == "not routed" and r["history_mode"] == "day_type" and "note" in r
    assert "02:20" not in buckets                                   # paged at 02:22: the 02:20 bucket is not complete


def test_baseline_comparison_reads_only_up_to_the_paging_time(seasonal_world):
    ctx = seasonal_world
    early = ct.Context(**{**ctx.__dict__, "now": world.at("02:04"), "metrics": ctx.metrics.loc[:world.at("02:04")], "calls": []})
    r, _ = ct.call(early, "baseline_comparison", {"series_id": world.IN_RATE, "window_minutes": 10})
    assert [b["bucket"][11:16] for b in r["buckets"]] == ["01:55", "02:00"] and r["buckets"][-1]["hot"]


def test_related_entities_follows_the_graph(store, ds):
    ctx = ctx_of(store, ds, 1)
    r, _ = ct.call(ctx, "related_entities", {"entity": world.LAG})
    assert r["entity"] == "consumer_group:grp"
    assert {x["entity"]: x["relation"] for x in r["related"]} == {"topic:tp": "consumer_group:grp consumes topic:tp",
                                                                  "pod:p-a": "pod:p-a member_of consumer_group:grp"}
    two, _ = ct.call(ctx, "related_entities", {"entity": "pod:p-a", "hops": 2})
    assert {x["entity"] for x in two["related"] if x["hops"] == 2} == {"topic:tp"}
