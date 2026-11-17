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


def test_related_entities_follows_the_graph(store, ds):
    ctx = ctx_of(store, ds, 1)
    r, _ = ct.call(ctx, "related_entities", {"entity": world.LAG})
    assert r["entity"] == "consumer_group:grp"
    assert {x["entity"]: x["relation"] for x in r["related"]} == {"topic:tp": "consumer_group:grp consumes topic:tp",
                                                                  "pod:p-a": "pod:p-a member_of consumer_group:grp"}
    two, _ = ct.call(ctx, "related_entities", {"entity": "pod:p-a", "hops": 2})
    assert {x["entity"] for x in two["related"] if x["hops"] == 2} == {"topic:tp"}
