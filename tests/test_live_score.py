"""The live scorer's rule, on hand-made answer-key and page records (no broker, no engine).

The rule is fixed in docs/live-protocol.md; these tests pin each clause of it.
"""
import pandas as pd
import pytest

from sentinel_lens.correlator import Topology
from sentinel_stream import live_score

T0 = pd.Timestamp("2026-10-26 00:00", tz="UTC")
# pod-a -- group -- topic -- producer ; far is connected to nothing
TOPOLOGY = Topology(
    entity_of={"mem|pod-a": "pod:pod-a", "lag|group": "consumer_group:group", "in|topic": "topic:t", "cpu|producer": "pod:producer",
               "cpu|far": "pod:far"},
    neighbours={"pod:pod-a": {"consumer_group:group"}, "consumer_group:group": {"pod:pod-a", "topic:t"},
                "topic:t": {"consumer_group:group", "pod:producer"}, "pod:producer": {"topic:t"}})


def at(minute: float) -> str:
    return (T0 + pd.Timedelta(minutes=minute)).isoformat()


def started(id_, start, stop, entities=("pod:pod-a",), scenario="leak") -> dict:
    return {"event": "started", "id": id_, "scenario": scenario, "target": "x", "params": {"duration_min": float(stop - start)},
            "series": [], "entities": list(entities), "start": at(start), "stop": at(stop)}


def alert(target, minute, detector="seasonal") -> dict:
    return {"time": at(minute), "target": target, "detector": detector, "score": 0.8, "direction": "up",
            "explanation": "x", "measure": 7.0, "line": 6.0}


def event(kind, page, minute, target, absorbed=()) -> dict:
    return {"kind": kind, "emitted_at": at(minute), "page": page, "page_id": f"p{page}", "n_alerts": 1,
            "absorbed": list(absorbed), "alert": alert(target, minute)}


def page(page_no, minute, target, paged=True) -> list:
    """A page opened by one alert at `minute`, paging in the same step unless paged=False."""
    return [event("opened", page_no, minute, target)] + ([event("paged", page_no, minute, target)] if paged else [])


PUBLISHED = {"event": "published", "start": at(0), "end": at(2 * 1440)}          # a 2-day run


def score(answer_key, pages):
    return live_score.score(answer_key + [PUBLISHED], pages, TOPOLOGY)


def test_a_page_on_the_target_inside_the_window_detects_the_injection():
    r = score([started("a", 100, 160)], page(1, 112, "mem|pod-a"))
    assert (r["n_injections"], r["n_detected"], r["false_page_outs"]) == (1, 1, 0)
    row = r["injections"][0]
    assert row["detected"] and row["time_to_detect_min"] == 12 and row["paged_by"] == "mem|pod-a (seasonal)"
    assert r["median_time_to_detect_min"] == 12 and r["false_pages_per_day"] == 0


def test_one_hop_from_a_target_counts_and_two_hops_do_not():
    one = score([started("a", 100, 160)], page(1, 120, "lag|group"))               # the pod's consumer group
    two = score([started("a", 100, 160)], page(1, 120, "in|topic"))                # the group's topic: 2 hops from the pod
    assert one["n_detected"] == 1 and one["false_page_outs"] == 0
    assert two["n_detected"] == 0 and two["false_page_outs"] == 1 and two["false_pages_per_day"] == 0.5
    assert two["false"][0]["alerts"] == ["in|topic (seasonal)"]


def test_the_window_is_start_to_stop_plus_30_minutes_inclusive():
    for minute, detected in [(99, False), (100, True), (160, True), (190, True), (191, False)]:
        r = score([started("a", 100, 160)], page(1, minute, "mem|pod-a"))
        assert r["n_detected"] == int(detected), minute
        assert r["false_page_outs"] == int(not detected), minute


def test_time_to_detect_is_the_first_matching_page_out():
    pages = page(1, 130, "mem|pod-a") + page(2, 115, "cpu|far") + page(3, 150, "lag|group")
    r = score([started("a", 100, 160)], sorted(pages, key=lambda e: e["emitted_at"]))
    row = r["injections"][0]
    assert row["time_to_detect_min"] == 30 and row["matching_page_outs"] == 2
    assert r["false_page_outs"] == 1 and r["page_outs"] == 3 and r["matched_page_outs"] == 2


def test_a_page_that_does_not_page_detects_nothing_and_is_not_a_false_page():
    r = score([started("a", 100, 160)], page(1, 120, "mem|pod-a", paged=False))
    assert r["n_detected"] == 0 and r["page_outs"] == 0 and r["false_page_outs"] == 0
    assert r["injections"][0]["alerts_near_target"] == 1                            # ... but the column shows it was seen


def test_what_counts_is_what_the_page_holds_at_the_moment_it_pages():
    """A page pages for something unrelated; the target's alert joins it later. Nobody is paged
    again, so the injection is not detected, and the page-out stays false."""
    pages = page(1, 110, "cpu|far") + [event("alert_added", 1, 125, "mem|pod-a")]
    r = score([started("a", 100, 160)], pages)
    assert r["n_detected"] == 0 and r["false_page_outs"] == 1
    # the other way round: the page holds the target's alert (demoted, say) and pages later through another alert
    pages = page(1, 110, "mem|pod-a", paged=False) + [event("alert_added", 1, 118, "cpu|far"), event("paged", 1, 118, "cpu|far")]
    r = score([started("a", 100, 160)], pages)
    assert r["n_detected"] == 1 and r["injections"][0]["time_to_detect_min"] == 18


def test_a_merge_moves_the_absorbed_pages_alerts_into_the_page_that_pages():
    pages = (page(1, 105, "mem|pod-a", paged=False) + page(2, 108, "cpu|far", paged=False)
             + [event("merged", 1, 112, "cpu|far", absorbed=[2]), event("paged", 1, 112, "cpu|far")])
    r = score([started("a", 100, 160)], pages)
    assert r["n_detected"] == 1 and r["injections"][0]["time_to_detect_min"] == 12


def test_the_actual_stop_replaces_the_planned_one():
    answer_key = [started("a", 100, 400), {"event": "stopped", "id": "a", "stop": at(150), "reason": "stop command"}]
    late = score(answer_key, page(1, 200, "mem|pod-a"))                             # 50 minutes after the real stop
    assert late["n_detected"] == 0 and late["false_page_outs"] == 1
    assert late["injections"][0]["stop"] == pd.Timestamp(at(150))


def test_several_injections_several_pages_overall_numbers():
    answer_key = [started("a", 100, 160, scenario="leak"), started("b", 600, 660, ("pod:producer",), scenario="crash"),
                  started("c", 1200, 1260, scenario="leak")]
    pages = page(1, 104, "mem|pod-a") + page(2, 400, "cpu|far") + page(3, 640, "in|topic") + page(4, 2000, "mem|pod-a")
    r = score(answer_key, pages)
    assert (r["n_injections"], r["n_detected"], r["page_outs"], r["false_page_outs"]) == (3, 2, 4, 2)
    assert r["median_time_to_detect_min"] == 22 and r["max_time_to_detect_min"] == 40 and r["false_pages_per_day"] == 1.0
    assert r["by_scenario"]["leak"] == {"injected": 2, "detected": 1, "times": [4.0]}
    assert [i["detected"] for i in r["injections"]] == [True, True, False]
    text = live_score.markdown(r)
    assert "| 3 | 2 | 22 | 40 | 4 | 2 | 2 | 1.00 |" in text and "| c | leak | x | Oct 26 20:00 | Oct 26 21:00 |" in text and "NO" in text


def test_no_injections_every_page_is_false_and_the_length_must_be_known():
    r = score([], page(1, 50, "mem|pod-a") + page(2, 70, "cpu|far"))
    assert r["n_injections"] == 0 and r["false_page_outs"] == 2 and r["median_time_to_detect_min"] is None
    assert "None." not in live_score.markdown(r) and "| 0 | 0 | - | - | 2 | 0 | 2 | 1.00 |" in live_score.markdown(r)
    with pytest.raises(ValueError, match="published"):
        live_score.score([], [], TOPOLOGY)
    assert live_score.score([], [], TOPOLOGY, days=7)["false_pages_per_day"] == 0
