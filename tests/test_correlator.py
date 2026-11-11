import random

import pandas as pd
import pytest

from sentinel_lens.alerts import Alert
from sentinel_lens.correlator import Incident, build_topology, correlate

T0 = pd.Timestamp("2026-01-01T12:00:00Z")

# A small platform:   consumer pods -- consumer group -- topic -- cluster -- broker pod
#                     and an unrelated Trino cluster with one worker, and a DAG with no relationships.
RELATIONSHIPS = pd.DataFrame(
    [("pod", "consumer-0", "member_of", "consumer_group", "orders-etl"),
     ("pod", "consumer-1", "member_of", "consumer_group", "orders-etl"),
     ("consumer_group", "orders-etl", "consumes", "topic", "orders"),
     ("topic", "orders", "hosted_on", "cluster", "kafka"),
     ("pod", "kafka-0", "broker_of", "cluster", "kafka"),
     ("pod", "trino-worker-0", "member_of", "cluster", "trino")],
    columns=["child_kind", "child", "relation", "parent_kind", "parent"])
CATALOG = pd.DataFrame(
    [("cpu|consumer-0", "pod", "consumer-0"), ("mem|consumer-0", "pod", "consumer-0"),
     ("cpu|consumer-1", "pod", "consumer-1"), ("lag|orders-etl", "consumer_group", "orders-etl"),
     ("msgs|orders", "topic", "orders"), ("cpu|kafka-0", "pod", "kafka-0"),
     ("cpu|trino-worker-0", "pod", "trino-worker-0"), ("queued|trino", "cluster", "trino")],
    columns=["series_id", "entity_kind", "entity"])
TOPOLOGY = build_topology(RELATIONSHIPS, CATALOG, extra_edges=())    # the synthetic platform only


def alert(minutes, target, detector="seasonal", score=0.7, explanation=None):
    return Alert(time=T0 + pd.Timedelta(minutes=minutes), target=target, detector=detector, score=score,
                 direction="up", explanation=explanation or f"{target} at +{minutes} min", measure=12.0, line=6.0)


def groups(incidents):
    """Each incident as a sorted list of (minutes after T0, target): easy to compare."""
    return [sorted(((a.time - T0).total_seconds() / 60, a.target) for a in inc.alerts) for inc in incidents]


# ------------------------------------------------------------------ topology
def test_series_map_to_entities_like_the_scorer():
    assert TOPOLOGY.entity("lag|orders-etl") == "consumer_group:orders-etl"
    assert TOPOLOGY.entity("dag:daily_orders") == "dag:daily_orders"          # already an entity
    with pytest.raises(KeyError, match="not in the catalog"):
        TOPOLOGY.entity("mystery|thing")


def test_graph_is_undirected_and_counts_hops():
    assert TOPOLOGY.within("pod:consumer-0", 0) == {"pod:consumer-0"}
    assert TOPOLOGY.within("pod:consumer-0", 1) == {"pod:consumer-0", "consumer_group:orders-etl"}
    assert TOPOLOGY.within("consumer_group:orders-etl", 1) == {"consumer_group:orders-etl", "pod:consumer-0",
                                                               "pod:consumer-1", "topic:orders"}
    assert "pod:consumer-1" in TOPOLOGY.within("pod:consumer-0", 2)           # via the consumer group
    assert "pod:trino-worker-0" not in TOPOLOGY.within("pod:consumer-0", 10)  # not connected at all
    assert TOPOLOGY.within("dag:daily_orders", 3) == {"dag:daily_orders"}     # no relationships: alone


# ------------------------------------------------------------------ linking rules
def test_close_in_time_on_connected_entities_merge():
    incidents = correlate([alert(0, "lag|orders-etl"), alert(5, "cpu|consumer-0"), alert(9, "mem|consumer-0")],
                          TOPOLOGY)
    assert len(incidents) == 1
    assert incidents[0].n_targets == 3 and incidents[0].entities == ("consumer_group:orders-etl", "pod:consumer-0")


def test_close_in_time_but_unconnected_stay_separate():
    incidents = correlate([alert(0, "cpu|consumer-0"), alert(1, "cpu|trino-worker-0")], TOPOLOGY)
    assert groups(incidents) == [[(0.0, "cpu|consumer-0")], [(1.0, "cpu|trino-worker-0")]]


def test_connected_but_far_apart_in_time_stay_separate():
    incidents = correlate([alert(0, "cpu|consumer-0"), alert(16, "lag|orders-etl")], TOPOLOGY)   # gap 15
    assert len(incidents) == 2
    assert len(correlate([alert(0, "cpu|consumer-0"), alert(15, "lag|orders-etl")], TOPOLOGY)) == 1   # exactly 15 links


def test_same_target_far_apart_in_time_is_two_incidents():
    assert len(correlate([alert(0, "cpu|consumer-0"), alert(60, "cpu|consumer-0")], TOPOLOGY)) == 2


def test_hop_limit():
    # consumer-0 and consumer-1 are 2 hops apart (through their consumer group)
    two_pods = [alert(0, "cpu|consumer-0"), alert(2, "cpu|consumer-1")]
    assert len(correlate(two_pods, TOPOLOGY, max_hops=0)) == 2
    assert len(correlate(two_pods, TOPOLOGY, max_hops=1)) == 2
    assert len(correlate(two_pods, TOPOLOGY, max_hops=2)) == 1
    # same entity is 0 hops; pod and its group is 1 hop
    assert len(correlate([alert(0, "cpu|consumer-0"), alert(2, "mem|consumer-0")], TOPOLOGY, max_hops=0)) == 1
    pod_and_group = [alert(0, "cpu|consumer-0"), alert(2, "lag|orders-etl")]
    assert len(correlate(pod_and_group, TOPOLOGY, max_hops=0)) == 2
    assert len(correlate(pod_and_group, TOPOLOGY, max_hops=1)) == 1


def test_an_alert_in_between_bridges_two_pods_that_are_too_far_apart():
    # the two pods are 2 hops apart, but each is 1 hop from the group that alerts between them
    alerts = [alert(0, "cpu|consumer-0"), alert(2, "lag|orders-etl"), alert(4, "cpu|consumer-1")]
    assert len(correlate(alerts, TOPOLOGY, max_hops=1)) == 1


def test_single_linkage_in_time_chains_a_long_incident():
    alerts = [alert(m, "cpu|consumer-0") for m in (0, 10, 20, 30, 40)]        # 40 min end to end, gaps of 10
    (inc,) = correlate(alerts, TOPOLOGY, gap_min=15)
    assert (inc.start, inc.end) == (T0, T0 + pd.Timedelta(minutes=40))
    assert len(correlate(alerts, TOPOLOGY, gap_min=9)) == 5


def test_time_only_and_topology_only():
    alerts = [alert(0, "cpu|consumer-0"), alert(1, "cpu|trino-worker-0"), alert(300, "lag|orders-etl")]
    assert groups(correlate(alerts, TOPOLOGY, max_hops=None)) == [                 # time only
        [(0.0, "cpu|consumer-0"), (1.0, "cpu|trino-worker-0")], [(300.0, "lag|orders-etl")]]
    assert groups(correlate(alerts, TOPOLOGY, gap_min=None)) == [                  # topology only
        [(0.0, "cpu|consumer-0"), (300.0, "lag|orders-etl")], [(1.0, "cpu|trino-worker-0")]]


# ------------------------------------------------------------------ the incident record
def test_incident_fields():
    alerts = [alert(5, "cpu|consumer-0", detector="seasonal", score=0.6),
              alert(0, "lag|orders-etl", detector="seasonal fallback", score=0.8, explanation="lag is 9x expected"),
              alert(7, "cpu|consumer-0", detector="counter_rule", score=1.0),
              alert(9, "dag:x")]
    lag, dag = correlate(alerts, TOPOLOGY)
    assert isinstance(lag, Incident)
    assert lag.id == "2026-01-01T12:00:00Z|consumer_group:orders-etl"
    assert (lag.start, lag.end) == (T0, T0 + pd.Timedelta(minutes=7))
    assert [a.target for a in lag.alerts] == ["lag|orders-etl", "cpu|consumer-0", "cpu|consumer-0"]   # time order
    assert lag.targets == ("cpu|consumer-0", "lag|orders-etl") and lag.n_targets == 2
    assert lag.entities == ("consumer_group:orders-etl", "pod:consumer-0")
    assert lag.detectors == ("counter_rule", "seasonal", "seasonal fallback")
    assert lag.n_detectors == 2                                    # seasonal and its fallback are one kind
    assert lag.max_severity == 1.0
    assert lag.title == "lag is 9x expected"                        # explanation of the earliest alert
    assert dag.entities == ("dag:x",) and dag.title == "dag:x at +9 min"


def test_every_alert_lands_in_exactly_one_incident():
    alerts = [alert(m, t) for m in range(0, 120, 7) for t in CATALOG.series_id]
    incidents = correlate(alerts, TOPOLOGY)
    assert sum(len(i.alerts) for i in incidents) == len(alerts)
    assert len({(a.time, a.target) for i in incidents for a in i.alerts}) == len(alerts)


def test_output_is_deterministic_and_ordered():
    alerts = [alert(m, t, detector=d) for m in (0, 3, 40, 41, 90) for t in CATALOG.series_id
              for d in ("seasonal", "counter_rule")]
    expected = correlate(alerts, TOPOLOGY)
    rng = random.Random(0)
    for _ in range(5):
        shuffled = alerts[:]
        rng.shuffle(shuffled)
        assert correlate(shuffled, TOPOLOGY) == expected          # same incidents, same order, same contents
    assert [(i.start, i.id) for i in expected] == sorted((i.start, i.id) for i in expected)
    assert len({i.id for i in expected}) == len(expected)          # ids are unique


def test_no_alerts_no_incidents():
    assert correlate([], TOPOLOGY) == []


# ------------------------------------------------------------------ the platform's extra edges
def test_every_extra_edge_is_merged_by_default():
    from sentinel_lens.topology_extra import EXTRA_EDGES
    topo = build_topology(RELATIONSHIPS, CATALOG)                      # default: extras merged in
    assert len(EXTRA_EDGES) == 6
    for child_kind, child, _, parent_kind, parent, _ in EXTRA_EDGES:
        a, b = f"{child_kind}:{child}", f"{parent_kind}:{parent}"
        assert b in topo.neighbours[a] and a in topo.neighbours[b], f"{a} - {b}"
    # the given relationships are still there
    assert "consumer_group:orders-etl" in topo.neighbours["pod:consumer-0"]


def test_the_named_extra_edges():
    topo = build_topology(RELATIONSHIPS, CATALOG)
    for a, b in [("resource_group:adhoc", "cluster:trino-lakehouse"), ("spark_app:orders-stream", "topic:orders"),
                 ("flink_job:clickstream-enrich", "topic:clicks"), ("dag:daily_orders", "cluster:airflow-main"),
                 ("dag:customer_agg", "cluster:airflow-main"), ("dag:hourly_quality_check", "cluster:airflow-main")]:
        assert b in topo.within(a, 1)


def test_extra_edges_can_be_switched_off():
    topo = build_topology(RELATIONSHIPS, CATALOG, extra_edges=())
    assert "resource_group:adhoc" not in topo.neighbours and "dag:daily_orders" not in topo.neighbours


def test_every_extra_edge_has_a_justification_and_is_listed_once():
    from sentinel_lens.topology_extra import EXTRA_EDGES, edges_frame
    assert all(len(e) == 6 and len(e[5]) > 20 for e in EXTRA_EDGES)
    assert len({(e[0], e[1], e[3], e[4]) for e in EXTRA_EDGES}) == len(EXTRA_EDGES)
    assert list(edges_frame().columns) == ["child_kind", "child", "relation", "parent_kind", "parent"]
    assert len(edges_frame()) == len(EXTRA_EDGES)


def test_extra_edge_links_alerts_that_were_separate():
    catalog = pd.DataFrame([("queued|adhoc", "resource_group", "adhoc"), ("cpu|trino-worker-0", "pod", "trino-worker-0"),
                            ("queries|trino-lakehouse", "cluster", "trino-lakehouse")],
                           columns=["series_id", "entity_kind", "entity"])
    alerts = [alert(0, "queued|adhoc"), alert(3, "queries|trino-lakehouse")]
    assert len(correlate(alerts, build_topology(RELATIONSHIPS, catalog, extra_edges=()))) == 2
    assert len(correlate(alerts, build_topology(RELATIONSHIPS, catalog))) == 1
