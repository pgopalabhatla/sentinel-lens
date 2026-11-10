import pandas as pd
import pytest

from sentinel_lens.router import RULES, route_series

UNITS = {"container_cpu_usage_cores": "cores", "kafka_messages_in_per_sec": "msg/s",
         "container_memory_working_set_bytes": "bytes", "kafka_consumergroup_lag": "messages",
         "trino_query_p95_sec": "s", "airflow_scheduler_heartbeat_age_sec": "s",
         "trino_queued_queries": "queries", "kafka_under_replicated_partitions": "partitions",
         "container_cpu_throttled_ratio": "ratio", "jvm_heap_used_ratio": "ratio"}

# (metric, entity, default_detector) -> expected (rule, fallback)
CASES = [
    ("kube_pod_container_status_restarts_total", "kafka-0", "guardrail", "counter_rule", False),
    ("flink_job_restarts_total", "orders-enrich", "guardrail", "counter_rule", False),
    ("flink_checkpoint_failures_total", "orders-enrich", "guardrail", "counter_rule", False),
    ("kafka_under_replicated_partitions", "kafka", "guardrail", "above_zero", False),
    ("container_cpu_usage_cores", "kafka-0", "seasonal_baseline", "seasonal", False),
    ("kafka_messages_in_per_sec", "orders", "seasonal_baseline", "seasonal", False),
    ("container_memory_working_set_bytes", "kafka-0", "saturation_forecast", "seasonal", True),
    ("kafka_consumergroup_lag", "orders-etl", "saturation_forecast", "seasonal", True),
    ("trino_query_p95_sec", "trino", "change_point", "seasonal", True),
    ("airflow_scheduler_heartbeat_age_sec", "airflow", "guardrail", "seasonal", True),
    ("container_cpu_throttled_ratio", "kafka-0", "seasonal_baseline", "seasonal", False),
    ("jvm_heap_used_ratio", "kafka-0", "saturation_forecast", "seasonal", True),
]


def catalog(cases=CASES):
    return pd.DataFrame([{"series_id": f"{m}|{e}", "metric": m, "entity": e, "default_detector": d,
                          "unit": UNITS.get(m, "count")}
                         for m, e, d, *_ in cases])


def test_every_series_gets_exactly_one_rule():
    cat = catalog()
    routes = route_series(cat)
    assert list(routes.columns) == ["series_id", "rule", "fallback", "history", "min_change", "abs_floor", "limit"]
    assert list(routes.series_id) == list(cat.series_id)      # same series, once each, same order
    assert routes.series_id.is_unique
    assert routes.rule.isin(RULES).all()


def test_each_kind_of_series_gets_the_right_rule():
    routes = route_series(catalog()).set_index("series_id")
    for metric, entity, _, rule, fallback in CASES:
        got = routes.loc[f"{metric}|{entity}"]
        assert (got.rule, bool(got.fallback)) == (rule, fallback), metric


def test_only_seasonal_can_be_a_fallback():
    routes = route_series(catalog())
    assert (routes[routes.fallback].rule == "seasonal").all()


def test_unknown_default_detector_raises():
    with pytest.raises(ValueError, match="mystery_detector"):
        route_series(catalog([("new_metric", "x", "mystery_detector")]))


# --- seasonal settings handed out with the route

def test_default_seasonal_settings():
    routes = route_series(catalog()).set_index("series_id")
    seasonal = routes[routes.rule == "seasonal"]
    assert (seasonal.history == "day_type").all()
    assert seasonal.abs_floor["kafka_consumergroup_lag|orders-etl"] == 1.0     # counted in whole messages
    assert seasonal.abs_floor["container_cpu_usage_cores|kafka-0"] == 0.0     # cores: no absolute floor
    assert seasonal.abs_floor["trino_query_p95_sec|trino"] == 0.0


def test_other_rules_carry_no_seasonal_settings():
    routes = route_series(catalog())
    others = routes[routes.rule != "seasonal"]
    assert others.history.isna().all() and (others.abs_floor == 0.0).all()   # even though their unit is a count


def test_first_build_settings_are_still_available():
    routes = route_series(catalog(), history="previous_days", count_min_change=0.0, metric_min_change={})
    seasonal = routes[routes.rule == "seasonal"]
    assert (seasonal.history == "previous_days").all()
    assert (seasonal.abs_floor == 0.0).all() and (seasonal.min_change == 0.0).all()


def test_count_min_change_is_converted_with_the_threshold():
    lag = "kafka_consumergroup_lag|orders-etl"
    assert route_series(catalog()).set_index("series_id").abs_floor[lag] == 1.0           # 6.0 / 6.0
    assert route_series(catalog(), count_min_change=3.0).set_index("series_id").abs_floor[lag] == 0.5
    assert route_series(catalog(), count_min_change=3.0, threshold=3.0).set_index("series_id").abs_floor[lag] == 1.0


# --- per-metric minimum change

THROTTLE, HEAP = "container_cpu_throttled_ratio|kafka-0", "jvm_heap_used_ratio|kafka-0"


def test_metric_override_sets_min_change_and_floor():
    routes = route_series(catalog()).set_index("series_id")
    assert routes.min_change[THROTTLE] == 0.18
    assert routes.abs_floor[THROTTLE] == pytest.approx(0.03)          # 0.18 / threshold 6
    assert routes.min_change[HEAP] == 0.0 and routes.abs_floor[HEAP] == 0.0   # same unit, no override
    assert routes.min_change["kafka_consumergroup_lag|orders-etl"] == 6.0     # the unit rule still applies


def test_metric_overrides_can_be_switched_off_or_replaced():
    off = route_series(catalog(), metric_min_change={}).set_index("series_id")
    assert off.min_change[THROTTLE] == 0.0 and off.abs_floor[THROTTLE] == 0.0
    custom = route_series(catalog(), metric_min_change={"kafka_consumergroup_lag": 600.0}).set_index("series_id")
    assert custom.min_change["kafka_consumergroup_lag|orders-etl"] == 600.0   # wins over the count rule
    assert custom.abs_floor["kafka_consumergroup_lag|orders-etl"] == 100.0
    assert custom.min_change[THROTTLE] == 0.0                                 # replaced table, no throttle entry


def test_min_change_and_abs_floor_always_agree():
    routes = route_series(catalog())
    assert list(routes.abs_floor * 6.0) == pytest.approx(list(routes.min_change))


# --- saturation rule (off by default)

MEMORY = "container_memory_working_set_bytes|kafka-0"
LIMITS = {"kafka-0": 4 * 1024 ** 3}


def test_saturation_is_off_by_default():
    routes = route_series(catalog()).set_index("series_id")
    assert routes.rule[MEMORY] == "seasonal" and bool(routes.fallback[MEMORY])
    assert routes.limit.isna().all()
    assert "saturation" not in set(routes.rule)


def test_saturation_flag_routes_only_container_memory():
    routes = route_series(catalog(), saturation=True, limits=LIMITS).set_index("series_id")
    assert routes.rule[MEMORY] == "saturation" and not routes.fallback[MEMORY]
    assert routes.limit[MEMORY] == 4 * 1024 ** 3
    assert pd.isna(routes.history[MEMORY]) and routes.abs_floor[MEMORY] == 0.0   # no seasonal settings
    assert list(routes.index[routes.rule == "saturation"]) == [MEMORY]
    # same catalog detector, deliberately left on the seasonal fallback
    for sid in [HEAP, "kafka_consumergroup_lag|orders-etl"]:
        assert routes.rule[sid] == "seasonal" and bool(routes.fallback[sid])
    # everything else is routed exactly as without the flag
    off = route_series(catalog()).set_index("series_id")
    others = routes.index != MEMORY
    pd.testing.assert_frame_equal(routes[others], off[others])


def test_saturation_without_a_limit_raises():
    with pytest.raises(ValueError, match="no limit for container_memory_working_set_bytes.kafka-0"):
        route_series(catalog(), saturation=True, limits={})
    with pytest.raises(ValueError, match="no limit"):
        route_series(catalog(), saturation=True)
    with pytest.raises(ValueError, match="no limit"):
        route_series(catalog(), saturation=True, limits={"kafka-0": 0})
