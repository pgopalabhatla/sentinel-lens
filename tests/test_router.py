import pandas as pd
import pytest

from sentinel_lens.router import RULES, route_series

UNITS = {"container_cpu_usage_cores": "cores", "kafka_messages_in_per_sec": "msg/s",
         "container_memory_working_set_bytes": "bytes", "kafka_consumergroup_lag": "messages",
         "trino_query_p95_sec": "s", "airflow_scheduler_heartbeat_age_sec": "s",
         "trino_queued_queries": "queries", "kafka_under_replicated_partitions": "partitions"}

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
]


def catalog(cases=CASES):
    return pd.DataFrame([{"series_id": f"{m}|{e}", "metric": m, "entity": e, "default_detector": d,
                          "unit": UNITS.get(m, "count")}
                         for m, e, d, *_ in cases])


def test_every_series_gets_exactly_one_rule():
    cat = catalog()
    routes = route_series(cat)
    assert list(routes.columns) == ["series_id", "rule", "fallback", "history", "abs_floor"]
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
    routes = route_series(catalog(), history="previous_days", count_floor=0.0)
    seasonal = routes[routes.rule == "seasonal"]
    assert (seasonal.history == "previous_days").all() and (seasonal.abs_floor == 0.0).all()
