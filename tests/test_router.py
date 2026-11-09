import pandas as pd
import pytest

from sentinel_lens.router import RULES, route_series

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
    return pd.DataFrame([{"series_id": f"{m}|{e}", "metric": m, "entity": e, "default_detector": d}
                         for m, e, d, *_ in cases])


def test_every_series_gets_exactly_one_rule():
    cat = catalog()
    routes = route_series(cat)
    assert list(routes.columns) == ["series_id", "rule", "fallback"]
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
