"""Relationships that are true of the platform but missing from the generated topology.

The topology file lives in data/, which is regenerated and not in git, so these additions are kept
here, in a tracked file, and merged in by `sentinel_lens.correlator.build_topology`.

This is platform configuration, not detector logic: on another platform this list would be replaced
(or emptied with `build_topology(..., extra_edges=())`).

Rule for this list: an edge is added only when the platform's own description supports it (the
source descriptions and the series catalog, read through DataSource). An edge is never added because
it improves a score. The gaps were noticed while looking at how train incidents fragmented into
several pages, so the list was chosen after seeing train; each edge is nevertheless a structural fact
that can be checked against the platform description quoted next to it.

Audit that produced the list (scripts/audit_topology.py):
- entities that appear in alerts or the catalog and have no relationship at all:
  resource_group:adhoc, dag:hourly_quality_check;
- entities that have relationships but are cut off from the thing the description says they depend
  on: spark_app:orders-stream and flink_job:clickstream-enrich (from their Kafka topics),
  dag:daily_orders and dag:customer_agg (from the Airflow instance that schedules them).

Considered and NOT added, because the description does not state them:
- spark_app:orders-stream - consumer_group:orders-stream: only the names match.
- Trino - the Spark apps, through the Iceberg tables one writes and the other reads: there is no
  table entity in the graph, and a direct edge would be an inference, not a stated relationship.
- any edge between entities merely because they share a catalog source_id.
"""
from __future__ import annotations

import pandas as pd

# (child_kind, child, relation, parent_kind, parent, justification from the platform description)
EXTRA_EDGES = (
    ("resource_group", "adhoc", "member_of", "cluster", "trino-lakehouse",
     "a Trino resource group is a query queue inside its cluster; the catalog files "
     "trino_resource_group_queued|adhoc under source trino-lakehouse"),
    ("spark_app", "orders-stream", "consumes", "topic", "orders",
     "source spark-orders-stream: 'Spark Structured Streaming app orders-stream (Kafka orders -> Iceberg)'"),
    ("flink_job", "clickstream-enrich", "consumes", "topic", "clicks",
     "source flink-clickstream: 'Flink job clickstream-enrich (Kafka clicks -> MinIO)'"),
    ("dag", "daily_orders", "scheduled_by", "cluster", "airflow-main",
     "source airflow-main: 'Airflow (LocalExecutor). DAGs: daily_orders, customer_agg, hourly_quality_check'"),
    ("dag", "customer_agg", "scheduled_by", "cluster", "airflow-main",
     "source airflow-main: 'Airflow (LocalExecutor). DAGs: daily_orders, customer_agg, hourly_quality_check'"),
    ("dag", "hourly_quality_check", "scheduled_by", "cluster", "airflow-main",
     "source airflow-main: 'Airflow (LocalExecutor). DAGs: daily_orders, customer_agg, hourly_quality_check'"),
)

RELATIONSHIP_COLUMNS = ["child_kind", "child", "relation", "parent_kind", "parent"]


def edges_frame(edges=EXTRA_EDGES) -> pd.DataFrame:
    """The extra edges as a table shaped like the relationships table (without the justification)."""
    return pd.DataFrame([e[:5] for e in edges], columns=RELATIONSHIP_COLUMNS)
