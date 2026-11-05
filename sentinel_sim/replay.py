"""Replay API over the generated dataset.

This stands in for Prometheus, the Kubernetes API, Loki, Airflow and the Spark History
Server. Its methods match the RCA agent's tools in the design doc, so detector and
agent code written against `DataSource` can later run against real systems by
implementing the same methods.

Everything respects `now`: nothing after `now` is ever returned. Use `ds.at(ts)` to
move the clock, or `ds.stream(...)` to replay minute by minute.
"""
from __future__ import annotations

import gzip
import json
from functools import cached_property
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import pandas as pd

TS = pd.Timestamp


def _ts(x) -> pd.Timestamp:
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _dur(x) -> pd.Timedelta:
    return x if isinstance(x, pd.Timedelta) else pd.Timedelta(x)


class _Store:
    """Loaded once, shared by every DataSource clock view."""

    def __init__(self, data_dir: Path):
        self.dir = data_dir

    @cached_property
    def metrics(self) -> pd.DataFrame:
        df = pd.read_csv(self.dir / "metrics/metrics.csv.gz", index_col=0)
        df.index = pd.to_datetime(df.index, utc=True)
        return df

    @cached_property
    def catalog(self) -> pd.DataFrame:
        return pd.read_csv(self.dir / "catalog/series.csv").fillna("")

    @cached_property
    def topology(self) -> dict:
        return json.loads((self.dir / "catalog/topology.json").read_text())

    @cached_property
    def logs(self) -> pd.DataFrame:
        with gzip.open(self.dir / "logs/logs.jsonl.gz", "rt") as f:
            df = pd.DataFrame(json.loads(l) for l in f)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df

    def _jsonl(self, rel):
        with open(self.dir / rel) as f:
            return [json.loads(l) for l in f]

    @cached_property
    def events(self) -> pd.DataFrame:
        df = pd.json_normalize(self._jsonl("k8s/events.jsonl"))
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df.rename(columns={"involved_object.name": "object", "involved_object.kind": "object_kind",
                                  "involved_object.namespace": "namespace"})

    @cached_property
    def changes(self) -> list[dict]:
        return self._jsonl("changes/changes.jsonl")

    @cached_property
    def dag_runs(self) -> list[dict]:
        return self._jsonl("jobs/dag_runs.jsonl")

    @cached_property
    def spark_apps(self) -> dict[str, dict]:
        return {a["app_id"]: a for a in self._jsonl("jobs/spark_apps.jsonl")}


class DataSource:
    def __init__(self, data_dir: str | Path = "data", now=None, _store: _Store | None = None):
        self._s = _store or _Store(Path(data_dir))
        end = _ts(self._s.topology["time_range"]["end"])
        self.now = min(_ts(now), end) if now is not None else end

    # ------------------------------------------------------------------ clock
    def at(self, now) -> "DataSource":
        """Same data, different clock (cheap)."""
        return DataSource(now=now, _store=self._s)

    def _win(self, start=None, end=None, window=None):
        end = min(_ts(end), self.now) if end is not None else self.now
        if start is None:
            start = end - _dur(window or "1h")
        return _ts(start), end

    # ------------------------------------------------------------------ catalog
    def sources(self) -> list[dict]:
        return self._s.topology["sources"]

    def pods(self) -> pd.DataFrame:
        return pd.DataFrame(self._s.topology["pods"])

    def relationships(self) -> pd.DataFrame:
        return pd.DataFrame(self._s.topology["relationships"])

    def series(self, **filters) -> pd.DataFrame:
        """Series catalog, filtered by any column: layer, source_id, source_type, canonical_event,
        entity, entity_kind, metric. Values can be scalars or lists."""
        df = self._s.catalog
        for k, v in filters.items():
            df = df[df[k].isin(v if isinstance(v, (list, tuple, set)) else [v])]
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------ metrics
    def query(self, series: str | Iterable[str], start=None, end=None, window=None, step="1min") -> pd.DataFrame:
        """Wide DataFrame (index = ts, one column per series_id), never past `now`.
        step > 1min averages within each step."""
        cols = [series] if isinstance(series, str) else list(series)
        start, end = self._win(start, end, window)
        df = self._s.metrics.loc[start:end - pd.Timedelta("1ns"), cols]
        if _dur(step) > pd.Timedelta("1min"):
            df = df.resample(step).mean()
        return df

    def query_metric(self, series_id: str, window="6h", end=None) -> dict:
        """Agent tool: a compact, LLM-friendly summary of one series (not raw points)."""
        start, end = self._win(None, end, window)
        s = self.query(series_id, start, end)[series_id].dropna()
        meta = self._s.catalog.set_index("series_id").loc[series_id]
        if s.empty:
            return {"series_id": series_id, "points": 0, "note": "no data in window (pod absent or not running)"}
        y = self.query(series_id, start - pd.Timedelta("1D"), end - pd.Timedelta("1D"))[series_id].dropna()
        x = (s.index - s.index[0]).total_seconds() / 3600
        slope = float(np.polyfit(x, s.values, 1)[0]) if len(s) > 2 else 0.0
        last30 = s[s.index >= end - pd.Timedelta("30min")]
        return {"series_id": series_id, "unit": meta["unit"], "window": f"{start} -> {end}", "points": int(len(s)),
                "min": round(float(s.min()), 4), "max": round(float(s.max()), 4), "mean": round(float(s.mean()), 4),
                "last": round(float(s.iloc[-1]), 4), "last_30m_mean": round(float(last30.mean()), 4),
                "slope_per_hour": round(slope, 4), "max_at": str(s.idxmax()),
                "same_window_yesterday_mean": round(float(y.mean()), 4) if not y.empty else None}

    def stream(self, start, end=None, step="1min", series: Iterable[str] | None = None) -> Iterator[tuple]:
        """Replay minute by minute: yields (ts, pandas Series of values at ts) for online detectors."""
        cols = list(series) if series is not None else list(self._s.metrics.columns)
        end = _ts(end) if end is not None else self.now
        block = self._s.metrics.loc[_ts(start):end - pd.Timedelta("1ns"), cols]
        if _dur(step) > pd.Timedelta("1min"):
            block = block.resample(step).last()
        for ts, row in block.iterrows():
            yield ts, row

    # ------------------------------------------------------------------ logs / events
    def logs(self, pod: str | None = None, start=None, end=None, window="30min", level: str | list | None = None,
             contains: str | None = None, limit: int | None = 5000) -> pd.DataFrame:
        """Raw log lines (like `kubectl logs` / LogQL). Feed these to Drain3 for templates."""
        start, end = self._win(start, end, window)
        df = self._s.logs
        m = (df.ts >= start) & (df.ts < end)
        if pod:
            m &= df.pod == pod
        if level:
            m &= df.level.isin([level] if isinstance(level, str) else level)
        if contains:
            m &= df.message.str.contains(contains, regex=False)
        out = df[m]
        return out.tail(limit) if limit else out

    def k8s_events(self, pod: str | None = None, start=None, end=None, window="2h", reason=None,
                   type_: str | None = None) -> pd.DataFrame:
        start, end = self._win(start, end, window)
        df = self._s.events
        m = (df.ts >= start) & (df.ts < end)
        if pod:
            m &= df.object == pod
        if reason:
            m &= df.reason.isin([reason] if isinstance(reason, str) else reason)
        if type_:
            m &= df.type == type_
        return df[m]

    def changes(self, start=None, end=None, window="24h", target: str | None = None) -> list[dict]:
        """Deploys, config changes, scaling, DAG pauses, maintenance, annotations."""
        start, end = self._win(start, end, window)
        return [c for c in self._s.changes if start <= _ts(c["ts"]) < end
                and (target is None or target in c["target"])]

    def pod_spec(self, pod: str, at=None) -> dict:
        """Pod resources, image and restart count as of `at` (default now), with changes applied."""
        at = min(_ts(at), self.now) if at is not None else self.now
        p = next(x for x in self._s.topology["pods"] if x["pod"] == pod)
        spec = {"pod": pod, "namespace": p["namespace"], "role": p["role"], "source_id": p["source_id"],
                "cpu_limit_cores": p["cpu_limit_cores"], "memory_limit_bytes": p["memory_limit_bytes"],
                "image": self._s.topology["images"].get(pod), "env": {}}
        group = pod.rsplit("-", 1)[0] if pod.startswith("orders-etl-consumer") else pod
        for c in self._s.changes:
            if _ts(c["ts"]) > at or c["kind"] not in ("deploy", "config", "flink_job_update"):
                continue
            if c["target"] in (pod, group) or (c["target"] == "trino-worker" and pod.startswith("trino-worker")):
                for k, v in c["details"].items():
                    if k == "image":
                        spec["image"] = "registry.local/" + v[1] if "/" not in v[1] else v[1]
                    elif k.startswith("env."):
                        spec["env"][k[4:]] = v[1]
                    else:
                        spec.setdefault("config", {})[k] = v[1]
        if p["batch_only"]:
            job = pod.rsplit("-", 2)[0] if "exec" in pod else pod.rsplit("-", 1)[0]
            cfg = self.spark_config(job, at)
            spec["spark_config"] = cfg
            if "exec" in pod:
                mem = float(cfg["spark.executor.memory"][:-1]) + float(cfg["spark.executor.memoryOverhead"][:-1])
                spec["memory_limit_bytes"] = int(mem * 1024 ** 3)
        rs = self.query(f"kube_pod_container_status_restarts_total|{pod}", end=at, window="7D").iloc[:, 0].dropna()
        spec["restart_count"] = int(rs.iloc[-1]) if not rs.empty else 0
        return spec

    def spark_config(self, job: str, at=None) -> dict:
        at = min(_ts(at), self.now) if at is not None else self.now
        cfg = dict(self._s.topology["spark_base_config"][job])
        for c in self._s.changes:
            if c["kind"] == "spark_config" and c["target"] == job and _ts(c["ts"]) <= at:
                cfg.update({k: v[1] for k, v in c["details"].items()})
        return cfg

    # ------------------------------------------------------------------ jobs
    def job_runs(self, dag_id: str | None = None, start=None, end=None, window="7D") -> pd.DataFrame:
        """Flat table of DAG runs that had started by `now`. Runs still in progress at `now` show
        state='running' and no end."""
        start, end = self._win(start, end, window)
        rows = []
        for r in self._s.dag_runs:
            s = _ts(r["start"])
            if (dag_id and r["dag_id"] != dag_id) or not (start <= s < end):
                continue
            e = _ts(r["end"])
            done = e <= self.now
            app_ids = [t["spark_app_id"] for t in r["tasks"] if t.get("spark_app_id") and _ts(t["start"]) <= self.now]
            rows.append({"dag_id": r["dag_id"], "run_id": r["run_id"], "run_type": r["run_type"],
                         "scheduled_for": r["scheduled_for"], "start": r["start"], "end": r["end"] if done else None,
                         "duration_min": round((e - s).total_seconds() / 60, 2) if done else None,
                         "lateness_min": round((s - _ts(r["scheduled_for"])).total_seconds() / 60, 2),
                         "state": r["state"] if done else "running",
                         "sla_missed": r["sla_missed"] if done else None, "spark_app_ids": app_ids})
        return pd.DataFrame(rows)

    def job_history(self, dag_id: str, n: int = 14) -> pd.DataFrame:
        """Agent tool: last n completed scheduled runs with their Spark input sizes and errors."""
        df = self.job_runs(dag_id, window="60D")
        df = df[(df.state != "running") & (df.run_type == "scheduled")].tail(n).copy()
        apps = self._s.spark_apps
        df["input_rows"] = [apps[i[-1]]["input_rows"] if i else None for i in df.spark_app_ids]
        df["error"] = [apps[i[-1]]["error"][:160] if i and apps[i[-1]]["error"] else None for i in df.spark_app_ids]
        return df

    def expected_runs(self, start=None, end=None, window="24h") -> pd.DataFrame:
        """Scheduled run times for each DAG in the window (for the schedule watcher)."""
        start, end = self._win(start, end, window)
        rows = []
        for dag, d in self._s.topology["dags"].items():
            mm, hh = d["schedule"].split()[:2]
            hours = range(24) if hh == "*" else [int(hh)]
            day = start.normalize()
            while day < end:
                for h in hours:
                    t = day + pd.Timedelta(hours=h, minutes=int(mm))
                    if start <= t < end:
                        rows.append({"dag_id": dag, "scheduled_for": t, "sla_minutes": d["sla_minutes"]})
                day += pd.Timedelta("1D")
        return pd.DataFrame(rows)

    def dag_run(self, run_id: str, dag_id: str | None = None) -> dict | None:
        for r in self._s.dag_runs:
            if r["run_id"] == run_id and (dag_id is None or r["dag_id"] == dag_id) and _ts(r["start"]) <= self.now:
                r = json.loads(json.dumps(r))
                r["tasks"] = [t for t in r["tasks"] if _ts(t["start"]) <= self.now]
                if _ts(r["end"]) > self.now:
                    r["state"], r["end"] = "running", None
                return r
        return None

    def spark_app(self, app_id: str) -> dict | None:
        """Agent tool: Spark History Server view of one application."""
        a = self._s.spark_apps.get(app_id)
        if a is None or _ts(a["start"]) > self.now:
            return None
        if _ts(a["end"]) > self.now:
            a = {**a, "state": "RUNNING", "end": None, "error": None,
                 "stages": [s for s in a["stages"] if s["stage_id"] < 2]}
        return a

    def kafka_group(self, group: str = "orders-etl", at=None) -> dict:
        """Agent tool: per-partition lag and member count for a consumer group."""
        at = min(_ts(at), self.now) if at is not None else self.now
        sid = f"kafka_consumergroup_lag|{group}"
        lag = self.query(sid, end=at, window="5min")[sid].dropna()
        total = float(lag.iloc[-1]) if not lag.empty else 0.0
        w = np.array([0.19, 0.17, 0.16, 0.18, 0.15, 0.15])
        members = sum(1 for p in ("orders-etl-consumer-0", "orders-etl-consumer-1")
                      if not np.isnan(self.query(f"container_cpu_usage_cores|{p}", end=at, window="2min").iloc[-1, 0]))
        return {"group": group, "topic": "orders", "partitions": 6, "members": members, "total_lag": int(total),
                "lag_by_partition": {f"orders-{i}": int(total * w[i]) for i in range(6)},
                "max_useful_consumers": 6}


def load_labels(data_dir: str | Path = "data") -> tuple[list[dict], pd.DataFrame]:
    """Ground truth. For evaluation only - never give these to detectors or the agent."""
    d = Path(data_dir)
    return json.loads((d / "labels/incidents.json").read_text()), pd.read_csv(d / "labels/anomaly_windows.csv")
