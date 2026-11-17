"""Stream v2: the logs, Kubernetes events and change records a live scenario's failure would produce.

In stream version 1 an injection changes readings only (and, for a paused DAG, removes its runs);
logs, events and changes are not on the stream. In version 2 they are (wire.py), and each named
scenario adds what its failure would leave there. The templates are the batch scenarios' own
(sentinel_sim/scenarios.py); what each live scenario emits:

    producer_crashloop     a deploy record; per restart (the restart counter's steps): a DNS WARN, the
                           startup ERROR and a BackOff event; NO other log line from the pod while it is
                           down; at the stop a rollback record, the old image pulled and started
    consumer_slow_deploy   a deploy record; per pod restart (the rollout): Killing, Pulled, Started;
                           "fraud-api call slow" WARN lines; a rollback record at the stop
    consumer_memory_leak   a deploy record; "customer profile cache size" lines growing every 30 minutes;
                           an OOMKilled and a Started event when a pod reaches its limit; a rollback record
    flink_state_leak       a job-update record; RocksDB live-data-size lines growing every hour; on an OOM
                           kill the OOMKilled event and the JobManager's restart lines; a rollback record
    broker_gc_pressure     a deploy record of the client that causes it; its 500 MB fetch lines and GC-pause
                           WARN lines on the broker; at each fall out of the in-sync replicas an ISR-shrink
                           line on the leader and a fetch timeout on the broker; a client-quota record at the stop
    trino_heavy_query      the query's "created" line (a comma join with no join condition), queries failing
                           with EXCEEDED_LOCAL_MEMORY_LIMIT, the query cancelled at the stop
    dag_paused             a dag_pause record and a dag_unpause record at the stop; the missing runs leave
                           no scheduler or task line and no Spark pod event (that removal is in injector.py,
                           which knows which runs were dropped)

Evidence is driven by what the injected readings do (each restart step, each OOM reset, each
under-replicated flap), so the two cannot disagree. It has its own random generator, seeded with the
injection id, so stream v2's readings are exactly stream v1's.

What it must never contain: a scenario name, an injection id, a parameter name or anything else only
the answer key knows. Records have the same fields as the data directory's own (tests/test_live_evidence.py).
Generic (primitive-effect) injections get no evidence: they stay metrics-only, as a novel failure
would be.
"""
from __future__ import annotations

import string
import zlib

import numpy as np
import pandas as pd

from sentinel_stream import wire

SECOND, MINUTE = pd.Timedelta("1s"), pd.Timedelta("1min")
METRICS, DAG_RUN = "metrics", "dag_run"


def _iso(t) -> str:
    return pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ")


class Every:
    """Lines that recur every `period` from `first`, each a little later than its slot (up to
    `jitter` of the period), as the batch generator's `_logs_every` does."""

    def __init__(self, first, period: pd.Timedelta, jitter: float = 0.5):
        self.next, self.period, self.jitter, self.k = first, period, jitter, 0

    def due(self, ts, rng) -> list:
        """[(k, time)] of the slots that begin in this minute or earlier and have not been given yet."""
        out = []
        while self.next < ts + MINUTE:
            out.append((self.k, self.next + self.period * float(rng.uniform(0, self.jitter))))
            self.next, self.k = self.next + self.period, self.k + 1
        return out


class Evidence:
    """What one injection adds to the logs, events and changes. The injector calls begin() when the
    injection starts, minute(ts, happened) every streamed minute while it runs, finish(at) at its stop,
    and silences(kind, record) for every streamed record stamped while it runs."""

    kinds: tuple = ()                 # the message types it changes besides metrics (for the answer key)

    def __init__(self, inj, env):
        self.inj, self.env = inj, env
        self.target, self.params, self.start = inj.target, inj.params, inj.start
        self.rng = np.random.default_rng(zlib.crc32((inj.id + "|evidence").encode()))
        self.out: list = []

    # -------------------------------------------------------------------------------- records
    def log(self, t, pod, level, logger, message) -> None:
        self.out.append((wire.LOGS, {"ts": _iso(t), "pod": pod, "level": level, "logger": logger, "message": message}))

    def event(self, t, pod, reason, message, type_="Normal") -> None:
        self.out.append((wire.K8S_EVENTS, {"ts": _iso(t), "type": type_, "reason": reason,
                                           "involved_object": {"kind": "Pod", "name": pod, "namespace": self.env.namespace(pod)},
                                           "message": message, "count": 1}))

    def change(self, t, kind, target, details, author, description) -> None:
        self.out.append((wire.CHANGES, {"change_id": self.env.change_id(), "ts": _iso(t), "kind": kind, "target": target,
                                        "details": details, "author": author, "description": description}))

    def take(self) -> list:
        out, self.out = self.out, []
        return out

    def seconds(self, lo, hi) -> pd.Timedelta:
        return SECOND * float(self.rng.uniform(lo, hi))

    # ---------------------------------------------------------------------------------- hooks
    def begin(self) -> None:
        pass

    def minute(self, ts, happened: list) -> None:
        """happened: [("restart", pod), ...] one per restart-counter step this minute, and
        [("oom", pod), ...] for a leak that reached its memory limit this minute."""

    def finish(self, at) -> None:
        pass

    def silences(self, kind: str, record: dict) -> bool:
        return False


# ------------------------------------------------------------------------------------- scenarios
class ProducerCrashloop(Evidence):
    kinds = (METRICS, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES)
    GOOD, BAD = "kafka-ingest-kafka-bootstrap.kafka.svc:9092", "kafka-ingest-kafka-bootstrp.kafka.svc:9092"

    def begin(self):
        self.change(self.start, "deploy", self.target, {"image": [f"{self.target}:2.2.4", f"{self.target}:2.3.0"],
                                                        "env.KAFKA_BOOTSTRAP": [self.GOOD, self.BAD]},
                    "d.wu", "Move producer config to env vars")

    def minute(self, ts, happened):
        for what, pod in happened:
            if what != "restart" or pod != self.target:
                continue
            t = ts + self.seconds(2, 40)
            self.log(t, pod, "WARN", "org.apache.kafka.clients.ClientUtils",
                     f"Couldn't resolve server {self.BAD} from bootstrap.servers as DNS resolution failed for {self.BAD.split(':')[0]}")
            self.log(t + SECOND, pod, "ERROR", "orders_producer.main",
                     "Exception in thread \"main\" org.apache.kafka.common.KafkaException: Failed to construct kafka producer\n"
                     "Caused by: org.apache.kafka.common.config.ConfigException: No resolvable bootstrap urls given in bootstrap.servers")
            self.event(t + 3 * SECOND, pod, "BackOff", f"Back-off restarting failed container producer in pod {pod}", "Warning")

    def finish(self, at):
        self.change(at, "deploy", self.target, {"image": [f"{self.target}:2.3.0", f"{self.target}:2.2.4"]},
                    "oncall-sre", f"Roll back {self.target} to 2.2.4")
        self.event(at + self.seconds(5, 15), self.target, "Pulled",
                   f"Container image \"registry.local/{self.target}:2.2.4\" already present on machine")
        self.event(at + self.seconds(16, 25), self.target, "Started", "Started container producer")

    def silences(self, kind, record):
        return kind == wire.LOGS and record["pod"] == self.target           # the pod is down: it logs nothing


class ConsumerSlowDeploy(Evidence):
    kinds = (METRICS, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES)
    APP = "orders-etl-consumer"
    PODS = ("orders-etl-consumer-0", "orders-etl-consumer-1")

    def begin(self):
        self.change(self.start, "deploy", self.APP, {"image": [f"{self.APP}:1.7.3", f"{self.APP}:1.8.0"]}, "r.gomez",
                    "Add per-order fraud score enrichment (calls fraud-api synchronously)")
        self.every = {p: Every(self.start + MINUTE, 1.5 * MINUTE) for p in self.PODS}

    def minute(self, ts, happened):
        for what, pod in happened:
            if what == "restart" and pod in self.PODS:                   # the rollout
                t = ts + self.seconds(2, 10)
                self.event(t, pod, "Killing", "Stopping container consumer")
                self.event(t + self.seconds(8, 15), pod, "Pulled", f"Successfully pulled image \"registry.local/{self.APP}:1.8.0\"")
                self.event(t + self.seconds(20, 30), pod, "Started", "Started container consumer")
        slow = float(self.params.get("magnitude", 1.0))
        for pod, every in self.every.items():
            for _, t in every.due(ts, self.rng):
                self.log(t, pod, "WARN", "orders_etl.enrich",
                         f"fraud-api call slow: {int(self.rng.uniform(380, 900) * slow)} ms (order batch of 50)")

    def finish(self, at):
        self.change(at, "deploy", self.APP, {"image": [f"{self.APP}:1.8.0", f"{self.APP}:1.7.3"]}, "oncall-sre",
                    f"Roll back {self.APP} to 1.7.3")


class ConsumerMemoryLeak(Evidence):
    kinds = (METRICS, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES)
    APP = "orders-etl-consumer"

    def begin(self):
        self.change(self.start, "deploy", self.APP, {"image": [f"{self.APP}:1.7.3", f"{self.APP}:1.8.1"]}, "r.gomez",
                    "Cache customer profiles in-process to cut DB lookups")
        self.every = {f"{self.APP}-{n}": Every(self.start + 30 * MINUTE, 30 * MINUTE) for n in (0, 1)}

    def minute(self, ts, happened):
        for what, pod in happened:
            if what == "oom" and pod in self.every:
                t = ts + self.seconds(1, 20)
                self.event(t, pod, "OOMKilled", f"Container consumer in pod {pod} was OOMKilled (exit code 137, memory limit 1Gi)",
                           "Warning")
                self.event(t + self.seconds(15, 25), pod, "Started", "Started container consumer")
                self.every[pod] = Every(ts + 30 * MINUTE, 30 * MINUTE)        # the cache starts empty again
        for pod, every in self.every.items():
            for k, t in every.due(ts, self.rng):
                self.log(t, pod, "INFO", "orders_etl.cache", f"customer profile cache size={(k + 1) * 9100} entries (no eviction policy)")

    def finish(self, at):
        self.change(at, "deploy", self.APP, {"image": [f"{self.APP}:1.8.1", f"{self.APP}:1.7.3"]}, "oncall-sre",
                    f"Roll back {self.APP} to 1.7.3")


class FlinkStateLeak(Evidence):
    kinds = (METRICS, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES)
    TMS = ("flink-taskmanager-0", "flink-taskmanager-1")

    def begin(self):
        self.change(self.start, "flink_job_update", self.target,
                    {"jar": [f"{self.target}-3.1.0.jar", f"{self.target}-3.2.0.jar"]}, "t.ivanova",
                    "Add sessionization operator (keyed state per user)")
        self.every = {tm: Every(self.start + 60 * MINUTE, 60 * MINUTE) for tm in self.TMS}

    def minute(self, ts, happened):
        for what, pod in happened:
            if what == "oom" and pod in self.every:
                t = ts + self.seconds(1, 20)
                self.event(t, pod, "OOMKilled", f"Container flink-main-container in pod {pod} was OOMKilled "
                                                "(exit code 137, memory limit 4Gi)", "Warning")
                self.log(t + 12 * SECOND, "flink-jobmanager", "INFO", "org.apache.flink.runtime.executiongraph.ExecutionGraph",
                         f"Task 'Sessionize -> Sink: MinIO' switched from RUNNING to FAILED. org.apache.flink.runtime.jobmaster."
                         f"JobMasterException: TaskManager with id {pod}:6122 is no longer reachable.")
                self.log(t + 18 * SECOND, "flink-jobmanager", "INFO", "org.apache.flink.runtime.jobmaster.JobMaster",
                         f"Restarting job {self.target} (restart strategy: exponential-delay)")
                self.log(t + 90 * SECOND, "flink-jobmanager", "INFO", "org.apache.flink.runtime.checkpoint.CheckpointCoordinator",
                         f"Restoring job {self.target} from Checkpoint {int(self.rng.integers(9000, 9900))}")
                self.every[pod] = Every(ts + 60 * MINUTE, 60 * MINUTE)
        for n, (pod, every) in enumerate(self.every.items()):
            rate = float(self.params.get(f"rate_tm{n}_mib_per_hour", 75.0))
            for k, t in every.due(ts, self.rng):
                self.log(t, pod, "INFO", "org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor",
                         f"RocksDB estimate-live-data-size={0.4 + (k + 1) * rate / 1024:.2f} GB for operator 'Sessionize' "
                         "(state.backend.rocksdb.memory.managed=false)")

    def finish(self, at):
        self.change(at, "flink_job_update", self.target, {"jar": [f"{self.target}-3.2.0.jar", f"{self.target}-3.1.0.jar"]},
                    "t.ivanova", f"Roll back {self.target} to 3.1.0")


class BrokerGcPressure(Evidence):
    kinds = (METRICS, wire.LOGS, wire.CHANGES)
    CLIENT = "finance-reporting-consumer"

    def begin(self):
        self.broker = int(self.target.rsplit("-", 1)[1])
        self.leader = (self.broker + 2) % 3
        self.isr = ",".join(str(b) for b in range(3) if b != self.broker)
        self.change(self.start, "deploy", self.CLIENT, {"replicas": [0, 4], "auto.offset.reset": "earliest",
                                                        "fetch.max.bytes": "524288000"}, "b.okafor",
                    "Launch finance reporting consumer (initial backfill of orders from offset 0)")
        self.fetch = Every(self.start + 2 * MINUTE, 4 * MINUTE)
        self.gc = Every(self.start + 5 * MINUTE, 3 * MINUTE)
        flap = next((r for r in self.inj.runs if r.effect.flap and r.sid.startswith("kafka_under_replicated_partitions|")), None)
        self.flaps = [] if flap is None else [flap.t0 + MINUTE * a for a, _ in flap.on if flap.t0 + MINUTE * a < flap.t1]

    def minute(self, ts, happened):
        size = float(self.params.get("magnitude", 1.0))
        for k, t in self.fetch.due(ts, self.rng):
            self.log(t, self.target, "INFO", "kafka.server.KafkaApis",
                     f"[KafkaApi-{self.broker}] Fetch request from client finance-reporting-{k % 4} for orders offset "
                     f"{k * 250000} returned 500 MB (fetch.max.bytes=524288000)")
        for _, t in self.gc.due(ts, self.rng):
            self.log(t, self.target, "WARN", "gc", f"[GC pause (G1 Evacuation Pause) (young) "
                                                   f"{self.rng.uniform(1.8, 4.2) * size:.2f}s] heap 3584M->3410M(3584M)")
        for start in [f for f in self.flaps if ts <= f < ts + MINUTE]:
            p = int(self.rng.integers(0, 6))
            self.log(start + self.seconds(1, 10), f"kafka-{self.leader}", "INFO", "kafka.cluster.Partition",
                     f"[Partition orders-{p} broker={self.leader}] Shrinking ISR from 0,1,2 to {self.isr}. Out of sync replicas: "
                     f"(brokerId: {self.broker}, endOffset: {int(self.rng.integers(90_000_000, 99_000_000))}, "
                     f"lastCaughtUpTimeMs: {int(self.rng.integers(30_000, 40_000))})")
            self.log(start + self.seconds(11, 20), self.target, "WARN", "kafka.server.ReplicaFetcherThread",
                     f"[ReplicaFetcher replicaId={self.broker}, leaderId={self.leader}, fetcherId=0] Error sending fetch request "
                     f"(sessionId={int(self.rng.integers(10**8, 10**9))}) to node {self.leader}: java.net.SocketTimeoutException")

    def finish(self, at):
        self.change(at, "kafka_quota", self.CLIENT, {"consumer_byte_rate": [None, "20MB/s"]}, "oncall-sre",
                    f"Throttle {self.CLIENT} with a client quota")


class TrinoHeavyQuery(Evidence):
    kinds = (METRICS, wire.LOGS)

    def _query_id(self, t) -> str:
        tail = "".join(self.rng.choice(list(string.ascii_lowercase + string.digits), 5))
        return f"{pd.Timestamp(t):%Y%m%d_%H%M%S}_{int(self.rng.integers(100, 99999)):05d}_{tail}"

    def begin(self):
        self.qid = self._query_id(self.start)
        self.log(self.start + self.seconds(0, 20), "trino-coordinator", "INFO", "io.trino.event.QueryMonitor",
                 f"Query {self.qid} created: user=analyst.rhea source=trino-cli resource_group=global.adhoc "
                 "sql=SELECT o.*, c.* FROM lake.orders_enriched o, lake.customers c WHERE o.order_ts > date '2026-01-01'")
        self.failing = Every(self.start + 6 * MINUTE, 1.2 * MINUTE / max(float(self.params.get("magnitude", 1.0)), 0.1))

    def minute(self, ts, happened):
        for _, t in self.failing.due(ts, self.rng):
            self.log(t, "trino-coordinator", "ERROR", "io.trino.event.QueryMonitor",
                     f"Query {self._query_id(t)} failed: EXCEEDED_LOCAL_MEMORY_LIMIT: Query exceeded per-node memory limit of 4GB "
                     "[Allocated: 4.00GB, Delta: 11.2MB, Top Consumers: {HashBuilderOperator=3.71GB}]")

    def finish(self, at):
        self.log(at, "trino-coordinator", "INFO", "io.trino.event.QueryMonitor",
                 f"Query {self.qid} failed: USER_CANCELED Query was canceled by oncall-sre")


class DagPaused(Evidence):
    kinds = (METRICS, DAG_RUN, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES)

    def begin(self):
        self.change(self.start, "dag_pause", self.target, {"is_paused": [False, True]}, "m.ortiz",
                    f"Pause {self.target} during warehouse table migration")

    def finish(self, at):
        self.change(at, "dag_unpause", self.target, {"is_paused": [True, False]}, "m.ortiz", f"Unpause {self.target}")


BY_SCENARIO = {"producer_crashloop": ProducerCrashloop, "consumer_slow_deploy": ConsumerSlowDeploy,
               "consumer_memory_leak": ConsumerMemoryLeak, "flink_state_leak": FlinkStateLeak,
               "broker_gc_pressure": BrokerGcPressure, "trino_heavy_query": TrinoHeavyQuery, "dag_paused": DagPaused}
METRICS_ONLY = (METRICS,)


def for_injection(inj, env) -> Evidence | None:
    """The evidence of a named injection, or None (generic effects, the rehearsal): metrics only."""
    cls = BY_SCENARIO.get(inj.scenario.name)
    return None if cls is None else cls(inj, env)
