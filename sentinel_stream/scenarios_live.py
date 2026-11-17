"""Failure scenarios for the live stream, as declarative effect specs.

The batch generator (sentinel_sim/scenarios.py) writes a scenario into a whole dataset before the
platform "physics" runs. Live, there is only the stream: an injection changes the readings the
publisher sends, from the injection time on. So each scenario here is a list of EFFECTS on named
series, in the vocabulary of the batch generator's `_bump`:

    op      add    add `amount` to the reading            mult   multiply the reading by `amount`
            drop   send no reading (the pod is not running)
    shape   flat   full size for the whole injection, easing in and out over `rise` of its length
            ramp   grows from zero: `amount` is per HOUR (add), or reached at the planned end (mult)
            decay  jumps to full size, then falls off as exp(-4 x) over its length

Sizes. A scenario at `magnitude` 1.0 has the sizes of its batch original; every amount with
`scale="magnitude"` is multiplied by the drawn magnitude (a factor f becomes 1 + (f - 1) * magnitude).
Ramps take their speed from a parameter of their own. Each scenario declares the RANGE of every
parameter; an injection must give values inside the ranges.

Ranges, and where they come from. They were fixed before any scenario was run through the engine and
are not tuned to what the detectors catch:

    magnitude      0.5 - 1.2 of the batch scenario
    durations      around the batch duration, except the two leaks: the batch leaks last 27 and 34
                   hours, which does not fit a week with two injections a day, so they last 4 - 10 hours
    ramp speeds    around the batch speeds (leak rates), or around what the batch physics produced on
                   the train split (consumer lag)

What is approximated. The batch scenarios change causes and let `kafka_physics` and `finalize` turn
them into symptoms. Here the symptoms are written down directly; each scenario lists what that loses
in `approximations`. Two things are still computed, not declared, because they depend on the reading
itself: a leak that reaches `reset_at` (the container's memory limit) is OOM-killed (the ramp starts
again from zero and `on_reset` effects fire), and CPU throttling follows CPU with the generator's own
formula (see injector.py). Logs, Kubernetes events and change records are not produced here: the
engine does not read them. In stream version 2 they are added by evidence_live.py, from what the
effects declared here do (restarts, OOM resets, flaps).

`REHEARSAL` is not a failure scenario. It exists to test the machinery without learning anything
about how detectable the seven are.
"""
from __future__ import annotations

from dataclasses import dataclass, field

MiB, GiB = 1024 ** 2, 1024 ** 3
OPS, SHAPES, PHASES = ("add", "mult", "drop"), ("flat", "ramp", "decay"), ("during", "after")


@dataclass(frozen=True)
class Effect:
    series: str                     # series id; {target} and {job} are filled in, * matches any series
    op: str = "add"
    amount: object = 0.0            # a number, or the name of a parameter
    shape: str = "flat"
    scale: str | None = None        # name of a parameter that scales the amount (see "Sizes")
    unit: float = 1.0               # multiplies the amount (e.g. MiB)
    phase: str = "during"           # during: from the injection's start to its stop.  after: from the stop on
    delay_min: float = 0.0          # starts this long after the phase begins
    minutes: float | None = None    # length; needed for phase "after" and for `on_reset` effects
    end_before_min: float = 0.0     # during: ends this long before the stop
    rise: float = 0.15              # flat: share of the length spent easing in, and again easing out
    jitter: float = 0.07            # the size varies by this much (std) from minute to minute, as in the batch `_bump`
    release_min: float = 0.0        # add: after the stop, what was added falls off (decay) over this long
    cap: float | None = None        # the resulting reading is clipped just below this
    counter: bool = False           # the series is a counter: whole steps only, and they stay after the stop
    reset_at: float | None = None   # add + ramp: when the reading reaches this, start again from zero (OOM kill)
    flap: tuple | None = None       # ((on_lo, on_hi), (off_lo, off_hi)) minutes: the effect switches on and off

    def __post_init__(self):
        if self.op not in OPS or self.shape not in SHAPES or self.phase not in PHASES:
            raise ValueError(f"bad effect spec: {self}")
        if self.phase == "after" and self.minutes is None:
            raise ValueError(f"an 'after' effect needs a length in minutes: {self}")


@dataclass(frozen=True)
class Scenario:
    name: str
    batch: str                      # the function in sentinel_sim/scenarios.py it is ported from
    summary: str
    targets: tuple                  # what it can be injected on; one is chosen per injection
    params: dict                    # parameter -> (low, high); always has duration_min
    effects: tuple
    on_reset: tuple = ()            # fired when an effect with `reset_at` resets; {hit} is the pod it happened to
    pauses_dag: bool = False        # runs of the target DAG that would start during the injection do not happen
    approximations: tuple = field(default=(), compare=False)

    def midpoints(self) -> dict:
        return {k: (lo + hi) / 2 for k, (lo, hi) in self.params.items()}

    def check(self, target: str, params: dict) -> dict:
        """The complete, validated parameters of one injection: missing ones are the midpoint of
        their range, unknown names and values outside the declared range are refused."""
        if target not in self.targets:
            raise ValueError(f"{self.name}: target must be one of {self.targets}, got {target!r}")
        unknown = sorted(set(params) - set(self.params))
        if unknown:
            raise ValueError(f"{self.name}: unknown parameter(s) {unknown}; it has {sorted(self.params)}")
        out = {**self.midpoints(), **{k: float(v) for k, v in params.items()}}
        for k, (lo, hi) in self.params.items():
            if not lo <= out[k] <= hi:
                raise ValueError(f"{self.name}: {k}={out[k]} is outside its range {lo} - {hi}")
        return out


MAGNITUDE = (0.5, 1.2)
RESTARTS = "kube_pod_container_status_restarts_total|"

CONSUMER_MEMORY_LEAK = Scenario(
    name="consumer_memory_leak", batch="consumer_memory_leak",
    summary="A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.",
    targets=("orders-etl-consumer",),
    params={"rate_pod0_mib_per_hour": (40, 80), "rate_pod1_mib_per_hour": (40, 80), "duration_min": (240, 600)},
    effects=tuple(Effect(f"container_memory_working_set_bytes|orders-etl-consumer-{n}", "add", f"rate_pod{n}_mib_per_hour",
                         "ramp", unit=MiB, jitter=0.0, reset_at=1 * GiB) for n in (0, 1)),
    on_reset=(Effect(RESTARTS + "{hit}", "add", 1, counter=True, minutes=1),),
    approximations=(
        "Batch: 48 and 61 MiB/hour for 27 hours, so each pod is OOM-killed at its 1 GiB limit about every 11 hours. "
        "Live: 40-80 MiB/hour for 4-10 hours; a kill only happens when rate x duration reaches the limit.",
        "Batch replaces the memory series by 400 MiB + growth; live adds the growth to the reading that is there.",
        "The 2-minute halving of consumer capacity after an OOM kill (a small lag blip through the physics) is left out.",
        "At the stop (rollback) memory returns to the base reading at once.",
    ))

FLINK_STATE_LEAK = Scenario(
    name="flink_state_leak", batch="flink_state_leak",
    summary="A Flink job update keeps keyed state forever: both TaskManagers' memory grows off-heap.",
    targets=("clickstream-enrich",),
    params={"rate_tm0_mib_per_hour": (50, 100), "rate_tm1_mib_per_hour": (50, 100), "duration_min": (240, 600)},
    effects=tuple(Effect(f"container_memory_working_set_bytes|flink-taskmanager-{n}", "add", f"rate_tm{n}_mib_per_hour",
                         "ramp", unit=MiB, jitter=0.0, reset_at=4 * GiB) for n in (0, 1)),
    on_reset=(Effect(RESTARTS + "{hit}", "add", 1, counter=True, minutes=1),
              Effect("flink_job_restarts_total|clickstream-enrich", "add", 1, counter=True, minutes=1),
              Effect("flink_records_out_per_sec|clickstream-enrich", "mult", 0.05, minutes=6, rise=0.05),
              Effect("flink_backpressure_ratio|clickstream-enrich", "add", 0.5, "decay", delay_min=5, minutes=20)),
    approximations=(
        "Batch: 55 and 85 MiB/hour for 34 hours, OOM-killed at the 4 GiB limit. Live: 50-100 MiB/hour for 4-10 hours, "
        "which never reaches the limit from the usual 2.4-2.7 GiB, so the kill and the job restart do not happen "
        "inside the declared ranges (they are implemented, for longer manual injections).",
        "Batch replaces the memory series by 2.4 GiB + growth; live adds the growth to the reading that is there.",
        "As in the batch scenario the JVM heap series is not touched (the leak is off-heap).",
    ))

CONSUMER_SLOW_DEPLOY = Scenario(
    name="consumer_slow_deploy", batch="consumer_slow_deploy",
    summary="A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.",
    targets=("orders-etl",),
    params={"lag_per_hour": (200_000, 900_000), "magnitude": MAGNITUDE, "duration_min": (120, 240)},
    effects=(
        Effect("kafka_consumergroup_lag|orders-etl", "add", "lag_per_hour", "ramp", release_min=30),
        Effect("kafka_consumer_records_consumed_per_sec|orders-etl", "mult", 0.85, scale="magnitude", rise=0.02, jitter=0.01),
        Effect("kafka_consumer_records_consumed_per_sec|orders-etl", "mult", 1.85, phase="after", minutes=30, rise=0.05, jitter=0.01),
        *(e for n in (0, 1) for e in (
            Effect(f"{RESTARTS}orders-etl-consumer-{n}", "add", 1, counter=True),              # the rollout restarts the pod
            Effect(f"container_cpu_usage_cores|orders-etl-consumer-{n}", "mult", 2.0, scale="magnitude", rise=0.02,
                   jitter=0.02, cap=0.97),
            Effect(f"container_cpu_usage_cores|orders-etl-consumer-{n}", "mult", 2.0, phase="after", minutes=30, rise=0.05,
                   jitter=0.02, cap=0.97))),
    ),
    approximations=(
        "Batch: per-pod capacity drops from 1100 to 470 msg/s and `kafka_physics` integrates lag = produce - capacity. "
        "On the train split (Oct 9, 14:00-17:30) that gave: lag from ~200 to 1.7 million, accelerating with the "
        "afternoon traffic (on average ~480,000 per hour); consumption pinned at 940 msg/s (0.77-0.97 of the produce "
        "rate); both pods' CPU from ~0.47 to ~0.95 cores; after the rollback consumption at 2,200 msg/s and CPU at "
        "~0.94 until the lag was gone, about 28 minutes later.",
        "Live: lag is a linear ramp at a drawn speed (it does not depend on the produce rate, so unlike the batch "
        "physics it also grows at night, when the slower consumers would in fact keep up); consumption is the "
        "reading x 0.85 and CPU x 2 clipped at 0.97 (both scaled by magnitude); after the stop the added lag decays "
        "over 30 minutes while consumption (x 1.85) and CPU (x 2) stay high.",
        "As in the batch scenario both pods restart once at the start (the rollout).",
    ))

BROKER_GC_PRESSURE = Scenario(
    name="broker_gc_pressure", batch="broker_gc_pressure",
    summary="A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, "
            "the broker falls out of the in-sync replicas again and again.",
    targets=("kafka-0", "kafka-1", "kafka-2"),
    params={"magnitude": MAGNITUDE, "duration_min": (90, 180)},
    effects=(
        Effect("jvm_heap_used_ratio|{target}", "add", 0.42, scale="magnitude", rise=0.08, cap=0.985),
        Effect("jvm_gc_time_ms_per_sec|{target}", "add", 520, scale="magnitude", delay_min=5, rise=0.1),
        Effect("container_cpu_usage_cores|{target}", "mult", 2.0, scale="magnitude"),
        Effect("kafka_produce_request_p99_ms|kafka-ingest", "add", 900, scale="magnitude", delay_min=8, rise=0.1),
        Effect("kafka_failed_produce_requests_per_sec|kafka-ingest", "add", 1.8, scale="magnitude", delay_min=10, rise=0.1),
        Effect("kafka_under_replicated_partitions|kafka-ingest", "add", 12, delay_min=12, end_before_min=5, rise=0.0,
               jitter=0.0, flap=((3, 9), (4, 12))),
    ),
    approximations=(
        "Batch: always kafka-1, 150 minutes. Live: any of the three brokers, 90-180 minutes.",
        "Under-replicated partitions flap as in the batch scenario (12 partitions for 3-9 minutes, then 4-12 minutes "
        "in sync), but the batch sets max(reading, 12) and live adds 12 to a reading that is 0.",
        "The ISR-shrink spikes (kafka_isr_shrinks_per_sec +0.2 for one minute per flap) are left out.",
        "CPU is not clipped at the pod's limit, as in the batch scenario.",
    ))

TRINO_HEAVY_QUERY = Scenario(
    name="trino_heavy_query", batch="trino_heavy_query",
    summary="One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.",
    targets=("trino-lakehouse",),
    params={"magnitude": MAGNITUDE, "duration_min": (25, 60)},
    effects=(
        *(e for w in ("trino-worker-0", "trino-worker-1") for e in (
            Effect(f"jvm_heap_used_ratio|{w}", "add", 0.5, scale="magnitude", rise=0.08, cap=0.97),
            Effect(f"jvm_gc_time_ms_per_sec|{w}", "add", 180, scale="magnitude"),
            Effect(f"container_cpu_usage_cores|{w}", "add", 2.2, scale="magnitude", cap=4.0))),
        Effect("trino_queued_queries|trino-lakehouse", "add", 22, scale="magnitude", delay_min=3),
        Effect("trino_resource_group_queued|adhoc", "add", 14, scale="magnitude", delay_min=3),
        Effect("trino_query_p95_sec|trino-lakehouse", "mult", 9, scale="magnitude", delay_min=2),
        Effect("trino_completed_queries_per_min|trino-lakehouse", "mult", 0.35, scale="magnitude", delay_min=2),
        Effect("trino_failed_queries_per_min|trino-lakehouse", "add", 4, scale="magnitude", delay_min=5),
    ),
    approximations=(
        "A direct port: the batch scenario is itself a list of `_bump` effects (42 minutes). Live: 25-60 minutes.",
        "Batch clips heap and CPU with a hard ceiling over the whole series; live clips only while the effect is on.",
    ))

PRODUCER_CRASHLOOP = Scenario(
    name="producer_crashloop", batch="producer_crashloop",
    summary="A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.",
    targets=("orders-producer",),
    params={"duration_min": (20, 60)},
    effects=(
        Effect(RESTARTS + "orders-producer", "add", 1, counter=True),
        Effect(RESTARTS + "orders-producer", "add", 12, "ramp", counter=True),            # 12 restarts an hour
        Effect("container_cpu_usage_cores|orders-producer", "drop", delay_min=1),
        Effect("container_memory_working_set_bytes|orders-producer", "drop", delay_min=1),
        *(e for sid in ("kafka_messages_in_per_sec|orders", "kafka_consumer_records_consumed_per_sec|orders-etl",
                        "kafka_consumergroup_lag|orders-etl", "spark_streaming_input_rows_per_sec|orders-stream") for e in (
            Effect(sid, "mult", 0.0, delay_min=1, rise=0.0, jitter=0.0),
            Effect(sid, "mult", 1.6, phase="after", minutes=20, rise=0.0, jitter=0.01))),
        *(e for n in (0, 1) for e in (
            Effect(f"container_cpu_usage_cores|orders-etl-consumer-{n}", "mult", 0.1, delay_min=1, rise=0.0, jitter=0.02),
            Effect(f"container_cpu_usage_cores|orders-etl-consumer-{n}", "mult", 1.55, phase="after", minutes=20, rise=0.0,
                   jitter=0.02, cap=1.0))),
    ),
    approximations=(
        "Batch: the produce rate is set to 0, then to 1.6x for 20 minutes (the upstream queue drains), and "
        "`kafka_physics` carries that into consumption, lag, the Spark stream's input and the consumers' CPU. On the "
        "train split (Oct 12, 16:20-16:55): all four series at 0, consumer CPU at 0.05 cores (about a tenth of "
        "normal); afterwards 1.6x and CPU ~0.8 for 20 minutes. Live writes exactly those factors on the readings.",
        "Batch restarts follow Kubernetes' back-off (10 s doubling to 5 minutes: 6 restarts in the first 7 minutes, "
        "then one every 5). Live: one restart at once, then 12 an hour, so the early burst is smaller.",
        "Batch: the producer pod reports CPU 0.01 in 30% of the minutes and nothing in the rest. Live: no CPU or "
        "memory reading at all while it crash-loops.",
        "There is no magnitude: a crash loop is all or nothing. Only the duration varies (batch: 35 minutes).",
    ))

DAG_PAUSED = Scenario(
    name="dag_paused", batch="dag_paused",
    summary="Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.",
    targets=("daily_orders", "customer_agg", "hourly_quality_check"),
    params={"lead_min": (10, 180), "duration_min": (240, 630)},
    effects=(Effect("*|{job}-driver", "drop"), Effect("*|{job}-exec-*", "drop")),
    pauses_dag=True,
    approximations=(
        "Batch: daily_orders, paused at 23:10, 170 minutes before its 02:00 run, for 630 minutes; at the unpause "
        "the missed run starts as a catch-up. Live: any of the three DAGs; `lead_min` is how long before a scheduled "
        "run the pause begins (used by the schedule generator; a manual injection starts when it is sent).",
        "No catch-up run: a run of the DAG that would have started during the pause is simply missing (neither its "
        "start nor its finish is published), and the readings of the job's batch-only pods are dropped for the pause. "
        "A run that started before the pause finishes normally; one that starts after the unpause is published as is.",
        "airflow_task_completions_per_min and airflow_queued_tasks keep the missing run's small contribution.",
    ))

SCENARIOS = {s.name: s for s in (CONSUMER_MEMORY_LEAK, FLINK_STATE_LEAK, CONSUMER_SLOW_DEPLOY, BROKER_GC_PRESSURE,
                                 TRINO_HEAVY_QUERY, PRODUCER_CRASHLOOP, DAG_PAUSED)}

REHEARSAL = Scenario(
    name="rehearsal_restart", batch="(none)",
    summary="Not a failure scenario: one restart of airflow-worker-0, to rehearse the injection machinery.",
    targets=("airflow-worker-0",),
    params={"duration_min": (10, 30)},
    effects=(Effect(RESTARTS + "{target}", "add", 1, counter=True),))
