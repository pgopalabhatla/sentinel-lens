"""Injector: applies live scenarios (scenarios_live.py) to the telemetry the publisher sends.

    python -m sentinel_stream.injector list                                   # scenarios and their ranges
    python -m sentinel_stream.injector inject trino_heavy_query -p magnitude=1.0 -p duration_min=40
    python -m sentinel_stream.injector stop <id>

Two topics belong to the injection side and to nobody else:

    control      commands for the publisher
                   {"cmd": "inject", "id", "scenario", "target", "params": {...}, "at": time or null}
                   {"cmd": "stop", "id", "at": time or null}
                 `at` is event time; null means "from the next message the publisher sends".
    answer_key   what was really injected, written by the publisher
                   {"event": "started", "id", "scenario", "target", "params", "series", "entities", "start", "stop"}
                   {"event": "stopped", "id", "stop", "reason"}       # the actual stop, if it differs or not
                   {"event": "published", "start", "end"}             # the span of the stream, at its end

The consumer and the engine must never read either of them: they would be reading the answer. They
do not know these topics exist (tests/test_live_isolation.py). Only the publisher reads `control`,
and only the scorer (live_score.py) reads `answer_key`.

`Injector` itself is plain Python with no Kafka in it: commands in, telemetry messages through,
answer-key records out. An injection is active for start <= t < stop: nothing stamped before its
start is changed. Everything is deterministic: the minute-to-minute jitter of an injection comes
from a random generator seeded with the injection's id.

Two things are computed here instead of being declared in the scenario, because they depend on the
reading:
  * a ramp with `reset_at` that reaches it starts again from zero the minute after and fires the
    scenario's `on_reset` effects (the pod was OOM-killed);
  * when an injection changes a pod's CPU, that pod's throttling follows with the batch generator's
    formula (sentinel_sim/core.py, finalize): throttled = clip((cpu / limit - 0.72) / 0.28, 0, 1) * 0.55.
    The reading keeps its own noise: the formula's value for the old CPU is taken out and the value
    for the new CPU put in.
"""
from __future__ import annotations

import argparse
import fnmatch
import math
import uuid
import zlib

import numpy as np
import pandas as pd

from sentinel_stream import wire
from sentinel_stream.events import utc
from sentinel_stream.scenarios_live import SCENARIOS, Effect, Scenario

CONTROL_TOPIC = "control"
ANSWER_KEY_TOPIC = "answer_key"
CPU, THROTTLED = "container_cpu_usage_cores|", "container_cpu_throttled_ratio|"
ONE_MINUTE = pd.Timedelta("1min")


def minutes(n: float) -> pd.Timedelta:
    return pd.Timedelta(minutes=float(n))


def throttled_for(cpu: float, limit: float) -> float:
    return min(1.0, max(0.0, (cpu / limit - 0.72) / 0.28)) * 0.55


class _Run:
    """One effect on one series from `t0` to `end` (planned length: until `t1`)."""

    def __init__(self, effect: Effect, sid: str, t0, t1, amount: float, rng):
        self.effect, self.sid, self.t0, self.t1, self.end = effect, sid, t0, t1, t1
        self.amount = amount                 # add: series units (ramp: per hour).  mult: the factor at full size
        self.rng = rng
        self.origin = t0                     # where the ramp counts from (moves when it resets)
        self.last_add = 0.0
        self.count = 0                       # counter steps given so far
        self.on = None
        if effect.flap:                      # on/off intervals in minutes from t0, drawn once
            (on_lo, on_hi), (off_lo, off_hi) = effect.flap
            self.on, m, length = [], 0.0, (t1 - t0) / ONE_MINUTE
            while m < length:
                d = float(rng.uniform(on_lo, on_hi))
                self.on.append((m, m + d))
                m += d + float(rng.uniform(off_lo, off_hi))

    def _size(self, ts) -> float:
        """How much of the effect is on at `ts`, 0..1 (for an add-ramp: hours since it began)."""
        e = self.effect
        m = (ts - self.origin) / ONE_MINUTE
        length = max((self.t1 - self.t0) / ONE_MINUTE, 1.0)
        if self.on is not None and not any(a <= (ts - self.t0) / ONE_MINUTE < b for a, b in self.on):
            return 0.0
        if e.shape == "ramp":                # counts the minute it is in, so the first reading already moves
            w = (m + 1) / 60.0 if e.op == "add" else min(1.0, (m + 1) / length)
        elif e.shape == "decay":
            w = math.exp(-4.0 * m / length)
        elif e.rise > 0:                     # flat, easing in and out as the batch `_bump` does
            x = (m + 1) / (length + 1)
            w = min(1.0, max(0.0, min(x / e.rise, (1 - x) / e.rise)))
        else:
            w = 1.0
        return w * (1.0 + float(self.rng.normal(0.0, e.jitter))) if e.jitter > 0 else w

    def steps(self, ts) -> int:
        """Counter: how many whole steps are new at `ts`."""
        m = (ts - self.t0) / ONE_MINUTE
        total = math.floor(self.amount * m / 60.0 + 1e-9) if self.effect.shape == "ramp" else round(self.amount)
        new, self.count = total - self.count, total
        return max(new, 0)

    def apply(self, values: dict, ts) -> bool:
        """Change this run's series in `values`. True if the ramp reset (the reading reached `reset_at`)."""
        e, sid = self.effect, self.sid
        if sid not in values:                # no reading this minute (pod not running): nothing to change
            return False
        if e.op == "drop":
            del values[sid]
            return False
        size = self._size(ts)
        if e.op == "add":
            self.last_add = self.amount * size
            new = values[sid] + self.last_add
            if e.reset_at is not None and new >= e.reset_at:
                self.origin, self.last_add = ts + ONE_MINUTE, 0.0
                return True                  # this minute keeps the base reading: the container was just killed
        else:
            new = max(0.0, values[sid] * (1.0 + (self.amount - 1.0) * size))
        if e.cap is not None:
            new = min(new, e.cap * (1.0 - abs(float(self.rng.normal(0.0, 0.008)))))
        values[sid] = new
        return False


class _Injection:
    def __init__(self, id_: str, scenario: Scenario, target: str, params: dict, start, series_ids):
        self.id, self.scenario, self.target, self.params = id_, scenario, target, params
        self.start = start
        self.planned_stop = self.stop = start + minutes(params["duration_min"])
        self.stopped = False
        self.rng = np.random.default_rng(zlib.crc32(id_.encode()))
        self._series_ids = series_ids
        self.runs: list = []
        self.series: list = []
        for e in scenario.effects:
            if e.phase == "during":
                for sid in self._resolve(e.series):
                    self.runs.append(_Run(e, sid, start + minutes(e.delay_min), self.planned_stop - minutes(e.end_before_min),
                                          self._amount(e), self.rng))
            self.series += [s for s in self._resolve(e.series) if s not in self.series]

    def _resolve(self, template: str, **more) -> list:
        name = template.format(target=self.target, job=self.target.replace("_", "-"), **more)
        if "*" in name:
            return [s for s in self._series_ids if fnmatch.fnmatchcase(s, name)]
        if name not in self._series_ids:
            raise ValueError(f"{self.scenario.name}: series {name!r} is not in the catalog")
        return [name]

    def _amount(self, e: Effect) -> float:
        amount = float(self.params[e.amount] if isinstance(e.amount, str) else e.amount) * e.unit
        if e.scale is None:
            return amount
        m = self.params[e.scale]
        return 1.0 + (amount - 1.0) * m if e.op == "mult" else amount * m

    def finish(self, at) -> None:
        """Stop at `at`: the 'during' effects end, 'after' effects and releases begin."""
        self.stop, self.stopped = at, True
        after = []
        for run in self.runs:
            e = run.effect
            if e.op == "add" and e.release_min > 0 and run.last_add and at > run.t0:
                tail = Effect(run.sid, "add", run.last_add, "decay", jitter=0.0, minutes=e.release_min)
                after.append(_Run(tail, run.sid, at, at + minutes(e.release_min), run.last_add, self.rng))
        for e in self.scenario.effects:
            if e.phase == "after":
                t0 = at + minutes(e.delay_min)
                after += [_Run(e, sid, t0, t0 + minutes(e.minutes), self._amount(e), self.rng) for sid in self._resolve(e.series)]
        self.runs = after

    def apply(self, values: dict, ts, counters: dict) -> None:
        self.runs = [r for r in self.runs if ts < r.end]
        todo = list(self.runs)
        while todo:
            run = todo.pop(0)
            if ts < run.t0:
                continue
            if run.effect.counter:
                n = run.steps(ts)
                if n:
                    counters[run.sid] = counters.get(run.sid, 0) + n
            elif run.apply(values, ts):
                hit = run.sid.split("|", 1)[1]
                for e in self.scenario.on_reset:             # they start this very minute
                    t0 = ts + minutes(e.delay_min)
                    new = [_Run(e, sid, t0, t0 + minutes(e.minutes), self._amount(e), self.rng)
                           for sid in self._resolve(e.series, hit=hit)]
                    self.runs += new
                    todo += new

    def done(self) -> bool:
        return self.stopped and not self.runs


class Injector:
    """series_ids     every series the publisher sends (the catalog)
    entity_of      {series_id: "kind:name"}, for the answer key
    cpu_limit_of   {pod: cores}, for throttling; pods not in it keep their throttling reading
    scenarios      {name: Scenario}
    """

    def __init__(self, series_ids, entity_of: dict, cpu_limit_of: dict | None = None, scenarios: dict = SCENARIOS):
        self.series_ids = list(series_ids)
        self.entity_of, self.cpu_limit_of, self.scenarios = entity_of, cpu_limit_of or {}, scenarios
        self._waiting: list = []             # inject commands not yet due
        self._stops: list = []               # stop commands not yet due
        self._live: list = []                # injections that still change something
        self._known: set = set()             # every injection id seen
        self._counters: dict = {}            # series_id -> steps added so far; a counter never goes back
        self._dropped_runs: set = set()      # (dag_id, run_id) of runs that do not happen

    @classmethod
    def for_data(cls, ds, scenarios: dict = SCENARIOS) -> "Injector":
        """From the replay API: the series catalog and the pods' CPU limits (configuration, not telemetry)."""
        catalog, pods = ds.series(), ds.pods()
        return cls(list(catalog.series_id), {s: f"{k}:{e}" for s, k, e in zip(catalog.series_id, catalog.entity_kind, catalog.entity)},
                   dict(zip(pods.pod, pods.cpu_limit_cores.astype(float))), scenarios)

    # ---------------------------------------------------------------------------------- commands
    def command(self, cmd: dict) -> None:
        """Take one control command. Raises ValueError if it cannot be carried out."""
        kind = cmd.get("cmd")
        at = None if cmd.get("at") is None else utc(cmd["at"])
        if kind == "inject":
            scenario = self.scenarios.get(cmd.get("scenario"))
            if scenario is None:
                raise ValueError(f"unknown scenario {cmd.get('scenario')!r}; there are {sorted(self.scenarios)}")
            id_ = str(cmd.get("id") or f"inj-{uuid.uuid4().hex[:8]}")
            if id_ in self._known:
                raise ValueError(f"injection id {id_!r} was already used")
            target = cmd.get("target") or scenario.targets[0]
            params = scenario.check(target, cmd.get("params") or {})
            _Injection(id_, scenario, target, params, utc("2000-01-01"), self.series_ids)     # resolves the series now
            self._known.add(id_)
            self._waiting.append((at, id_, scenario, target, params))
        elif kind == "stop":
            if cmd.get("id") not in self._known:
                raise ValueError(f"cannot stop unknown injection {cmd.get('id')!r}")
            self._stops.append((at, cmd["id"]))
        else:
            raise ValueError(f"unknown command {kind!r}")

    # ---------------------------------------------------------------------------------- the stream
    def apply(self, message: dict) -> tuple:
        """One telemetry message -> (the message to send, or None if it is not sent; answer-key records)."""
        ts = wire.event_time(message)
        records = []
        for item in [w for w in self._waiting if w[0] is None or w[0] <= ts]:
            self._waiting.remove(item)
            at, id_, scenario, target, params = item
            inj = _Injection(id_, scenario, target, params, ts if at is None or at < ts else at, self.series_ids)
            self._live.append(inj)
            records.append(self._started(inj))
        for item in [s for s in self._stops if s[0] is None or s[0] <= ts]:
            inj = next((i for i in self._live if i.id == item[1] and not i.stopped), None)
            waiting = any(w[1] == item[1] for w in self._waiting)
            if not waiting:                  # otherwise it has not started yet: keep the stop for later
                self._stops.remove(item)
                if inj is not None:
                    inj.finish(max(inj.start, ts if item[0] is None else item[0]))
                    records.append({"event": "stopped", "id": inj.id, "stop": wire.iso(inj.stop), "reason": "stop command"})
        for inj in self._live:
            if not inj.stopped and ts >= inj.planned_stop:
                inj.finish(inj.planned_stop)
                records.append({"event": "stopped", "id": inj.id, "stop": wire.iso(inj.stop), "reason": "duration"})

        if message["type"] == wire.DAG_RUN:
            key = (message["dag_id"], message["run_id"])
            if message["event"] == "start" and any(i.scenario.pauses_dag and not i.stopped and i.target == message["dag_id"]
                                                    for i in self._live):
                self._dropped_runs.add(key)
            out = None if key in self._dropped_runs else message
        elif message["type"] == wire.METRICS and (self._live or self._counters):
            out = self._metrics(message, ts)
        else:
            out = message
        self._live = [i for i in self._live if not i.done()]
        return out, records

    def _metrics(self, message: dict, ts) -> dict:
        before = message["values"]
        values = dict(before)
        for inj in self._live:
            inj.apply(values, ts, self._counters)
        for sid, n in self._counters.items():
            if sid in values:
                values[sid] = values[sid] + n
        for pod, limit in self.cpu_limit_of.items():             # throttling follows CPU
            cpu, thr = CPU + pod, THROTTLED + pod
            if thr in values and values.get(cpu) != before.get(cpu):
                if cpu not in values:
                    del values[thr]
                else:
                    values[thr] = max(0.0, values[thr] - throttled_for(before[cpu], limit) + throttled_for(values[cpu], limit))
        return {**message, "values": values}

    def _started(self, inj: _Injection) -> dict:
        series = list(inj.series)
        series += [THROTTLED + s[len(CPU):] for s in inj.series
                   if s.startswith(CPU) and THROTTLED + s[len(CPU):] in self.series_ids and s[len(CPU):] in self.cpu_limit_of]
        entities = {self.entity_of[s] for s in series} | ({f"dag:{inj.target}"} if inj.scenario.pauses_dag else set())
        return {"event": "started", "id": inj.id, "scenario": inj.scenario.name, "target": inj.target, "params": inj.params,
                "series": sorted(set(series)), "entities": sorted(entities), "start": wire.iso(inj.start),
                "stop": wire.iso(inj.planned_stop)}

    def active(self) -> list:
        return [i.id for i in self._live if not i.stopped]


def published_record(start, end) -> dict:
    return {"event": "published", "start": wire.iso(start), "end": wire.iso(end)}


# ------------------------------------------------------------------------------------------ CLI
def send(bootstrap: str, topic: str, commands) -> None:
    """Write control commands and wait until the broker has them."""
    from confluent_kafka import Producer

    from sentinel_stream import broker

    broker.ensure_topics(bootstrap, [topic])
    failed = []
    producer = Producer(broker.client_config(bootstrap, **{"enable.idempotence": True}))
    for cmd in commands:
        producer.produce(topic, value=wire.dumps(cmd), key=str(cmd.get("id", "")),
                         on_delivery=lambda err, msg: failed.append(err) if err is not None else None)
    producer.flush()
    if failed:
        raise RuntimeError(f"the broker rejected a command: {failed[0]}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Send inject / stop commands to the publisher.")
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--control-topic", default=CONTROL_TOPIC)
    sub = ap.add_subparsers(dest="action", required=True)
    sub.add_parser("list", help="the scenarios, their targets and parameter ranges")
    inject = sub.add_parser("inject", help="start a scenario")
    inject.add_argument("scenario", choices=sorted(SCENARIOS))
    inject.add_argument("--target", default=None)
    inject.add_argument("-p", "--param", action="append", default=[], metavar="NAME=VALUE",
                        help="a parameter; those not given take the middle of their range")
    inject.add_argument("--at", default=None, help="event time to start at (default: the publisher's next message)")
    inject.add_argument("--id", default=None)
    stop = sub.add_parser("stop", help="stop an injection before its duration is over")
    stop.add_argument("id")
    stop.add_argument("--at", default=None)
    args = ap.parse_args(argv)

    if args.action == "list":
        for s in SCENARIOS.values():
            print(f"{s.name}\n  {s.summary}\n  targets: {', '.join(s.targets)}")
            for k, (lo, hi) in s.params.items():
                print(f"  {k}: {lo:g} - {hi:g}")
        return 0
    from sentinel_stream import broker
    if args.action == "inject":
        scenario = SCENARIOS[args.scenario]
        target = args.target or scenario.targets[0]
        params = scenario.check(target, dict((p.split("=", 1)[0], float(p.split("=", 1)[1])) for p in args.param))
        cmd = {"cmd": "inject", "id": args.id or f"inj-{uuid.uuid4().hex[:8]}", "scenario": scenario.name, "target": target,
               "params": params, "at": None if args.at is None else wire.iso(args.at)}
    else:
        cmd = {"cmd": "stop", "id": args.id, "at": None if args.at is None else wire.iso(args.at)}
    send(args.bootstrap or broker.DEFAULT_BOOTSTRAP, args.control_topic, [cmd])
    print(wire.dumps(cmd).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
