"""Injector: applies live scenarios (scenarios_live.py) to the telemetry the publisher sends.

    python -m sentinel_stream.injector list                                   # scenarios and their ranges
    python -m sentinel_stream.injector inject trino_heavy_query -p magnitude=1.0 -p duration_min=40
    python -m sentinel_stream.injector custom -e "jvm_heap_used_ratio|kafka-0" add ramp 0.2 90 --severity subtle \
                                              --intent "heap creeping up on one broker"
    python -m sentinel_stream.injector stop <id>

Two topics belong to the injection side and to nobody else:

    control      commands for the publisher
                   {"cmd": "inject", "id", "scenario", "target", "params": {...}, "at": time or null}
                   {"cmd": "inject", "id", "effects": [{"series_id", "op": "add" | "mult",
                       "shape": "flat" | "ramp" | "decay", "magnitude", "start": time or null, "duration_min"}, ...],
                    "at": time or null}                           # a generic injection, see below
                   {"cmd": "stop", "id", "at": time or null}
                 `at` is event time; null means "from the next message the publisher sends". Any inject
                 command may also carry "intent" (free text) and "severity" ("noticeable" | "subtle").
    answer_key   what was really injected, written by the publisher
                   {"event": "started", "id", "scenario", "target", "params", "series", "entities", "start", "stop"}
                   {"event": "stopped", "id", "stop", "reason"}       # the actual stop, if it differs or not
                   {"event": "rejected", "id", "reasons"}             # a command or all of its effects refused: nothing applied
                 A generic injection's `started` record has "scenario": "custom" and lists the effects
                 exactly as applied (resolved start and stop) and the ones rejected, each with its reason;
                 its `stopped` record says for how many minutes a reading had to be held at a limit.
                 `intent` and `severity` are copied from the command.
                   {"event": "published", "start", "end"}             # the span of the stream, at its end

The consumer and the engine must never read either of them: they would be reading the answer. They
do not know these topics exist (tests/test_live_isolation.py). Only the publisher reads `control`,
and only the scorer (live_score.py) reads `answer_key`.

`Injector` itself is plain Python with no Kafka in it: commands in, telemetry messages through,
answer-key records out. An injection is active for start <= t < stop: nothing stamped before its
start is changed. Everything is deterministic: the minute-to-minute jitter of an injection comes
from a random generator seeded with the injection's id.

Generic injections. Instead of a named scenario a command can give a list of primitive effects, each
on one series with its own start and duration. They are exact: no jitter and no easing. flat holds
`magnitude` for the whole duration; ramp grows from zero to `magnitude` at the end; decay starts at
`magnitude` and falls off as exp(-4 x). For mult, `magnitude` is the factor at full size. Every
effect is checked against what its series can physically be (`Injector.limits_of`, by the catalog's
unit):

    ratio                       stays in [0, 1]
    bytes (memory), cores       0 .. the pod's limit
    ms/s (GC time)              0 .. 1000
    partitions, queries, tasks  whole numbers >= 0: an added amount must be whole, results are rounded
    count (the *_total series)  a counter: only add, a whole positive number, flat or ramp; the steps stay
    everything else             >= 0 (latency, rates, lag)

An effect is rejected when it can never be valid (unknown series, a ratio moved by more than 1, a
counter multiplied) or when, on the reading its series has at the moment the injection begins, its
full size would leave the limits ("heap is 0.71; + 0.42 would be 1.13, above 1"). Rejected effects are
not applied and are reported in the answer key with the reason; the valid ones of the same command
are applied. Readings move after that moment, so while an effect runs its result is also held inside
the limits, and the answer key counts the minutes in which that was needed.

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
CUSTOM = Scenario(name="custom", batch="(none)", summary="A list of primitive effects given by the caller.",
                  targets=("",), params={}, effects=())
SEVERITIES = ("noticeable", "subtle")
COUNT_UNITS = ("partitions", "queries", "tasks")       # gauges that count things: whole numbers
COUNTER_UNIT = "count"                                 # the *_total series: cumulative, never go back


def minutes(n: float) -> pd.Timedelta:
    return pd.Timedelta(minutes=float(n))


def throttled_for(cpu: float, limit: float) -> float:
    return min(1.0, max(0.0, (cpu / limit - 0.72) / 0.28)) * 0.55


class _Run:
    """One effect on one series from `t0` to `end` (planned length: until `t1`)."""

    def __init__(self, effect: Effect, sid: str, t0, t1, amount: float, rng, bounds=None, clipped: dict | None = None):
        self.effect, self.sid, self.t0, self.t1, self.end = effect, sid, t0, t1, t1
        self.bounds = bounds                 # generic effects: (low, high, whole numbers?) the reading must stay in
        self.clipped = clipped               # ... and where the minutes it had to be held there are counted
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
        m = (ts - self.t0) / ONE_MINUTE + (1 if self.bounds is not None else 0)     # generic: all steps by the last minute
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
        if self.bounds is not None:
            low, high, whole = self.bounds
            held = min(max(new, low), high)
            if held != new:
                self.clipped[sid] = self.clipped.get(sid, 0) + 1
            new = float(round(held)) if whole else held
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
        self.clipped: dict = {}
        self.runs: list = []
        self.series: list = []
        for e in scenario.effects:
            if e.phase == "during":
                for sid in self._resolve(e.series):
                    self.runs.append(_Run(e, sid, start + minutes(e.delay_min), self.planned_stop - minutes(e.end_before_min),
                                          self._amount(e), self.rng))
            self.series += [s for s in self._resolve(e.series) if s not in self.series]

    @classmethod
    def custom(cls, id_: str, effects: list, start, series_ids, limits_of) -> "_Injection":
        """A generic injection: `effects` are checked primitive effects with resolved `t0` and `t1`."""
        self = cls.__new__(cls)
        self.id, self.scenario, self.target, self.params = id_, CUSTOM, "", {}
        self.start = start
        self.planned_stop = self.stop = max(e["t1"] for e in effects)
        self.stopped = False
        self.rng = np.random.default_rng(zlib.crc32(id_.encode()))
        self._series_ids = series_ids
        self.clipped = {}
        self.runs, self.series = [], []
        for e in effects:
            length = (e["t1"] - e["t0"]) / ONE_MINUTE
            amount = e["magnitude"] * 60.0 / length if e["shape"] == "ramp" and e["op"] == "add" else e["magnitude"]
            spec = Effect(e["series_id"], e["op"], amount, e["shape"], rise=0.0, jitter=0.0, counter=e["counter"])
            self.runs.append(_Run(spec, e["series_id"], e["t0"], e["t1"], amount, self.rng, limits_of(e["series_id"]), self.clipped))
            self.series += [e["series_id"]] if e["series_id"] not in self.series else []
        return self

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
    """series_ids       every series the publisher sends (the catalog)
    entity_of        {series_id: "kind:name"}, for the answer key
    cpu_limit_of     {pod: cores}, for throttling and the limit of a CPU reading
    scenarios        {name: Scenario}
    unit_of          {series_id: unit}, for the physical limits of generic effects
    memory_limit_of  {pod: bytes}
    """

    def __init__(self, series_ids, entity_of: dict, cpu_limit_of: dict | None = None, scenarios: dict = SCENARIOS,
                 unit_of: dict | None = None, memory_limit_of: dict | None = None):
        self.series_ids = list(series_ids)
        self.entity_of, self.cpu_limit_of, self.scenarios = entity_of, cpu_limit_of or {}, scenarios
        self.unit_of, self.memory_limit_of = unit_of or {}, memory_limit_of or {}
        self._known_series = set(self.series_ids)
        self._waiting: list = []             # inject commands not yet due
        self._stops: list = []               # stop commands not yet due
        self._live: list = []                # injections that still change something
        self._known: set = set()             # every injection id seen
        self._counters: dict = {}            # series_id -> steps added so far; a counter never goes back
        self._dropped_runs: set = set()      # (dag_id, run_id) of runs that do not happen
        self._last: dict = {}                # the latest readings sent by the data, before any injection

    @classmethod
    def for_data(cls, ds, scenarios: dict = SCENARIOS) -> "Injector":
        """From the replay API: the series catalog and the pods' limits (configuration, not telemetry)."""
        catalog, pods = ds.series(), ds.pods()
        return cls(list(catalog.series_id), {s: f"{k}:{e}" for s, k, e in zip(catalog.series_id, catalog.entity_kind, catalog.entity)},
                   dict(zip(pods.pod, pods.cpu_limit_cores.astype(float))), scenarios,
                   dict(zip(catalog.series_id, catalog.unit)), dict(zip(pods.pod, pods.memory_limit_bytes.astype(float))))

    # ---------------------------------------------------------------------------- physical limits
    def limits_of(self, sid: str) -> tuple:
        """(lowest, highest, whole numbers only?) a reading of this series can physically be."""
        unit, pod = self.unit_of.get(sid, ""), sid.split("|", 1)[-1]
        if unit == "ratio":
            return 0.0, 1.0, False
        if unit == "bytes":
            return 0.0, self.memory_limit_of.get(pod, math.inf), False
        if unit == "cores":
            return 0.0, self.cpu_limit_of.get(pod, math.inf), False
        if unit == "ms/s":
            return 0.0, 1000.0, False
        return 0.0, math.inf, unit in COUNT_UNITS

    def is_counter(self, sid: str) -> bool:
        return self.unit_of.get(sid) == COUNTER_UNIT or sid.split("|", 1)[0].endswith("_total")

    def _check_effect(self, effect) -> tuple:
        """One primitive effect as it came -> (the effect in a fixed form, None) or (None, why it can never be valid)."""
        if not isinstance(effect, dict):
            return None, "an effect must be an object with series_id, op, shape, magnitude, duration_min"
        sid, op, shape = effect.get("series_id"), effect.get("op"), effect.get("shape", "flat")
        if sid not in self._known_series:
            return None, f"series {sid!r} is not in the catalog"
        if op not in ("add", "mult"):
            return None, f"op must be add or mult, got {op!r}"
        if shape not in ("flat", "ramp", "decay"):
            return None, f"shape must be flat, ramp or decay, got {shape!r}"
        try:
            magnitude, duration = float(effect.get("magnitude")), float(effect.get("duration_min"))
            start = None if effect.get("start") is None else utc(effect["start"])
        except (TypeError, ValueError):
            return None, "magnitude and duration_min must be numbers, and start a time or null"
        if not (math.isfinite(magnitude) and math.isfinite(duration)) or duration <= 0:
            return None, "magnitude must be finite and duration_min greater than 0"
        low, high, whole = self.limits_of(sid)
        counter = self.is_counter(sid)
        if op == "mult" and magnitude < 0:
            return None, "a factor cannot be negative"
        if counter:
            if op != "add" or shape == "decay" or magnitude <= 0 or magnitude != round(magnitude):
                return None, "a counter only takes add with a whole positive number, flat or ramp: it cannot go back"
        elif op == "add":
            if whole and magnitude != round(magnitude):
                return None, f"{sid.split('|')[0]} counts things: the amount added must be a whole number, got {magnitude:g}"
            if abs(magnitude) > high - low:
                return None, f"adding {magnitude:g} cannot stay inside {low:g} .. {high:g}"
        return {"series_id": sid, "op": op, "shape": shape, "magnitude": magnitude, "duration_min": duration,
                "start": start, "counter": counter}, None

    def _check_on_reading(self, e: dict, readings: dict) -> str | None:
        """Would the effect at full size leave the limits, on the reading its series has now?"""
        v = readings.get(e["series_id"])
        if v is None or e["counter"]:        # no reading (pod not running), or a counter going up: nothing to exceed
            return None
        low, high, _ = self.limits_of(e["series_id"])
        full = v + e["magnitude"] if e["op"] == "add" else v * e["magnitude"]
        if low <= full <= high:
            return None
        how = f"+ {e['magnitude']:g}" if e["op"] == "add" else f"x {e['magnitude']:g}"
        return f"the reading is {v:.4g}; {how} would be {full:.4g}, {'above' if full > high else 'below'} its limit of {high if full > high else low:.4g}"

    # ---------------------------------------------------------------------------------- commands
    def command(self, cmd: dict) -> list:
        """Take one control command. Raises ValueError if nothing of it can be carried out. Returns
        the effects of a generic injection that were rejected at once: [{"effect", "reason"}]."""
        kind = cmd.get("cmd")
        at = None if cmd.get("at") is None else utc(cmd["at"])
        if kind == "stop":
            if cmd.get("id") not in self._known:
                raise ValueError(f"cannot stop unknown injection {cmd.get('id')!r}")
            self._stops.append((at, cmd["id"]))
            return []
        if kind != "inject":
            raise ValueError(f"unknown command {kind!r}")
        id_ = str(cmd.get("id") or f"inj-{uuid.uuid4().hex[:8]}")
        if id_ in self._known:
            raise ValueError(f"injection id {id_!r} was already used")
        severity = cmd.get("severity")
        if severity is not None and severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}, got {severity!r}")
        note = {k: cmd[k] for k in ("intent", "severity") if cmd.get(k) is not None}
        if "effects" in cmd:
            if cmd.get("scenario") is not None:
                raise ValueError("give either a scenario or a list of effects, not both")
            if not isinstance(cmd["effects"], list) or not cmd["effects"]:
                raise ValueError("effects must be a list with at least one effect")
            checked = [(e, *self._check_effect(e)) for e in cmd["effects"]]
            effects = [ok for _, ok, _ in checked if ok is not None]
            rejected = [{"effect": raw, "reason": why} for raw, ok, why in checked if ok is None]
            if not effects:
                raise ValueError("no valid effect: " + "; ".join(r["reason"] for r in rejected))
            starts = [e["start"] if e["start"] is not None else at for e in effects]
            self._known.add(id_)
            self._waiting.append({"at": None if None in starts else min(starts), "id": id_, "effects": effects,
                                  "default_start": at, "rejected": rejected, "note": note})
            return rejected
        scenario = self.scenarios.get(cmd.get("scenario"))
        if scenario is None:
            raise ValueError(f"unknown scenario {cmd.get('scenario')!r}; there are {sorted(self.scenarios)}")
        target = cmd.get("target") or scenario.targets[0]
        params = scenario.check(target, cmd.get("params") or {})
        _Injection(id_, scenario, target, params, utc("2000-01-01"), self.series_ids)     # resolves the series now
        self._known.add(id_)
        self._waiting.append({"at": at, "id": id_, "scenario": scenario, "target": target, "params": params, "note": note})
        return []

    # ---------------------------------------------------------------------------------- the stream
    def apply(self, message: dict) -> tuple:
        """One telemetry message -> (the message to send, or None if it is not sent; answer-key records)."""
        ts = wire.event_time(message)
        records = []
        if message["type"] == wire.METRICS:
            self._last = message["values"]
        for item in [w for w in self._waiting if w["at"] is None or w["at"] <= ts]:
            self._waiting.remove(item)
            inj = self._begin(item, ts, records)
            if inj is not None:
                self._live.append(inj)
        for item in [s for s in self._stops if s[0] is None or s[0] <= ts]:
            inj = next((i for i in self._live if i.id == item[1] and not i.stopped), None)
            waiting = any(w["id"] == item[1] for w in self._waiting)
            if not waiting:                  # otherwise it has not started yet: keep the stop for later
                self._stops.remove(item)
                if inj is not None:
                    inj.finish(max(inj.start, ts if item[0] is None else item[0]))
                    records.append(self._stopped(inj, "stop command"))
        for inj in self._live:
            if not inj.stopped and ts >= inj.planned_stop:
                inj.finish(inj.planned_stop)
                records.append(self._stopped(inj, "duration"))

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

    def _begin(self, item: dict, ts, records: list):
        """Start a waiting injection at `ts` and write its `started` record (or `rejected`, if it
        was generic and none of its effects is valid on the readings of this moment)."""
        at = item["at"]
        if "scenario" in item:
            inj = _Injection(item["id"], item["scenario"], item["target"], item["params"], ts if at is None or at < ts else at,
                             self.series_ids)
            records.append({**self._started(inj), **item["note"]})
            return inj
        applied, rejected = [], list(item["rejected"])
        for e in item["effects"]:
            start = e["start"] if e["start"] is not None else item["default_start"]
            t0 = ts if start is None or start < ts else start
            why = self._check_on_reading(e, self._last)
            public = {k: e[k] for k in ("series_id", "op", "shape", "magnitude")}
            if why is not None:
                rejected.append({"effect": {**public, "duration_min": e["duration_min"]}, "reason": why})
            else:
                applied.append({**e, "t0": t0, "t1": t0 + minutes(e["duration_min"])})
        if not applied:
            records.append({"event": "rejected", "id": item["id"], "reasons": [r["reason"] for r in rejected], **item["note"]})
            return None
        inj = _Injection.custom(item["id"], applied, min(e["t0"] for e in applied), self.series_ids, self.limits_of)
        record = self._started(inj)
        record["effects"] = [{"series_id": e["series_id"], "op": e["op"], "shape": e["shape"], "magnitude": e["magnitude"],
                              "start": wire.iso(e["t0"]), "stop": wire.iso(e["t1"])} for e in applied]
        record["rejected"] = rejected
        records.append({**record, **item["note"]})
        return inj

    def _stopped(self, inj: _Injection, reason: str) -> dict:
        record = {"event": "stopped", "id": inj.id, "stop": wire.iso(inj.stop), "reason": reason}
        if inj.clipped:                      # generic effects: minutes in which a reading was held at its limit
            record["minutes_held_at_a_limit"] = dict(sorted(inj.clipped.items()))
        return record

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
    custom = sub.add_parser("custom", help="a generic injection: one or more primitive effects")
    custom.add_argument("-e", "--effect", nargs=5, action="append", default=[],
                        metavar=("SERIES_ID", "OP", "SHAPE", "MAGNITUDE", "DURATION_MIN"),
                        help="op: add | mult; shape: flat | ramp | decay. Repeat for several effects.")
    custom.add_argument("--json", default=None, help="a file with the whole command as JSON (for per-effect start times)")
    for which in (inject, custom):
        which.add_argument("--at", default=None, help="event time to start at (default: the publisher's next message)")
        which.add_argument("--id", default=None)
        which.add_argument("--intent", default=None, help="free text: what this injection is meant to be")
        which.add_argument("--severity", default=None, choices=SEVERITIES,
                           help="how visible you think it is; the scorer reports by it and never matches on it")
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
    elif args.action == "custom":
        import json
        cmd = json.loads(open(args.json).read()) if args.json else {}
        cmd.setdefault("effects", [])
        cmd["effects"] += [{"series_id": sid, "op": op, "shape": shape, "magnitude": float(magnitude), "duration_min": float(duration)}
                           for sid, op, shape, magnitude, duration in args.effect]
        if not cmd["effects"]:
            raise SystemExit("give at least one effect (-e) or a --json file with effects")
        cmd.update({"cmd": "inject", "id": args.id or cmd.get("id") or f"inj-{uuid.uuid4().hex[:8]}"})
        if args.at is not None:
            cmd["at"] = wire.iso(args.at)
    else:
        cmd = {"cmd": "stop", "id": args.id, "at": None if args.at is None else wire.iso(args.at)}
    if args.action in ("inject", "custom"):
        cmd.update({k: v for k, v in (("intent", args.intent), ("severity", args.severity)) if v is not None})
    send(args.bootstrap or broker.DEFAULT_BOOTSTRAP, args.control_topic, [cmd])
    print(wire.dumps(cmd).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
