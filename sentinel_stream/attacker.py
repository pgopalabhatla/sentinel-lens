"""Attacker: an LLM agent that invents incidents and injects them into the live stream.

    python -m sentinel_stream.attacker campaign --data data_live --start 2026-10-26 --end 2026-10-27 --name dry-run-1
    python -m sentinel_stream.attacker live --data data_live --start 2026-10-26 --speed 60      # with a running publisher
    python -m sentinel_stream.attacker replay --data data_live --commands runs/attacker/dry-run-1/control.jsonl \\
        --start 2026-10-26 --end 2026-10-27                                                    # no LLM: the stream it gives

It is Claude with seven tools (`TOOLS`, also served over MCP: `python -m sentinel_stream.mcp_server
--group attacker ...`):

    list_platform      entities, their series, units, physical limits, layers, and the entity graph (configuration)
    describe_normal    one series' quiet-world history up to now: hour-of-day profile, last reading (the data directory)
    list_scenarios     the seven named scenarios with their targets and parameter ranges
    inject_scenario    a named scenario on a target, with parameters, intent and severity
    inject_effects     a generic incident: a list of primitive effects (injector.py), with intent and severity
    list_active        its own injections: active, scheduled, and today's count against the budget
    stop               end one of its own injections now

Rules in code, besides the budget: an injection may only touch series that `describe_normal` was
called on during the same wake (so its effects are sized against what is normal; a named scenario
needs all of its series described, `list_scenarios` lists them), and injection starts must be at
least `min_gap_minutes` (3 h) apart. A generic incident must say, in `novelty`, which plausible real
failure it stands for that the named scenarios do not cover; how much its series overlap with each
named scenario's is recorded and reported, not enforced.

Every injection goes the validated way: the command is checked with the injector's own rules (the
same `Injector` code, unchanged) before it is sent, including the physical-limit check against the
quiet world's reading at the moment the effect starts. Rejected effects and refused commands come back
to the agent as tool errors and are recorded; only what passes is sent, as a `control` command. (The
publisher checks again against the reading it actually meets; an effect that overlaps another
injection can still be refused there, and that is recorded only where the attacker cannot look.)

Blind. The attacker knows the platform's configuration, the quiet world's history and its own commands.
It never reads the detection side's output topics, the notes written about them, the injection record the publisher keeps,
or any detector state; its only Kafka call writes to `control` (tests/test_live_isolation.py).

Modes (one loop; the agent is woken every `wake_every_minutes` of simulated time and decides whether
to inject now):
    live       the wakes follow the wall clock at `speed` simulated seconds per second, as the publisher
               does; commands go to `control` with `at` at least `lead_minutes` ahead, so they arrive in time.
    campaign   the same loop over a window at max speed, without a broker. Budget, enforced in code:
               at most `max_per_day` injections per simulated day and at most `max_active` active at once
               (the prompt asks for at least `min_per_day` a day and for variety). Everything is recorded:
               every command, every tool call and the agent's reasoning at every wake. The commands are the
               recorded `control` stream; `replay()` plays them through the injector without the LLM and
               gives, byte for byte, the stream and the injection record the publisher gives.
"""
from __future__ import annotations

import argparse
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd
import yaml

from sentinel_stream import wire
from sentinel_stream.events import utc
from sentinel_stream.injector import SEVERITIES, Injector, _Injection, published_record
from sentinel_stream.scenarios_live import SCENARIOS

CONFIG_PATH = Path(__file__).with_name("attacker.yaml")
CONTROL_TOPIC = "control"
ONE_MINUTE = pd.Timedelta("1min")
MAX_RESULT_CHARS = 24000

SYSTEM = """You design incidents for a test of a data platform's monitoring: Kafka, Spark, Airflow, Flink and Trino on Kubernetes.
You inject failures into the platform's live telemetry. You cannot see how the monitoring reacts and you are not told; your job is only to make the incidents realistic and varied, the kind of thing that really happens to such a platform.

You are woken at regular times. Each time, decide whether to inject something now (or a little later, with start_in_minutes), or to do nothing this time. Two ways to inject:
- inject_scenario: one of the named scenarios, on a target, with parameters inside their ranges.
- inject_effects: a NEW incident built from primitive effects on series (add or multiply; flat, ramp or decay; each with its own start and duration). It must stand for a plausible real failure that the named scenarios do NOT cover: not one of them rebuilt by hand. Say which failure and why it is not one of the named scenarios in the `novelty` field. Make it coherent: a real fault moves several related series together (for example a cause and its symptoms on neighbouring entities).

Before injecting, call describe_normal on every series the injection will touch, in the same wake (it takes up to 30 series at once; list_scenarios lists the series each named scenario touches), and size the effects against what is normal for them. An injection that touches a series not described in this wake is rejected. Injection starts must be at least {gap} apart.

Vary the incidents across the campaign: named scenarios and new generic incidents; different targets and parts of the platform; layer 1 (pods: CPU, memory, restarts), layer 2 (service signals: throughput, backlog, latency, errors, JVM) and layer 3 (product-specific signals); noticeable and subtle ones. Give every injection an intent (one or two sentences: the real-world failure it stands for) and a severity: noticeable or subtle.
Use list_platform to find series and entities. Effects must be physically possible (ratios stay in 0-1, memory and CPU under the pod's limit, counters only go up by whole numbers); the injector checks this and rejects what is not, and the tool tells you why. Respect the budget given at each wake.

When you are done for this wake, reply with a short plain-text note: what you did and why, or why you did nothing."""


# ------------------------------------------------------------------------------------------ config
@dataclass
class Budget:
    min_per_day: int = 1                # asked for in the prompt, reported; not enforceable
    max_per_day: int = 3                # enforced: a further injection that simulated day is refused
    max_active: int = 2                 # enforced: an injection that would make 3 active at any time is refused
    min_gap_minutes: float = 180.0      # enforced: injection starts at least this far apart
    max_tool_calls_per_wake: int = 8
    max_seconds_per_wake: float = 90.0
    max_tokens_per_wake: int = 80_000


@dataclass
class Config:
    model: str = "claude-sonnet-5-5"
    effort: str | None = "medium"
    max_tokens_per_call: int = 4000
    wake_every_minutes: float = 60.0
    lead_minutes: float = 2.0           # live: a command's `at` is at least this far ahead of the stream clock
    stream_version: int = 2
    budget: Budget = field(default_factory=Budget)
    runs_dir: str = "runs"
    prices_per_mtok: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path=CONFIG_PATH, **overrides) -> "Config":
        raw = yaml.safe_load(Path(path).read_text()) if path else {}
        raw.update({k: v for k, v in overrides.items() if v is not None})
        return cls(budget=Budget(**raw.pop("budget", {})), **raw)


def _r(x):
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return None
    return float(f"{float(x):.4g}")


def _iso(t) -> str:
    return utc(t).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------------------------- the world
class Attacker:
    """The attacker's world and its state: the clock, its own injections, what it has sent.

    ds      the replay API over the quiet world (configuration and history; read only up to `now`)
    send    called with every accepted command (campaign: kept in `commands`; live: also written to `control`)
    """

    def __init__(self, ds, config: Config | None = None, send=None, window=None, id_prefix: str = "atk"):
        self.ds, self.config = ds, config or Config.load()
        self.send = send or (lambda cmd: None)
        self.window = None if window is None else (utc(window[0]), utc(window[1]))
        self.id_prefix = id_prefix
        self.validator = Injector.for_data(ds, evidence=self.config.stream_version >= 2)
        catalog = ds.series()
        self.catalog = catalog
        self.layer_of = dict(zip(catalog.series_id, catalog.layer.astype(int)))
        self.entity_of = {s: f"{k}:{e}" for s, k, e in zip(catalog.series_id, catalog.entity_kind, catalog.entity)}
        self.now = None
        self.commands: list = []        # every command sent, in order: the recorded control stream
        self.injections: dict = {}      # id -> what the attacker knows of its own injection
        self.rejections: list = []      # every refused or partly refused injection, with the reasons
        self.described: set = set()     # series describe_normal was called on in this wake (Agent.wake resets it)
        self._scenario_series = {(n, t): self.scenario_series(n, t) for n, sc in SCENARIOS.items() for t in sc.targets}

    # ---------------------------------------------------------------------------------- budget
    def _intervals(self) -> list:
        return [(i["at"], i["stopped_at"] or i["stop"]) for i in self.injections.values()]

    def active_at(self, t) -> list:
        return [k for k, i in self.injections.items() if i["at"] <= t < (i["stopped_at"] or i["stop"])]

    def today(self, t) -> list:
        day = utc(t).floor("D")
        return [k for k, i in self.injections.items() if i["at"].floor("D") == day]

    def check_budget(self, at, stop) -> str | None:
        b = self.config.budget
        if self.window and not (self.window[0] <= at < self.window[1]):
            return f"budget: the start {_iso(at)} is outside the campaign window {_iso(self.window[0])} .. {_iso(self.window[1])}"
        if at < self.now:
            return f"budget: the start {_iso(at)} is in the past (now {_iso(self.now)})"
        gap = pd.Timedelta(minutes=b.min_gap_minutes)
        near = [i for i in self.injections.values() if abs(i["at"] - at) < gap]
        if near:
            return (f"budget: injection starts must be at least {b.min_gap_minutes / 60:g} h apart, and {near[0]['id']} starts at "
                    f"{_iso(near[0]['at'])}; start at {_iso(near[0]['at'] + gap)} or later")
        n = len(self.today(at))
        if n >= b.max_per_day:
            return f"budget: {n} injections already start on {at:%Y-%m-%d}; at most {b.max_per_day} a day"
        points = sorted({at} | {s for s, e in self._intervals() if at <= s < stop})
        most = max(sum(s <= p < e for s, e in self._intervals()) for p in points)
        if most >= b.max_active:
            return (f"budget: at most {b.max_active} injections may be active at once, and {most} would already be "
                    f"active during {_iso(at)} .. {_iso(stop)}; stop one or start later")
        return None

    # ---------------------------------------------------------------------------- validation
    def _quiet_reading(self, t) -> dict:
        """The quiet world's readings of the minute stamped t (what the publisher would meet, if nothing else
        were injected then)."""
        t = utc(t).floor("min")
        block = self.ds.at(t + ONE_MINUTE).query(list(self.catalog.series_id), start=t, end=t + ONE_MINUTE)
        return {} if block.empty else {k: float(v) for k, v in block.iloc[-1].items() if not pd.isna(v)}

    def scenario_series(self, name: str, target: str) -> list:
        """The series a named scenario changes on a target (as the injector resolves them)."""
        spec = SCENARIOS[name]
        return sorted(_Injection("probe", spec, target, spec.midpoints(), utc("2000-01-01"), self.validator.series_ids).series)

    def overlap(self, series) -> dict:
        """How much a generic incident's series overlap with each named scenario's (any of its targets):
        reported, not enforced."""
        mine = set(series)
        out = {}
        for name in SCENARIOS:
            theirs = set().union(*(set(v) for (n, _), v in self._scenario_series.items() if n == name))
            shared = mine & theirs
            out[name] = {"shared": len(shared), "of_incident": len(mine), "jaccard": round(len(shared) / len(mine | theirs), 3)}
        return out

    def _undescribed(self, series) -> list:
        return [x for x in series if x not in self.described]

    def _next_id(self) -> str:
        return f"{self.id_prefix}-{len(self.injections) + 1:03d}"         # ids count accepted injections only

    def _accept(self, cmd: dict, kind: str, at, stop, series: list, args: dict) -> dict:
        self.validator.command(cmd)                         # registers the id; already known to pass
        self.commands.append(cmd)
        self.send(cmd)
        self.injections[cmd["id"]] = {"id": cmd["id"], "kind": kind, "at": at, "stop": stop, "stopped_at": None,
                                      "scenario": cmd.get("scenario"), "target": cmd.get("target"),
                                      "severity": cmd["severity"], "intent": cmd["intent"],
                                      "layers": sorted({self.layer_of[s] for s in series if s in self.layer_of}),
                                      "entities": sorted({self.entity_of[s] for s in series if s in self.entity_of}),
                                      "decided_at": self.now, "args": args}
        return {"accepted": True, "injection_id": cmd["id"], "start": _iso(at), "planned_stop": _iso(stop),
                "active_at_start": len(self.active_at(at)), "today": len(self.today(at))}

    def _reject(self, tool: str, args: dict, reasons: list, id_=None) -> dict:
        self.rejections.append({"at": _iso(self.now), "tool": tool, "input": args, "reasons": reasons, "id": id_})
        return {"error": "rejected: " + "; ".join(reasons)}

    def _intent(self, intent, severity) -> list:
        bad = []
        if not (isinstance(intent, str) and intent.strip()):
            bad.append("give an intent: the real-world failure this stands for")
        if severity not in SEVERITIES:
            bad.append(f"severity must be one of {list(SEVERITIES)}")
        return bad

    def inject_scenario(self, scenario: str, target: str | None = None, params: dict | None = None, intent: str = "",
                        severity: str = "", start_in_minutes: float = 0) -> dict:
        args = {"scenario": scenario, "target": target, "params": params or {}, "intent": intent, "severity": severity,
                "start_in_minutes": start_in_minutes}
        spec = SCENARIOS.get(scenario)
        if spec is None:
            return self._reject("inject_scenario", args, [f"unknown scenario {scenario!r}; there are {sorted(SCENARIOS)}"])
        bad = self._intent(intent, severity)
        target = target or spec.targets[0]
        try:
            full = spec.check(target, params or {})
        except (ValueError, TypeError) as e:
            bad.append(str(e))
        if bad:
            return self._reject("inject_scenario", args, bad)
        missing = self._undescribed(self._scenario_series[(scenario, target)])
        if missing:
            return self._reject("inject_scenario", args, [
                f"describe_normal was not called in this wake on {len(missing)} of the series this scenario touches: "
                f"{missing[:12]}{' ...' if len(missing) > 12 else ''}; describe them first (up to 30 at once)"])
        at = self.now + pd.Timedelta(minutes=float(start_in_minutes))
        stop = at + pd.Timedelta(minutes=full["duration_min"])
        why = self.check_budget(at, stop)
        if why:
            return self._reject("inject_scenario", args, [why])
        cmd = {"cmd": "inject", "id": self._next_id(), "scenario": scenario, "target": target, "params": full,
               "at": wire.iso(at), "intent": intent, "severity": severity}
        try:
            Injector.for_data(self.ds, evidence=False).command(dict(cmd))      # the injector's own check, on a scratch copy
        except ValueError as e:
            return self._reject("inject_scenario", args, [str(e)])
        series = _Injection(cmd["id"], spec, target, full, at, self.validator.series_ids).series
        return self._accept(cmd, "scenario", at, stop, series, args)

    def inject_effects(self, effects: list, intent: str = "", severity: str = "", novelty: str = "",
                       start_in_minutes: float = 0) -> dict:
        args = {"effects": effects, "intent": intent, "severity": severity, "novelty": novelty, "start_in_minutes": start_in_minutes}
        bad = self._intent(intent, severity)
        if not (isinstance(novelty, str) and novelty.strip()):
            bad.append("give novelty: the plausible real failure this stands for that the named scenarios do not cover")
        if not isinstance(effects, list) or not effects:
            bad.append("effects must be a non-empty list")
        if bad:
            return self._reject("inject_effects", args, bad)
        at = self.now + pd.Timedelta(minutes=float(start_in_minutes))
        valid, reasons = [], []
        for n, raw in enumerate(effects, 1):
            if not isinstance(raw, dict):
                reasons.append(f"effect {n}: must be an object")
                continue
            offset = raw.get("start_in_minutes", 0) or 0
            try:
                start = at + pd.Timedelta(minutes=float(offset))
            except (TypeError, ValueError):
                reasons.append(f"effect {n}: start_in_minutes must be a number")
                continue
            effect = {k: v for k, v in raw.items() if k != "start_in_minutes"}
            effect["start"] = wire.iso(start)
            if raw.get("series_id") in self.layer_of and self._undescribed([raw["series_id"]]):
                reasons.append(f"effect {n} ({raw['series_id']}): describe_normal was not called on this series in this wake")
                continue
            checked, why = self.validator._check_effect(effect)           # the injector's rules, unchanged
            if why is None:
                why = self.validator._check_on_reading(checked, self._quiet_reading(start))
            if why is not None:
                reasons.append(f"effect {n} ({raw.get('series_id')}): {why}")
            else:
                valid.append((effect, start + pd.Timedelta(minutes=checked["duration_min"])))
        if not valid:
            return self._reject("inject_effects", args, reasons or ["no valid effect"])
        stop = max(end for _, end in valid)
        why = self.check_budget(at, stop)
        if why:
            return self._reject("inject_effects", args, [why])
        cmd = {"cmd": "inject", "id": self._next_id(), "effects": [e for e, _ in valid], "at": wire.iso(at),
               "intent": intent, "severity": severity}
        if reasons:                                                     # partly accepted: the rest is reported
            self.rejections.append({"at": _iso(self.now), "tool": "inject_effects", "input": args, "reasons": reasons,
                                    "id": cmd["id"], "partly_accepted": True})
        series = [e["series_id"] for e, _ in valid]
        out = self._accept(cmd, "generic", at, stop, series, args)
        self.injections[cmd["id"]].update(novelty=novelty, overlap=self.overlap(series))   # recorded, not shown to the agent
        if reasons:
            out["rejected_effects"] = reasons
        return out

    def stop(self, injection_id: str) -> dict:
        inj = self.injections.get(injection_id)
        if inj is None:
            return {"error": f"no injection {injection_id!r} of yours; yours are {sorted(self.injections)}"}
        if inj["stopped_at"] is not None or self.now >= inj["stop"]:
            return {"error": f"{injection_id} is not running (it ended at {_iso(inj['stopped_at'] or inj['stop'])})"}
        at = max(self.now, inj["at"])
        cmd = {"cmd": "stop", "id": injection_id, "at": wire.iso(at)}
        self.validator.command(cmd)
        self.commands.append(cmd)
        self.send(cmd)
        inj["stopped_at"] = at
        return {"stopped": injection_id, "at": _iso(at)}

    # ------------------------------------------------------------------------------ read-only tools
    def list_platform(self) -> dict:
        rel = self.ds.relationships()
        pods = self.ds.pods().set_index("pod")
        entities: dict = {}
        for r in self.catalog.itertuples():
            low, high, whole = self.validator.limits_of(r.series_id)
            e = entities.setdefault(f"{r.entity_kind}:{r.entity}", [])
            limit = f"{_r(low)}..{_r(high) if math.isfinite(high) else 'no max'}" + (", whole" if whole else "")
            e.append(f"{r.metric} [L{int(r.layer)}, {r.unit}, {limit}{', counter' if self.validator.is_counter(r.series_id) else ''}]")
        return {"source": "configuration (catalog and topology)",
                "how_to_read": "series id = '<metric>|<entity name>'; [layer, unit, physical limits]",
                "entities": entities,
                "pods": {p: {"cpu_limit_cores": _r(row.cpu_limit_cores), "memory_limit_bytes": int(row.memory_limit_bytes),
                             "batch_only": bool(row.batch_only)} for p, row in pods.iterrows()},
                "relationships": [f"{r.child_kind}:{r.child} {r.relation} {r.parent_kind}:{r.parent}" for r in rel.itertuples()]}

    def describe_normal(self, series_id: str | None = None, series_ids: list | None = None, days: int = 7) -> dict:
        """One series in detail, or up to 30 (`series_ids`; compact when more than 4). Marks them described."""
        ids = ([series_id] if series_id else []) + list(series_ids or [])
        if not ids or len(ids) > 30:
            return {"error": "give series_id or series_ids (1 to 30 series)"}
        unknown = [x for x in ids if x not in self.layer_of]
        if unknown:
            return {"error": f"unknown series {unknown}"}
        if len(ids) == 1:
            return self._describe(ids[0], days, compact=False)
        compact = len(ids) > 4
        return {"source": "the quiet world's history (data directory), up to now", "days": int(min(max(days, 1), 28)),
                "series": [self._describe(x, days, compact) for x in dict.fromkeys(ids)],
                **({"note": "compact: by_hour_median_utc is the median of each hour 0-23"} if compact else {})}

    def _describe(self, series_id: str, days: int, compact: bool) -> dict:
        self.described.add(series_id)
        days = int(min(max(days, 1), 28))
        s = self.ds.at(self.now).query([series_id], start=self.now - pd.Timedelta(days=days), end=self.now)
        s = s[series_id].dropna() if series_id in s else pd.Series(dtype=float)
        low, high, whole = self.validator.limits_of(series_id)
        out = {"source": "the quiet world's history (data directory), up to now", "series_id": series_id,
               "unit": self.catalog.set_index("series_id").unit[series_id], "layer": self.layer_of[series_id],
               "limits": [_r(low), _r(high) if math.isfinite(high) else None], "whole_numbers": whole,
               "counter": self.validator.is_counter(series_id), "days": days, "readings": int(len(s))}
        if s.empty:
            out["note"] = "no reading in this window (e.g. a batch pod that only runs a short while each day)"
            return out
        if out["counter"]:
            s = s.diff().clip(lower=0).dropna() * 60
            out["note"] = "a cumulative counter: shown as the increase per hour"
        by_hour = s.groupby(s.index.hour)
        out.update(last=_r(s.iloc[-1]), last_at=_iso(s.index[-1]), median=_r(s.median()), p05=_r(s.quantile(0.05)),
                   p95=_r(s.quantile(0.95)), min=_r(s.min()), max=_r(s.max()))
        if compact:
            for k in ("source", "days", "readings", "whole_numbers"):
                out.pop(k, None)
            out["by_hour_median_utc"] = [_r(by_hour.median().get(h)) if h in by_hour.groups else None for h in range(24)]
        else:
            out.update(by_hour_of_day_utc={int(h): [_r(g.quantile(0.05)), _r(g.median()), _r(g.quantile(0.95))] for h, g in by_hour},
                       by_hour_columns=["p05", "median", "p95"])
        return out

    def list_scenarios(self) -> dict:
        return {"scenarios": [{"name": s.name, "summary": s.summary, "targets": list(s.targets),
                               "params": {k: [lo, hi] for k, (lo, hi) in s.params.items()},
                               "series_by_target": {t: self._scenario_series[(s.name, t)] for t in s.targets}}
                              for s in SCENARIOS.values()],
                "note": "a parameter not given takes the middle of its range; duration_min is how long it lasts. "
                        "Describe every series of the chosen target (describe_normal) before injecting."}

    def list_active(self) -> dict:
        def row(i):
            state = ("stopped" if i["stopped_at"] is not None else
                     "scheduled" if i["at"] > self.now else "active" if self.now < i["stop"] else "ended")
            return {"injection_id": i["id"], "state": state, "kind": i["kind"], "scenario": i["scenario"], "target": i["target"],
                    "start": _iso(i["at"]), "stop": _iso(i["stopped_at"] or i["stop"]), "severity": i["severity"],
                    "intent": i["intent"]}
        b = self.config.budget
        return {"now": _iso(self.now), "active_now": len(self.active_at(self.now)), "max_active": b.max_active,
                "started_today": len(self.today(self.now)), "max_per_day": b.max_per_day,
                "injections": [row(i) for i in self.injections.values()]}


# ------------------------------------------------------------------------------------------- tools
def _schema(props, required=()):
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


_EFFECT = {"type": "object", "additionalProperties": False, "required": ["series_id", "op", "shape", "magnitude", "duration_min"],
           "properties": {"series_id": {"type": "string"}, "op": {"type": "string", "enum": ["add", "mult"]},
                          "shape": {"type": "string", "enum": ["flat", "ramp", "decay"]},
                          "magnitude": {"type": "number", "description": "add: amount in the series' unit; mult: the factor"},
                          "duration_min": {"type": "number", "exclusiveMinimum": 0},
                          "start_in_minutes": {"type": "number", "minimum": 0, "description": "after the injection's start"}}}
_INTENT = {"type": "string", "description": "the real-world failure this stands for, in one or two sentences"}
_SEVERITY = {"type": "string", "enum": list(SEVERITIES)}
_START = {"type": "number", "minimum": 0, "maximum": 180, "description": "start this many minutes from now (default 0)"}

TOOLS = [
    ("list_platform", "The platform: every entity with its series ([layer, unit, physical limits]), the pods' CPU and memory "
     "limits, and how entities are related. Configuration only.", _schema({})),
    ("describe_normal", "What is normal for one series (series_id) or up to 30 (series_ids): the quiet-world history over the "
     "last `days` days up to now, with an hour-of-day profile and the last reading. Use it to size effects. An injection may "
     "only touch series described in the same wake.",
     _schema({"series_id": {"type": "string"}, "series_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1,
                                                              "maxItems": 30},
              "days": {"type": "integer", "minimum": 1, "maximum": 28, "default": 7}})),
    ("list_scenarios", "The named failure scenarios, their targets, parameter ranges and the series each one touches on each target.",
     _schema({})),
    ("inject_scenario", "Inject a named scenario on one of its targets.",
     _schema({"scenario": {"type": "string"}, "target": {"type": "string"},
              "params": {"type": "object", "description": "parameter -> value, inside its range"},
              "intent": _INTENT, "severity": _SEVERITY, "start_in_minutes": _START}, ["scenario", "intent", "severity"])),
    ("inject_effects", "Inject a new, generic incident: a list of primitive effects on series. Each effect: series_id, op "
     "(add | mult), shape (flat | ramp: reaches the magnitude at its end | decay: starts at it and falls off), magnitude, "
     "duration_min, optional start_in_minutes after the injection's start. Effects that break a series' physical limits "
     "are rejected with the reason; the others are applied.",
     _schema({"effects": {"type": "array", "items": _EFFECT, "minItems": 1}, "intent": _INTENT, "severity": _SEVERITY,
              "novelty": {"type": "string", "description": "the plausible real failure this stands for that none of the named "
                                                           "scenarios covers, and why it is not one of them"},
              "start_in_minutes": _START}, ["effects", "intent", "severity", "novelty"])),
    ("list_active", "Your own injections (active, scheduled, ended, stopped) and today's count against the budget.", _schema({})),
    ("stop", "End one of your running injections now.", _schema({"injection_id": {"type": "string"}}, ["injection_id"])),
]


def definitions() -> list:
    return [{"name": n, "description": d, "input_schema": s} for n, d, s in TOOLS]


def call(attacker: Attacker, name: str, args: dict | None = None) -> tuple:
    """Run one tool: (result, is_error). Never raises for a bad call."""
    args = dict(args or {})
    fn = {"list_platform": attacker.list_platform, "describe_normal": attacker.describe_normal,
          "list_scenarios": attacker.list_scenarios, "inject_scenario": attacker.inject_scenario,
          "inject_effects": attacker.inject_effects, "list_active": attacker.list_active, "stop": attacker.stop}.get(name)
    if fn is None:
        return {"error": f"unknown tool {name!r}"}, True
    try:
        result = fn(**args)
    except TypeError as e:
        return {"error": f"bad arguments: {e}"}, True
    if len(dumps(result)) > MAX_RESULT_CHARS:
        return {"error": "result too large"}, True
    return result, "error" in result


def dumps(x) -> str:
    return json.dumps(x, separators=(",", ":"), default=str)


# --------------------------------------------------------------------------------------- the agent
def system_prompt(config: Config) -> str:
    return SYSTEM.replace("{gap}", f"{config.budget.min_gap_minutes / 60:g} hours")


def anthropic_create():
    import anthropic
    return anthropic.Anthropic(max_retries=2).messages.create


def _field(block, name):
    return block.get(name) if isinstance(block, dict) else getattr(block, name, None)


def _dump(block) -> dict:
    if hasattr(block, "model_dump"):
        return block.model_dump(mode="json", exclude_none=True)
    return block if isinstance(block, dict) else dict(vars(block))


class Agent:
    """The tool-use loop for one wake: a fresh conversation, the wake message, tools until the agent
    ends its turn or a budget runs out, then its note."""

    def __init__(self, attacker: Attacker, create=None, clock=time.monotonic):
        self.attacker, self.config = attacker, attacker.config
        self._create, self.clock = create, clock
        self.wakes: list = []
        self.cost = 0.0
        self.tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}

    @property
    def create(self):
        if self._create is None:
            self._create = anthropic_create()
        return self._create

    def wake_text(self, now, last_of_day: bool) -> str:
        a, b = self.attacker, self.config.budget
        mine = list(a.injections.values())
        kinds = {k: sum(i["kind"] == k for i in mine) for k in ("scenario", "generic")}
        sev = {s: sum(i["severity"] == s for i in mine) for s in SEVERITIES}
        layers = {f"L{l}": sum(l in i["layers"] for i in mine) for l in (1, 2, 3)}
        today = len(a.today(now))
        lines = [f"It is now {now:%Y-%m-%d %H:%M} UTC (simulated). You are woken every {self.config.wake_every_minutes:g} minutes.",
                 f"Budget: at most {b.max_per_day} injections starting per day (aim for {b.min_per_day} to {b.max_per_day}), at most "
                 f"{b.max_active} active at once, starts at least {b.min_gap_minutes / 60:g} h apart. Today so far: {today}. "
                 f"Active now: {len(a.active_at(now))}.",
                 f"So far in the campaign: {len(mine)} injections; named {kinds['scenario']}, generic {kinds['generic']}; "
                 f"noticeable {sev['noticeable']}, subtle {sev['subtle']}; touching layers {layers}.",
                 *(f"- {i['id']}: {i['kind']} {i['scenario'] or ''} {', '.join(i['entities'][:4])} from {i['at']:%m-%d %H:%M} to "
                   f"{(i['stopped_at'] or i['stop']):%m-%d %H:%M}, {i['severity']}: {i['intent']}" for i in mine[-8:])]
        if a.window:
            lines.append(f"The campaign runs until {a.window[1]:%Y-%m-%d %H:%M} UTC.")
        if last_of_day and today < b.min_per_day:
            lines.append(f"This is the last wake of the day and fewer than {b.min_per_day} injections started today.")
        lines.append("Decide whether to inject now. Doing nothing this time is allowed.")
        return "\n".join(lines)

    def wake(self, now, last_of_day: bool = False) -> dict:
        a, cfg, b = self.attacker, self.config, self.config.budget
        a.now = utc(now)
        a.described = set()                                 # describe_normal counts only within this wake
        began = self.clock()
        messages = [{"role": "user", "content": self.wake_text(a.now, last_of_day)}]
        calls, api, stop, final, used = [], [], None, False, 0
        record = {"now": _iso(a.now), "commands_before": len(a.commands)}
        while True:
            if not final and (len(calls) >= b.max_tool_calls_per_wake or self.clock() - began > b.max_seconds_per_wake
                              or used > b.max_tokens_per_wake):
                final, stop = True, "a per-wake budget ran out"
                messages.append({"role": "user", "content": [{"type": "text", "text": "The budget for this wake is used up; "
                                                              "tools are off. Write your short note now."}]})
            kwargs = {"model": cfg.model, "max_tokens": cfg.max_tokens_per_call, "system": system_prompt(cfg), "tools": definitions(),
                      "messages": messages, "tool_choice": {"type": "none" if final else "auto"}}
            if cfg.effort:
                kwargs["output_config"] = {"effort": cfg.effort}
            try:
                response = self.create(**kwargs)
            except Exception as e:                              # the wake ends; the campaign goes on
                stop = f"API request failed: {type(e).__name__}: {e}"[:300]
                break
            usage = _field(response, "usage")
            got = {"input": int(getattr(usage, "input_tokens", 0) or 0), "output": int(getattr(usage, "output_tokens", 0) or 0),
                   "cache_read": int(getattr(usage, "cache_read_input_tokens", 0) or 0),
                   "cache_write": int(getattr(usage, "cache_creation_input_tokens", 0) or 0)}
            used += sum(got.values())
            price = cfg.prices_per_mtok.get(_field(response, "model") or cfg.model)
            cost = sum(got[k] * price[k] for k in got) / 1e6 if price else 0.0
            self.cost += cost
            for k in got:
                self.tokens[k] += got[k]
            api.append({"usage": got, "cost": round(cost, 5), "stop_reason": _field(response, "stop_reason")})
            content = list(_field(response, "content") or [])
            messages.append({"role": "assistant", "content": content})
            uses = [blk for blk in content if _field(blk, "type") == "tool_use"]
            if not uses or final:
                record["note"] = "".join(_field(blk, "text") or "" for blk in content if _field(blk, "type") == "text").strip()
                break
            results = []
            for blk in uses:
                name, args = _field(blk, "name"), _field(blk, "input") or {}
                if len(calls) >= b.max_tool_calls_per_wake:
                    result, is_error = {"error": "tool-call budget for this wake used up"}, True
                else:
                    result, is_error = call(a, name, args)
                    calls.append({"tool": name, "input": args, "result": result, "is_error": is_error})
                results.append({"type": "tool_result", "tool_use_id": _field(blk, "id"), "content": dumps(result),
                                **({"is_error": True} if is_error else {})})
            messages.append({"role": "user", "content": results})
        record.update(stop=stop, tool_calls=calls, api_requests=api, seconds=round(self.clock() - began, 2),
                      cost_usd=round(sum(r["cost"] for r in api), 5),
                      commands=a.commands[record.pop("commands_before"):],
                      messages=[{"role": m["role"], "content": m["content"] if isinstance(m["content"], str)
                                 else [_dump(blk) for blk in m["content"]]} for m in messages])
        record.setdefault("note", "")
        self.wakes.append(record)
        return record


def wake_times(start, end, every_minutes: float) -> list:
    return list(pd.date_range(utc(start), utc(end), freq=pd.Timedelta(minutes=every_minutes), inclusive="left"))


def run_campaign(attacker: Attacker, agent: Agent, start, end, log=lambda line: None) -> dict:
    """The live loop at max speed over [start, end): wake, decide, record. Returns the record."""
    times = wake_times(start, end, attacker.config.wake_every_minutes)
    for n, t in enumerate(times):
        last_of_day = n + 1 == len(times) or times[n + 1].floor("D") != t.floor("D")
        w = agent.wake(t, last_of_day)
        log(f"{t:%m-%d %H:%M}: {len(w['tool_calls'])} tool calls, {len(w['commands'])} commands, ${w['cost_usd']:.4f}"
            + (f" ({w['stop']})" if w["stop"] else ""))
    return record_of(attacker, agent, start, end, mode="campaign")


def run_live(attacker: Attacker, agent: Agent, start, speed: float, end=None, sleep=time.sleep, wall=time.monotonic,
             log=lambda line: None) -> dict:
    """Wake every `wake_every_minutes` of stream time, the stream clock being start + elapsed wall time x speed
    (the publisher's pacing). Commands are scheduled at least `lead_minutes` ahead of the clock when sent."""
    began, start = wall(), utc(start)
    lead = pd.Timedelta(minutes=attacker.config.lead_minutes)
    t = start
    while end is None or t < utc(end):
        wait = (t - start).total_seconds() / speed - (wall() - began)
        if wait > 0:
            sleep(wait)
        attacker.now = t
        clock = start + pd.Timedelta(seconds=(wall() - began) * speed)
        agent.wake(max(t, clock + lead))                     # decisions are made for a time the publisher has not reached
        log(f"woke at {t:%m-%d %H:%M}: {len(attacker.commands)} commands so far")
        t = t + pd.Timedelta(minutes=attacker.config.wake_every_minutes)
    return record_of(attacker, agent, start, end, mode="live")


def record_of(attacker: Attacker, agent: Agent, start, end, mode: str) -> dict:
    return {"mode": mode, "window": [_iso(start), None if end is None else _iso(end)], "config": asdict(attacker.config),
            "system": system_prompt(attacker.config), "tools": definitions(), "commands": attacker.commands, "rejections": attacker.rejections,
            "injections": [{**i, "at": _iso(i["at"]), "stop": _iso(i["stop"]),
                            "stopped_at": None if i["stopped_at"] is None else _iso(i["stopped_at"]),
                            "decided_at": _iso(i["decided_at"])} for i in attacker.injections.values()],
            "wakes": agent.wakes, "cost_usd": round(agent.cost, 4), "tokens": agent.tokens}


def save(record: dict, folder: Path) -> tuple:
    """record.json (everything) and control.jsonl (the commands, one per line: the replayable control stream)."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "record.json").write_text(json.dumps(record, indent=1, default=str))
    control = folder / "control.jsonl"
    control.write_text("".join(wire.dumps(c).decode() + "\n" for c in record["commands"]))
    return folder / "record.json", control


def load_commands(path) -> list:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


# ------------------------------------------------------------------------------------------ replay
def replay(ds, start, end, commands: list, stream_version: int = 2, history_start=None) -> tuple:
    """The recorded control stream played through the injector without the LLM and without Kafka, the way
    the publisher plays a schedule loaded in advance: (the telemetry messages sent, the injection record).
    Byte for byte what `publisher.publish(..., control=..., stream_version=...)` writes for these commands."""
    from sentinel_stream.publisher import messages, sendable

    start = utc(start)
    end_ts = ds.now if end is None else min(utc(end), ds.now)
    inj = Injector.for_data(ds, evidence=stream_version >= 2)
    record = []
    for cmd in commands:
        if cmd.get("at") is None or utc(cmd["at"]) < start:       # as the publisher: only a schedule for its stream
            continue
        try:
            inj.command(cmd)
        except ValueError as e:
            record.append({"event": "rejected", "id": str(cmd.get("id", "")), "reasons": [str(e)]})
    sent = []
    for m in messages(ds, start, end, history_start, stream_version):
        out, new = inj.apply(m)
        record += new
        if sendable(out):
            sent.append(out)
    record.append(published_record(start, end_ts, stream_version))
    return sent, record


# --------------------------------------------------------------------------------------------- MCP
def build_mcp_server(attacker: Attacker):
    """The seven attacker tools over MCP (a tool group of its own; see mcp_server.py --group attacker)."""
    import mcp_types as types
    from mcp.server import Server

    async def list_tools(_request, _params):
        return types.ListToolsResult(tools=[types.Tool(name=d["name"], description=d["description"], input_schema=d["input_schema"])
                                            for d in definitions()])

    async def call_tool(_request, params):
        result, is_error = call(attacker, params.name, params.arguments or {})
        return types.CallToolResult(content=[types.TextContent(type="text", text=dumps(result))], is_error=is_error)

    return Server("sentinel-attacker", instructions=f"Inject incidents into the live stream; the clock is {_iso(attacker.now)}.",
                  on_list_tools=list_tools, on_call_tool=call_tool)


def mcp_main(argv=None) -> int:
    import anyio
    from mcp.server.stdio import stdio_server

    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    ap = argparse.ArgumentParser(description="Serve the attacker's tools over MCP (stdio).")
    ap.add_argument("--data", required=True)
    ap.add_argument("--now", required=True, help="the stream time the tools act at (injections start from it)")
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--control-topic", default=CONTROL_TOPIC)
    args = ap.parse_args(argv)
    bootstrap = args.bootstrap or broker.DEFAULT_BOOTSTRAP
    from sentinel_stream.injector import send
    attacker = Attacker(DataSource(args.data), send=lambda cmd: send(bootstrap, args.control_topic, [cmd]),
                        id_prefix=f"mcp-{uuid.uuid4().hex[:6]}")
    attacker.now = utc(args.now)
    server = build_mcp_server(attacker)

    async def serve():
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())

    anyio.run(serve)
    return 0


# --------------------------------------------------------------------------------------------- CLI
def main(argv=None) -> int:
    from sentinel_sim.replay import DataSource

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="mode", required=True)
    camp = sub.add_parser("campaign", help="run the loop at max speed over a window; record everything")
    live = sub.add_parser("live", help="wake with the publisher's clock and send to the control topic")
    rep = sub.add_parser("replay", help="the stream a recorded control stream gives, without the LLM")
    for p in (camp, live, rep):
        p.add_argument("--data", required=True)
        p.add_argument("--start", required=True)
    camp.add_argument("--end", required=True)
    camp.add_argument("--name", default=None, help="folder under runs/attacker/ for the record")
    camp.add_argument("--send", action="store_true", help="also write the commands to the control topic")
    live.add_argument("--speed", type=float, default=60.0)
    live.add_argument("--end", default=None)
    for p in (camp, live):
        p.add_argument("--bootstrap", default=None)
        p.add_argument("--control-topic", default=CONTROL_TOPIC)
        p.add_argument("--model", default=None)
        p.add_argument("--wake-every", type=float, default=None, help="minutes of simulated time between wakes")
    rep.add_argument("--end", required=True)
    rep.add_argument("--commands", required=True, help="a control.jsonl written by a campaign")
    rep.add_argument("--stream-version", type=int, default=2)
    args = ap.parse_args(argv)
    ds = DataSource(args.data)

    if args.mode == "replay":
        sent, record = replay(ds, args.start, args.end, load_commands(args.commands), args.stream_version)
        print(f"{len(sent)} telemetry messages; injection record: {len(record)} entries")
        return 0
    config = Config.load(model=args.model, wake_every_minutes=args.wake_every)
    send = None
    if args.mode == "live" or args.send:
        from sentinel_stream import broker
        from sentinel_stream.injector import send as send_commands
        bootstrap = args.bootstrap or broker.DEFAULT_BOOTSTRAP
        send = lambda cmd: send_commands(bootstrap, args.control_topic, [cmd])        # noqa: E731
    log = lambda line: print(line, flush=True)                                          # noqa: E731
    if args.mode == "campaign":
        attacker = Attacker(ds, config, send, window=(args.start, args.end))
        record = run_campaign(attacker, Agent(attacker), args.start, args.end, log)
    else:
        attacker = Attacker(ds, config, send, id_prefix=f"live-{uuid.uuid4().hex[:6]}")
        record = run_live(attacker, Agent(attacker), args.start, args.speed, args.end, log=log)
    name = getattr(args, "name", None) or f"{args.mode}-{pd.Timestamp.now(tz='UTC'):%Y%m%dT%H%M%S}"
    paths = save(record, Path(config.runs_dir) / "attacker" / name)
    log(f"{len(record['commands'])} commands, {len(record['rejections'])} rejections, ${record['cost_usd']:.3f}; saved {paths[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
