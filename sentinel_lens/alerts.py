"""One record for every alert, whatever detector raised it.

Each detector returns its own table (z-scores, counter increases, late runs, ...). The adapters here
turn all of them into the same `Alert` record: time, target, detector, score (0-1), direction,
explanation. The correlator, the feedback loop and the UI then only need to know this one shape.

How the score is made
---------------------
Every detector has a *measure* (how far out the reading is, in the detector's own terms) and an
*alert line* (the smallest measure that alerts):

    detector            measure                                   alert line
    seasonal            |z|                                       threshold (6)
    job_run_baseline    |z|                                       threshold (6)
    saturation          horizon / time_to_full  (1 = just inside  1
                        the horizon, 2 = full in half the horizon)
    counter_rule        increase since the previous reading       1  (one restart / one failure)
    above_zero          the value                                 1  (one partition)
    start_late,         1 (the condition is met or it is not)     1
    sla_breach, failed_run

A score formula turns (measure, line, detector) into a number in [0, 1]. It is pluggable: pass
`formula=` to any adapter. `measure` and `line` are kept on the record so alerts can be re-scored
with another formula without running the detectors again.

When persistence is involved (seasonal, saturation) the alert fires at the reading where 3 of the
last 5 are hot, and that reading need not be the worst or even hot itself. The alert then carries
the largest measure among the hot readings in that persistence window, with that reading's
direction and explanation.

Direction is "up" or "down": which side of normal the measure is on. One-sided rules (counters,
schedule, saturation) are always "up".
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from sentinel_lens.detectors.persistence import persistent_alerts

DIRECTIONS = ("up", "down")
# formula(measure, line, detector) -> score in [0, 1]
ScoreFormula = Callable[[float, float, str], float]


# ------------------------------------------------------------------------------------- formulas
def ratio_formula(f: Callable[[float], float]) -> ScoreFormula:
    """Make a score formula from a function of r = measure / line, clipped into [0, 1]."""
    def formula(measure: float, line: float, detector: str) -> float:
        if line <= 0:
            raise ValueError(f"alert line must be positive, got {line!r} for {detector}")
        r = measure / line
        if math.isnan(r):
            raise ValueError(f"cannot score a missing measure for {detector}")
        return float(min(1.0, max(0.0, f(r))))
    return formula


def _half_at_line(r: float) -> float:          # (1) 0.5 at the line, 1.0 at twice the line
    return r / 2


def _exponential(r: float) -> float:           # (2) 0 at the line, 0.63 at twice the line, -> 1
    return 1 - math.exp(-(r - 1))


def _log4(r: float) -> float:                  # (3) 0 at the line, 0.5 at twice, 1.0 at four times
    return math.log(r) / math.log(4) if r > 0 else 0.0


linear_score = ratio_formula(_half_at_line)
exponential_score = ratio_formula(_exponential)
log_score = ratio_formula(_log4)


def percentile_formula(reference: dict[str, Iterable[float]]) -> ScoreFormula:
    """(4) Score = share of a quiet reference period's measures that are smaller than this one.

    reference  {detector: measures of that detector over the reference period (e.g. the warm-up
               week), one per reading/run, alerting or not}. A detector without a reference raises.
    """
    ref = {d: np.sort(np.asarray([v for v in vals if not _isnan(v)], dtype=float))
           for d, vals in reference.items()}

    def formula(measure: float, line: float, detector: str) -> float:
        if detector not in ref or len(ref[detector]) == 0:
            raise KeyError(f"no reference measures for detector {detector!r}")
        if _isnan(measure):
            raise ValueError(f"cannot score a missing measure for {detector}")
        return float(np.searchsorted(ref[detector], measure, side="left") / len(ref[detector]))
    return formula


def _isnan(v) -> bool:
    return isinstance(v, float) and math.isnan(v)


# Placeholder default until one is chosen from scripts/score_formulas.py: the simplest to explain.
DEFAULT_FORMULA: ScoreFormula = linear_score


# --------------------------------------------------------------------------------------- record
@dataclass(frozen=True)
class Alert:
    time: pd.Timestamp       # when the alert became true (UTC)
    target: str              # a series_id ("metric|entity") or an entity ("dag:daily_orders")
    detector: str            # the rule that raised it; this is the scorer's `rule`
    score: float             # 0-1, from the score formula
    direction: str           # "up" or "down"
    explanation: str         # plain language, from the detector
    measure: float           # how far out, in the detector's own terms (see module docstring)
    line: float              # the smallest measure that alerts

    def __post_init__(self):
        t = pd.Timestamp(self.time)
        object.__setattr__(self, "time", t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC"))
        if not (isinstance(self.score, (int, float, np.floating)) and 0.0 <= self.score <= 1.0):
            raise ValueError(f"score must be between 0 and 1, got {self.score!r} ({self.detector}, {self.target})")
        if self.direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}, got {self.direction!r}")
        if not self.explanation:
            raise ValueError(f"an alert needs an explanation ({self.detector}, {self.target})")


def _alert(time, target, detector, measure, line, direction, explanation, formula) -> Alert:
    measure, line = float(measure), float(line)
    return Alert(time=time, target=str(target), detector=detector, score=formula(measure, line, detector),
                 direction=direction, explanation=str(explanation), measure=measure, line=line)


def to_frame(alerts: Iterable[Alert]) -> pd.DataFrame:
    """Alerts as a table the scorer accepts: ts, target, rule (= detector), plus score, direction,
    explanation, measure, line. The scorer ignores the extra columns and passes them through."""
    cols = ["ts", "target", "rule", "score", "direction", "explanation", "measure", "line"]
    rows = [asdict(a) for a in alerts]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows).rename(columns={"time": "ts", "detector": "rule"})
    df["ts"] = pd.to_datetime(df.ts, utc=True)
    return df[cols]


def rescore(alerts: Iterable[Alert], formula: ScoreFormula) -> list[Alert]:
    """The same alerts with their scores recomputed by another formula."""
    return [_alert(a.time, a.target, a.detector, a.measure, a.line, a.direction, a.explanation, formula)
            for a in alerts]


# -------------------------------------------------------------------------------------- adapters
def _persistent(scores: pd.DataFrame, measure: np.ndarray, k: int, n: int):
    """Yield (position of the alert reading, position of the worst hot reading in its window).

    Which readings alert is decided by `persistent_alerts` itself, so an adapter can never fire at
    different times than the detector pipeline does.
    """
    hot = pd.Series(scores.hot.to_numpy(), index=pd.DatetimeIndex(scores.ts))
    is_hot = hot.fillna(False).to_numpy(dtype=bool)
    for i in hot.index.get_indexer(persistent_alerts(hot, k=k, n=n)):
        lo = max(0, i - n + 1)
        window = np.where(is_hot[lo:i + 1], measure[lo:i + 1], -np.inf)
        yield i, lo + int(np.argmax(window))


def alerts_from_seasonal(scores: pd.DataFrame, target: str, detector: str = "seasonal", threshold: float = 6.0,
                         k: int = 3, n: int = 5, formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`seasonal_scores` output -> alerts after k-of-n persistence.

    measure = the largest |z| among the hot readings in the persistence window, line = `threshold`
    (must be the threshold the scores were made with).
    """
    z = scores.z.abs().to_numpy(dtype=float)
    ts, direction, explanation = scores.ts.to_numpy(), scores.direction.to_numpy(), scores.explanation.to_numpy()
    return [_alert(ts[i], target, detector, z[j], threshold, direction[j], explanation[j], formula)
            for i, j in _persistent(scores, z, k, n)]


def alerts_from_saturation(scores: pd.DataFrame, target: str, horizon_h: float = 4.0, k: int = 3, n: int = 5,
                           formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`saturation_scores` output -> alerts after k-of-n persistence.

    measure = horizon / time_to_full for the most urgent hot reading in the persistence window
    (1 = full exactly at the horizon, 2 = full in half that time), line = 1. `horizon_h` must be
    the horizon the scores were made with.
    """
    with np.errstate(all="ignore"):
        urgency = horizon_h / scores.time_to_full_h.to_numpy(dtype=float)
    urgency = np.where(np.isnan(urgency), -np.inf, urgency)
    ts, explanation = scores.ts.to_numpy(), scores.explanation.to_numpy()
    return [_alert(ts[i], target, "saturation", urgency[j], 1.0, "up", explanation[j], formula)
            for i, j in _persistent(scores, urgency, k, n)]


def alerts_from_counter(frame: pd.DataFrame, target: str, formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`counter_increase_alerts` output -> alerts. measure = the increase, line = 1 (one event)."""
    return [_alert(r.ts, target, "counter_rule", r.increase, 1.0, "up", r.explanation, formula)
            for r in frame.itertuples()]


def alerts_from_above_zero(frame: pd.DataFrame, target: str, formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`above_zero_alerts` output -> alerts. measure = the value, line = 1 (one unit above zero)."""
    return [_alert(r.ts, target, "above_zero", r.value, 1.0, "up", r.explanation, formula)
            for r in frame.itertuples()]


def alerts_from_schedule(frame: pd.DataFrame, formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`schedule_alerts` / `failed_run_alerts` output -> alerts (detector = the row's rule:
    start_late, sla_breach or failed_run). These rules are yes/no, so measure = line = 1."""
    return [_alert(r.ts, r.target, r.rule, 1.0, 1.0, "up", r.explanation, formula) for r in frame.itertuples()]


def alerts_from_job_runs(frame: pd.DataFrame, threshold: float = 6.0,
                         formula: ScoreFormula = DEFAULT_FORMULA) -> list[Alert]:
    """`job_run_alerts` output -> alerts. measure = |z|, line = `threshold` (the one the alerts were
    made with). slower -> "up", faster -> "down"; the explanation keeps the detector's own words."""
    side = {"slower": "up", "faster": "down"}
    return [_alert(r.ts, r.target, "job_run_baseline", abs(r.z), threshold, side[r.direction], r.explanation, formula)
            for r in frame.itertuples()]
