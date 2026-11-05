import numpy as np
import pandas as pd

STEPS_PER_DAY = 288  # 5-minute readings


def daily_series(days: int = 8, level: float = 100.0, amplitude: float = 30.0, noise: float = 1.0,
                 seed: int = 0, name: str = "test_metric|test-entity") -> pd.Series:
    """5-minute series with a daily cycle and small Gaussian noise; the last day is the one we score."""
    idx = pd.date_range("2026-01-01", periods=days * STEPS_PER_DAY, freq="5min", tz="UTC")
    hours = idx.hour + idx.minute / 60
    rng = np.random.default_rng(seed)
    values = level + amplitude * np.sin(2 * np.pi * hours / 24) + rng.normal(0, noise, len(idx))
    return pd.Series(values, index=idx, name=name)


def last_day(s: pd.Series) -> pd.DatetimeIndex:
    return s.index[-STEPS_PER_DAY:]
