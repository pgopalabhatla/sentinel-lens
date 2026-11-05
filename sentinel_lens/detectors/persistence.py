"""Persistence check: only alert when a problem lasts, so single-reading blips stay quiet."""
from __future__ import annotations

import pandas as pd


def persistent_alerts(hot: pd.Series, k: int = 3, n: int = 5) -> pd.DatetimeIndex:
    """Times where at least `k` of the last `n` readings (including this one) are hot.

    Missing readings count as not hot. Uses only the current and earlier readings.
    """
    count = hot.fillna(False).astype(int).rolling(n, min_periods=1).sum()
    return hot.index[(count >= k).to_numpy()]
