"""How fast is the streaming engine? Wall time per simulated minute over the test week.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/measure_stream.py

Replays the test week through sentinel_stream, warm-started on everything before it, and times
each simulated minute (the minute of metrics plus the DAG-run events and deadlines that fall in it).
Reads no labels and scores nothing; the output of the run is checked against the batch pipeline in
tests/test_stream_test_week.py, not here.
"""
import time
import warnings
from collections import Counter

import numpy as np
import pandas as pd

from scripts.run_test_week import DATA, HISTORY_START, SPLITS
from sentinel_stream import AlertEvent, PageEvent, StreamEngine, run_events
from sentinel_sim.replay import DataSource


def main():
    warnings.simplefilter("ignore", RuntimeWarning)
    lo, hi = SPLITS["test"]
    ds = DataSource(DATA).at(hi)
    ds.query("kafka_consumergroup_lag|orders-etl", window="1min")       # load the dataset before timing

    t0 = time.perf_counter()
    engine = StreamEngine.warm_start(ds, lo, history_start=HISTORY_START)
    warm = time.perf_counter() - t0

    events = run_events(ds, lo, hi, HISTORY_START)
    rows, i, n_out = [], 0, Counter()
    for ts, values in ds.stream(lo, hi):
        t0 = time.perf_counter()
        out = []
        while i < len(events) and events[i].time <= ts:
            out += engine.on_run_event(events[i])
            i += 1
        chunk_before = engine.seasonal._chunk_start
        out += engine.on_minute(ts, values)
        ms = 1000 * (time.perf_counter() - t0)
        kind = ("builds the next day's baselines" if engine.seasonal._chunk_start != chunk_before
                else "closes a 5-minute bucket" if (ts + pd.Timedelta("1min")).floor("5min") > ts.floor("5min")
                else "ordinary minute")
        rows.append({"ms": ms, "kind": kind, "outputs": len(out)})
        n_out.update("alert" if isinstance(e, AlertEvent) else f"page {e.kind}" for e in out if isinstance(e, (AlertEvent, PageEvent)))
    df = pd.DataFrame(rows)

    def line(name, ms):
        return {"minutes": name, "n": len(ms), "mean_ms": round(ms.mean(), 2), "median_ms": round(float(np.median(ms)), 2),
                "p95_ms": round(float(np.percentile(ms, 95)), 2), "p99_ms": round(float(np.percentile(ms, 99)), 2),
                "max_ms": round(ms.max(), 1)}

    table = [line("all", df.ms)] + [line(k, g.ms) for k, g in df.groupby("kind", sort=False)]
    total = df.ms.sum() / 1000
    print(f"Test week {lo} to {hi}: {len(df)} simulated minutes, {len(events)} DAG-run events.")
    print(f"Series watched: {len(engine.seasonal.series_ids)} seasonal, {len(engine.guardrails.counter_ids)} counters, "
          f"{len(engine.guardrails.zero_ids)} must-be-zero.")
    print(f"Warm start from history: {warm:.2f} s. Replay: {total:.1f} s, "
          f"{1000 * total / len(df):.2f} ms per simulated minute ({60 / (total / len(df)):,.0f}x real time).\n")
    print(pd.DataFrame(table).to_string(index=False))
    print(f"\nOutputs: {dict(n_out)}")


if __name__ == "__main__":
    main()
