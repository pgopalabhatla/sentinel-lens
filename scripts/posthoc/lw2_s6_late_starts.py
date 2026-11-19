"""POST-HOC, section 6 of docs/results/live-week-2-posthoc.md: injections that started too late in the
stream to be caught realistically (the protocol's planned list). For each: start, stop, why it stopped,
and how much of its stop + 30 min matching tail is inside the stream.

    python scripts/posthoc/lw2_s6_late_starts.py
"""
from __future__ import annotations

import pandas as pd

from lw2_common import load


def main() -> None:
    from sentinel_stream import live_score
    key = load("answer_key")
    injs, (s0, s1) = live_score.injections(key)
    reasons = {r["id"]: r.get("reason") for r in key if r["event"] == "stopped"}
    print(f"stream {s0} - {s1}")
    for i in injs:
        tail_end = i["stop"] + live_score.TAIL
        print(f"{i['id']}  {i['start']:%b %d %H:%M} - {i['stop']:%b %d %H:%M}  stopped: {reasons.get(i['id'])}  "
              f"hours from stop to end of stream: {(s1 - i['stop']) / pd.Timedelta('1h'):.1f}  "
              f"tail inside stream: {tail_end <= s1}")
    last = max(injs, key=lambda i: i["start"])
    print(f"\nlast start: {last['id']} at {last['start']}, {(s1 - last['start']) / pd.Timedelta('1h'):.1f} h before the end")


if __name__ == "__main__":
    main()
