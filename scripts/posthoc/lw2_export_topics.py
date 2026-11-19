"""POST-HOC. Export the live-week-2 topics to runs/live-week-2/topics/*.json (gitignored), so the other
lw2_* scripts run without the broker. Read-only on the broker.

    python scripts/posthoc/lw2_export_topics.py
"""
from __future__ import annotations

import json

from lw2_common import RUN, TOPICS, TRIAGE_RUNS


def main() -> None:
    from sentinel_stream import broker
    topics = dict(json.loads((RUN / "replay.json").read_text())["topics"])
    for k in TRIAGE_RUNS:
        topics[f"triage-r{k}"] = json.loads((RUN / f"triage-r{k}.json").read_text())["topic"]
    TOPICS.mkdir(parents=True, exist_ok=True)
    for name, topic in topics.items():
        rows = broker.read_topic(broker.DEFAULT_BOOTSTRAP, topic)
        (TOPICS / f"{name}.json").write_text(json.dumps(rows, default=str))
        print(f"{name:12s} {topic:28s} {len(rows):6d} records")


if __name__ == "__main__":
    main()
