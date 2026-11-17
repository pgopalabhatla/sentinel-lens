"""Dispatcher: one triage agent per page-out, notes on the `triage` topic.

    python -m sentinel_stream.dispatcher --data data_live          # follows `pages` until stopped

It reads `pages` from the beginning, in order. For every `paged` record (a page-out) it starts an
investigator (investigator.py) on that page AS IT WAS AT THAT MOMENT (context_tools.Context.at_page:
the page records up to that one, the readings stamped up to then) and writes the note it returns to
the topic `triage`, keyed by the page's id. At most `concurrency` (3) investigations run at once.

What goes on `triage`:
    {"type": "note", "page_id", "page", "verdict", ... }        one per page-out (investigator.py has the fields)
    {"type": "link", "link_id", "page", "page_id", "absorbed", "notes": [page_id, ...], "at"}
            a page that had paged (or several) merged with another page later. The notes of every page
            involved are linked under the merged page; nothing is investigated again.

Notes are ADVISORY. Nothing reads `triage` to change a page, a detector, a threshold or the feedback;
an engineer reads it next to the page.

Restarts. The dispatcher keeps nothing on disk; `triage` is its memory. On start it reads `triage`
(which page ids have a note, which links are written), then reads `pages` from the beginning again.
A page-out whose page id already has a note is not investigated again; a link already written is not
written again. Links are recomputed from the page records each time, so they come out the same. An
investigation that was running when the dispatcher died left no note and is started again (a fresh
agent): a page can be investigated twice, its note is written once.

Limits: one dispatcher at a time (two would both investigate and both write); the readings for a page
are read from `telemetry` when the page-out is read, which holds them because the consumer writes a
page-out only after it has processed the readings that caused it.
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from sentinel_stream import wire
from sentinel_stream.context_tools import Catalog, Context, StreamStore


@dataclass(frozen=True)
class Topics:
    telemetry: str = "telemetry"
    alerts: str = "alerts"
    pages: str = "pages"
    triage: str = "triage"


class MemoryTopics:
    """Topics as lists in memory, for the tests."""

    def __init__(self, **topics):
        self.topics = {k: list(v) for k, v in topics.items()}
        self.lock = threading.Lock()

    def read(self, topic: str, start: int = 0) -> list:
        with self.lock:
            return list(self.topics.get(topic, [])[start:])

    def write(self, topic: str, key: str, record: dict) -> None:
        with self.lock:
            self.topics.setdefault(topic, []).append(wire.loads(wire.dumps(record)))


class KafkaTopics:
    """The same two calls over the broker. `write` returns when the broker has the record."""

    def __init__(self, bootstrap: str, topics: Topics):
        from confluent_kafka import Producer

        from sentinel_stream import broker
        self.bootstrap, self._broker = bootstrap, broker
        broker.ensure_topics(bootstrap, [topics.telemetry, topics.alerts, topics.pages, topics.triage])
        self.producer = Producer(broker.client_config(bootstrap, **{"enable.idempotence": True}))
        self.lock = threading.Lock()

    def read(self, topic: str, start: int = 0) -> list:
        return self._broker.read_topic(self.bootstrap, topic, start)

    def write(self, topic: str, key: str, record: dict) -> None:
        failed = []
        with self.lock:
            self.producer.produce(topic, value=wire.dumps(record), key=key,
                                  on_delivery=lambda err, msg: failed.append(err) if err is not None else None)
            self.producer.flush()
        if failed:
            raise RuntimeError(f"the broker rejected a triage record: {failed[0]}")


@dataclass
class Dispatcher:
    io: object                        # MemoryTopics or KafkaTopics
    ds: object                        # DataSource of the data directory (catalog, logs, events, changes)
    investigate: object               # Context -> note (dict); e.g. Investigator(...).investigate
    topics: Topics = field(default_factory=Topics)
    concurrency: int = 3
    log: object = field(default=lambda line: print(line, file=sys.stderr, flush=True))
    stream_version: int = 1           # 2: logs, events and changes are read from `telemetry` (context_tools)

    def __post_init__(self):
        self.store = StreamStore()
        self.catalog = Catalog.from_ds(self.ds)
        self._offset = {"telemetry": 0, "alerts": 0, "pages": 0}
        self.noted: set = set()          # page ids with a note on `triage`
        self.linked: set = set()         # link ids on `triage`
        self.started: set = set()        # page ids investigated by this process
        self._notes_of: dict = {}        # page number -> page ids of the page-outs it now holds
        self._pool = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="triage")
        self._futures: list = []
        self.investigated = self.failed = 0
        for r in self.io.read(self.topics.triage):
            if r.get("type") == "note":
                self.noted.add(r["page_id"])
            elif r.get("type") == "link":
                self.linked.add(r["link_id"])

    # ------------------------------------------------------------------------------------ reading
    def _read(self, name: str) -> list:
        records = self.io.read(getattr(self.topics, name), self._offset[name])
        self._offset[name] += len(records)
        return records

    def _catch_up(self) -> None:
        for m in self._read("telemetry"):
            self.store.add_telemetry(m)
        for r in self._read("alerts"):
            self.store.add_alert(r)

    def step(self) -> int:
        """Handle the page records that arrived since the last step. Returns how many there were."""
        new = self._read("pages")
        for record in new:
            self.store.add_page(record)
            index = len(self.store.pages) - 1
            key = record["page"]
            if record["kind"] == "paged":
                self._notes_of.setdefault(key, set()).add(record["page_id"])
                if record["page_id"] not in self.noted and record["page_id"] not in self.started:
                    self._catch_up()
                    ctx = Context.at_page(self.store, self.ds, index, self.catalog, self.stream_version)
                    self.started.add(record["page_id"])
                    self._futures.append(self._pool.submit(self._investigate, ctx))
            elif record["kind"] == "merged":
                involved = set(self._notes_of.pop(key, set()))
                for k in record["absorbed"]:
                    involved |= self._notes_of.pop(k, set())
                if involved:
                    self._notes_of[key] = involved
                    self._link(record, involved)
        self._futures = [f for f in self._futures if not f.done()]
        return len(new)

    def _link(self, record: dict, notes: set) -> None:
        link_id = f"{record['page']}@{record['emitted_at']}"
        if link_id in self.linked:
            return
        self.io.write(self.topics.triage, record["page_id"],
                      {"type": "link", "link_id": link_id, "page": record["page"], "page_id": record["page_id"],
                       "absorbed": record["absorbed"], "notes": sorted(notes), "at": record["emitted_at"]})
        self.linked.add(link_id)
        self.log(f"link: page {record['page']} absorbed {record['absorbed']}; notes {sorted(notes)}")

    def _investigate(self, ctx: Context) -> None:
        try:
            note = self.investigate(ctx)
        except Exception as e:                      # no note: it is investigated again after a restart
            self.failed += 1
            self.log(f"page {ctx.page} ({ctx.page_id}): investigation failed: {type(e).__name__}: {e}")
            return
        self.io.write(self.topics.triage, ctx.page_id, {"type": "note", **note})
        self.noted.add(ctx.page_id)
        self.investigated += 1
        self.log(f"page {ctx.page} at {ctx.now:%m-%d %H:%M}: {note['verdict']} ({note['confidence']}) "
                 f"{note['tool_calls']} tool calls, {note.get('seconds')} s, ${note.get('cost_usd_estimate')}")

    def wait(self) -> None:
        for f in list(self._futures):
            f.result()
        self._futures = []

    def run(self, stop=lambda: False, until_end: bool = False, poll_seconds: float = 1.0) -> "Dispatcher":
        """Follow `pages`. until_end: handle what is on it now, wait for the investigations, return."""
        try:
            while not stop():
                n = self.step()
                if until_end and n == 0:
                    break
                if n == 0:
                    time.sleep(poll_seconds)
            self.wait()
        finally:
            self._pool.shutdown(wait=True)
        return self


def main(argv=None) -> int:
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker
    from sentinel_stream.investigator import Config, Investigator

    ap = argparse.ArgumentParser(description="Start a triage agent for every page-out; write notes to the triage topic.")
    ap.add_argument("--data", required=True, help="data directory: catalog, topology, logs, Kubernetes events, changes")
    ap.add_argument("--bootstrap", default=broker.DEFAULT_BOOTSTRAP)
    ap.add_argument("--telemetry-topic", default="telemetry")
    ap.add_argument("--alerts-topic", default="alerts")
    ap.add_argument("--pages-topic", default="pages")
    ap.add_argument("--triage-topic", default="triage")
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--model", default=None, help="override the model in sentinel_stream/triage.yaml")
    ap.add_argument("--runs-dir", default=None, help="where transcripts go (default: triage.yaml, runs/)")
    ap.add_argument("--run-name", default=None, help="subfolder of the runs dir (default: the pages topic)")
    ap.add_argument("--until-end", action="store_true", help="handle the page-outs on the topic now, then exit")
    ap.add_argument("--stream-version", type=int, default=1, choices=(1, 2),
                    help="2: logs, Kubernetes events and changes are read from the telemetry topic")
    args = ap.parse_args(argv)

    topics = Topics(args.telemetry_topic, args.alerts_topic, args.pages_topic, args.triage_topic)
    config = Config.load(model=args.model, runs_dir=args.runs_dir)
    agent = Investigator(config, run_name=args.run_name or args.pages_topic)
    stopping = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopping.append(1))
    d = Dispatcher(KafkaTopics(args.bootstrap, topics), DataSource(args.data), agent.investigate, topics, args.concurrency,
                   stream_version=args.stream_version)
    d.log(f"{len(d.noted)} notes and {len(d.linked)} links already on {topics.triage!r}; model {config.model}")
    d.run(stop=lambda: bool(stopping), until_end=args.until_end)
    d.log(f"done: {d.investigated} investigated, {d.failed} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
