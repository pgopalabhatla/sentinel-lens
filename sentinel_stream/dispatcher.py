"""Dispatcher: one triage agent per page-out, notes on the `triage` topic.

    python -m sentinel_stream.dispatcher --data data_live          # follows `pages` until stopped

It reads `pages` from the beginning, in order. For every `paged` record (a page-out) it starts an
investigator (investigator.py) on that page AS IT WAS AT THAT MOMENT (context_tools.Context.at_page:
the page records up to that one, the readings stamped up to then) and writes the note it returns to
the topic `triage`, keyed by the page's id. At most `concurrency` (3) investigations run at once.

What goes on `triage`:
    {"type": "note", "page_id", "page", "needs_human", ... }    one per page-out (investigator.py has the fields)
    {"type": "follow_up", "follow_up_of": page_id, ... }        a second note, see "Follow-up" below
    {"type": "link", "link_id", "page", "page_id", "absorbed", "notes": [page_id, ...], "at"}
            a page that had paged (or several) merged with another page later. The notes of every page
            involved are linked under the merged page; nothing is investigated again.

Follow-up. With `follow_up_minutes` set, a note whose needs_human is "unsure" gets ONE follow-up: a
fresh agent on the same page as it is at paging time + follow_up_minutes (Context.at_page(at=...): the
tools see nothing after that time; a page merged in the meantime is followed into the page that holds
it). It is started once the stream has passed that time (telemetry stamped at least `follow_up_grace`
later, so the consumer has had time to write that minute's pages), or, at the end of a finished stream,
if the time is inside it. Its note is written as a `follow_up` record linked by page id. A follow-up
never gets a follow-up.

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

import pandas as pd

from sentinel_stream import wire
from sentinel_stream.context_tools import Catalog, Context, StreamStore
from sentinel_stream.events import utc


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
    follow_up_minutes: float | None = None    # re-investigate an "unsure" note this long after paging; None: never
    follow_up_grace: pd.Timedelta = pd.Timedelta("1min")

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
        self.followed: set = set()       # page ids with a follow-up on `triage`
        self._unsure: dict = {}          # page id -> its first note, if that said "unsure" (from `triage`)
        self._follow: list = []          # (due time, index into store.pages, first note) not started yet
        self._lock = threading.Lock()
        self._clock = None               # the latest event time read from `telemetry`
        self._ended = False              # the stream's closing watermark has been read
        self.followed_up = self.follow_ups_past_end = 0
        for r in self.io.read(self.topics.triage):
            if r.get("type") == "note":
                self.noted.add(r["page_id"])
                if r.get("needs_human") == "unsure":
                    self._unsure[r["page_id"]] = r
            elif r.get("type") == "follow_up":
                self.followed.add(r["follow_up_of"])
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
            t = wire.event_time(m)
            self._clock = t if self._clock is None else max(self._clock, t)
            self._ended = self._ended or m["type"] == wire.WATERMARK
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
                    self._futures.append(self._pool.submit(self._investigate, ctx, index))
                elif record["page_id"] in self._unsure and record["page_id"] not in self.followed:
                    self._schedule(index, self._unsure.pop(record["page_id"]))     # restarted before its follow-up
            elif record["kind"] == "merged":
                involved = set(self._notes_of.pop(key, set()))
                for k in record["absorbed"]:
                    involved |= self._notes_of.pop(k, set())
                if involved:
                    self._notes_of[key] = involved
                    self._link(record, involved)
        self._release(final=False)
        self._futures = [f for f in self._futures if not f.done()]
        return len(new)

    # ---------------------------------------------------------------------------------- follow-ups
    def _schedule(self, index: int, note: dict) -> None:
        if not self.follow_up_minutes:
            return
        due = utc(self.store.pages[index]["emitted_at"]) + pd.Timedelta(minutes=float(self.follow_up_minutes))
        with self._lock:
            self._follow.append((due, index, note))

    def _release(self, final: bool) -> None:
        """Start the follow-ups whose time the stream has passed. final: the topics are complete, so
        a follow-up whose time the stream never reaches is dropped."""
        if not self._follow:
            return
        self._catch_up()
        with self._lock:
            waiting, self._follow = self._follow, []
        for due, index, note in sorted(waiting, key=lambda x: x[0]):
            done = self._ended or final
            if self._clock is not None and (self._clock >= due + self.follow_up_grace or (done and self._clock >= due)):
                ctx = Context.at_page(self.store, self.ds, index, self.catalog, self.stream_version, at=due)
                self._futures.append(self._pool.submit(self._follow_up, ctx, note))
            elif done:
                self.follow_ups_past_end += 1
                self.log(f"page {note.get('page')} ({note['page_id']}): no follow-up, the stream ends before {due:%m-%d %H:%M}")
            else:
                with self._lock:
                    self._follow.append((due, index, note))

    def _follow_up(self, ctx: Context, first: dict) -> None:
        try:
            note = self.investigate(ctx, previous=first)
        except Exception as e:
            self.failed += 1
            self.log(f"page {ctx.page_id}: follow-up failed: {type(e).__name__}: {e}")
            return
        self.io.write(self.topics.triage, ctx.page_id, {"type": "follow_up", "follow_up_of": ctx.page_id, **note})
        self.followed.add(ctx.page_id)
        self.followed_up += 1
        self.log(f"follow-up of {ctx.page_id} at {ctx.now:%m-%d %H:%M}: {_answer(note)} ({note.get('confidence')}) "
                 f"{note.get('tool_calls')} tool calls, ${note.get('cost_usd_estimate')}")

    def _link(self, record: dict, notes: set) -> None:
        link_id = f"{record['page']}@{record['emitted_at']}"
        if link_id in self.linked:
            return
        self.io.write(self.topics.triage, record["page_id"],
                      {"type": "link", "link_id": link_id, "page": record["page"], "page_id": record["page_id"],
                       "absorbed": record["absorbed"], "notes": sorted(notes), "at": record["emitted_at"]})
        self.linked.add(link_id)
        self.log(f"link: page {record['page']} absorbed {record['absorbed']}; notes {sorted(notes)}")

    def _investigate(self, ctx: Context, index: int) -> None:
        try:
            note = self.investigate(ctx)
        except Exception as e:                      # no note: it is investigated again after a restart
            self.failed += 1
            self.log(f"page {ctx.page} ({ctx.page_id}): investigation failed: {type(e).__name__}: {e}")
            return
        self.io.write(self.topics.triage, ctx.page_id, {"type": "note", **note})
        self.noted.add(ctx.page_id)
        self.investigated += 1
        if note.get("needs_human") == "unsure" and ctx.page_id not in self.followed:
            self._schedule(index, note)
        self.log(f"page {ctx.page} at {ctx.now:%m-%d %H:%M}: {_answer(note)} ({note['confidence']}) "
                 f"{note['tool_calls']} tool calls, {note.get('seconds')} s, ${note.get('cost_usd_estimate')}")

    def wait(self) -> None:
        while self._futures:
            futures, self._futures = list(self._futures), []
            for f in futures:
                f.result()

    def run(self, stop=lambda: False, until_end: bool = False, poll_seconds: float = 1.0) -> "Dispatcher":
        """Follow `pages`. until_end: handle what is on the topics now (follow-ups included), wait for
        the investigations, return."""
        try:
            while not stop():
                n = self.step()
                if until_end and n == 0:
                    break
                if n == 0:
                    time.sleep(poll_seconds)
            self.wait()
            if until_end:
                while self._follow:              # first notes are in: their follow-ups, on the finished topics
                    self._release(final=True)
                    self.wait()
        finally:
            self._pool.shutdown(wait=True)
        return self


def _answer(note: dict) -> str:
    return str(note.get("needs_human", note.get("verdict")))


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
    ap.add_argument("--follow-up-minutes", type=float, default=None,
                    help="re-investigate an 'unsure' page this long after it paged (default: triage.yaml); 0: never")
    args = ap.parse_args(argv)

    topics = Topics(args.telemetry_topic, args.alerts_topic, args.pages_topic, args.triage_topic)
    config = Config.load(model=args.model, runs_dir=args.runs_dir)
    agent = Investigator(config, run_name=args.run_name or args.pages_topic)
    stopping = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopping.append(1))
    follow = config.follow_up_minutes if args.follow_up_minutes is None else args.follow_up_minutes
    d = Dispatcher(KafkaTopics(args.bootstrap, topics), DataSource(args.data), agent.investigate, topics, args.concurrency,
                   stream_version=args.stream_version, follow_up_minutes=follow or None)
    d.log(f"{len(d.noted)} notes and {len(d.linked)} links already on {topics.triage!r}; model {config.model}")
    d.run(stop=lambda: bool(stopping), until_end=args.until_end)
    d.log(f"done: {d.investigated} investigated, {d.failed} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
