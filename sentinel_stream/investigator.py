"""Investigator: one fresh agent per page-out, which writes an advisory triage note.

    note = Investigator(config).investigate(ctx)        # ctx: context_tools.Context of one page-out

The agent is Claude with the eight read-only context tools (context_tools.TOOLS), in a tool-use loop
written out here rather than the SDK's tool runner, because three budgets are checked between every
two steps and the loop has to stop cleanly when one runs out:

    tool calls   at most `max_tool_calls` (15). A call asked for beyond that is answered with an error
                 and no data.
    wall time    `max_seconds` (120) for the whole investigation. Each API request gets what is left as
                 its timeout; `final_reserve_seconds` (25) before the end, tools are switched off and
                 the agent is asked for its note.
    tokens       `max_tokens` (200,000) input + output over all requests. Before each request the next
                 one's size is estimated (the last request's input plus what was added since, plus the
                 largest possible response); if that would pass the cap, tools are switched off and the
                 note is asked for. If even that request would pass it, the investigation stops.

"Tools switched off" is `tool_choice: none` on the next request: the agent must answer in text.

The note is JSON with page_id, verdict (real | noise | unsure), confidence, suspected_cause,
affected_entities, evidence, next_step. It is validated (`validate_note`): the verdict and confidence
in range, every affected entity one that exists in the entity graph, and every piece of evidence citing
a tool call this agent actually made (the agent sees each result labelled with its call id, c1, c2, ...).
An invalid note is sent back once with the reasons, tools off; if the second answer is invalid too, or
a budget ends the run with no valid note, the note says `unsure` with confidence 0 and `valid: false`.

The note is advisory: nothing reads it to change a page, a detector or a threshold.

Every investigation's full transcript (the requests' settings, every message and content block, every
tool call with its result, token usage and timing) is saved as JSON under `runs_dir` (gitignored).

The client is created from the environment: the API key is read by the SDK from ANTHROPIC_API_KEY and
is never passed through, printed or written here. Tests pass a scripted fake instead (`create=`).
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from sentinel_stream import context_tools
from sentinel_stream.context_tools import Context

CONFIG_PATH = Path(__file__).with_name("triage.yaml")
VERDICTS = ("real", "noise", "unsure")
NOTE_FIELDS = ("page_id", "verdict", "confidence", "suspected_cause", "affected_entities", "evidence", "next_step")
FALLBACK_BETA = "server-side-fallback-2026-07-01"

SYSTEM = """You are the on-call triage assistant for a data platform: Kafka, Spark, Airflow, Flink and Trino on Kubernetes.
The alerting system has just paged an engineer. Investigate this ONE page with the read-only tools and write a short triage note that the engineer reads before acting. The note is advice; it does not change the page.

You see everything up to the moment the page went out and nothing later. The page, the metrics and the DAG runs come from the live telemetry stream; logs, Kubernetes events and changes come from their own stores. Every tool result says its source. Every tool result is labelled with a call id (c1, c2, ...).

Verdicts:
- real: the page points to a genuine problem that needs an engineer's attention now: something failing or not running, a sustained degradation, a resource running out.
- noise: nothing an engineer needs to act on: a short spike that has already recovered, a value that is unusual but harmless, normal variation, an expected pattern.
- unsure: the evidence does not settle it, or it contradicts itself. Say unsure rather than guess.

How to work: start with get_page. Look at the paged series and its neighbours over a window long enough to see what is normal for them. A real problem usually shows on more than one signal; look for corroboration and for contradiction. You have at most 15 tool calls and two minutes; most pages need 4 to 8 calls. Do not repeat a call.

When you are done, reply with ONLY a JSON object (no prose before or after it, no code fence):
{"page_id": "<the page_id>",
 "verdict": "real" | "noise" | "unsure",
 "confidence": <a number from 0 to 1>,
 "suspected_cause": "<one or two sentences: what is going on and why you think so>",
 "affected_entities": ["<kind:name as written in the tool results>", ...],
 "evidence": [{"tool_call_id": "<a call id such as c3>", "finding": "<what that result shows>"}, ...],
 "next_step": "<the first thing the engineer should check or do>"}
For real or noise, cite at least one tool call as evidence."""


# ------------------------------------------------------------------------------------------ config
@dataclass
class Budget:
    max_tool_calls: int = 15
    max_seconds: float = 120.0
    max_tokens: int = 200_000
    final_reserve_seconds: float = 25.0


@dataclass
class Config:
    model: str = "claude-sonnet-5-5"
    effort: str | None = "medium"
    max_tokens_per_call: int = 8000
    fallbacks: str | None = "default"
    budget: Budget = field(default_factory=Budget)
    runs_dir: str = "runs"
    prices_per_mtok: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path=CONFIG_PATH, **overrides) -> "Config":
        raw = yaml.safe_load(Path(path).read_text()) if path else {}
        raw.update({k: v for k, v in overrides.items() if v is not None})
        budget = Budget(**raw.pop("budget", {}))
        return cls(budget=budget, **raw)

    def public(self) -> dict:
        """The settings as saved in a transcript (there is no secret in them)."""
        return {"model": self.model, "effort": self.effort, "max_tokens_per_call": self.max_tokens_per_call,
                "fallbacks": self.fallbacks, "budget": vars(self.budget)}


def anthropic_create(config: Config):
    """The real `create`: the SDK's beta Messages endpoint (beta for the server-side fallback).
    Retries are left to the loop, so that a retry never runs past the deadline."""
    import anthropic

    client = anthropic.Anthropic(max_retries=0)
    return client.beta.messages.create if config.fallbacks else client.messages.create


def _is_timeout(exc: Exception) -> bool:
    try:
        import anthropic
    except ImportError:
        return getattr(exc, "is_timeout", False)
    return isinstance(exc, anthropic.APITimeoutError) or getattr(exc, "is_timeout", False)


def _retryable(exc: Exception) -> bool:
    try:
        import anthropic
    except ImportError:                       # a fake client in the tests
        return getattr(exc, "retryable", False)
    return isinstance(exc, (anthropic.RateLimitError, anthropic.InternalServerError, anthropic.APIConnectionError)) \
        or getattr(exc, "retryable", False)


# ---------------------------------------------------------------------------------------- the note
def parse_note(text: str) -> tuple:
    """(dict, None) or (None, why it is not a JSON object)."""
    s = text.strip()
    fence = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", s, re.S)
    if fence:
        s = fence.group(1)
    if not s.startswith("{"):
        a, b = s.find("{"), s.rfind("}")
        if a < 0 or b < a:
            return None, "the answer holds no JSON object"
        s = s[a:b + 1]
    try:
        obj = json.loads(s)
    except json.JSONDecodeError as e:
        return None, f"not valid JSON: {e}"
    return (obj, None) if isinstance(obj, dict) else (None, "the JSON is not an object")


def validate_note(note: dict, page_id: str, call_ids: set, entities) -> list:
    """Every reason the note is not acceptable (empty: it is)."""
    errors = []
    missing = [k for k in NOTE_FIELDS if k not in note]
    extra = [k for k in note if k not in NOTE_FIELDS]
    if missing:
        errors.append(f"missing fields: {missing}")
    if extra:
        errors.append(f"unknown fields: {extra}")
    if "page_id" in note and note["page_id"] != page_id:
        errors.append(f"page_id must be {page_id!r}")
    if "verdict" in note and note["verdict"] not in VERDICTS:
        errors.append(f"verdict must be one of {list(VERDICTS)}")
    c = note.get("confidence")
    if "confidence" in note and not (isinstance(c, (int, float)) and not isinstance(c, bool) and 0 <= c <= 1):
        errors.append("confidence must be a number from 0 to 1")
    for k in ("suspected_cause", "next_step"):
        if k in note and not (isinstance(note[k], str) and note[k].strip()):
            errors.append(f"{k} must be a non-empty string")
    ents = note.get("affected_entities")
    if "affected_entities" in note:
        if not isinstance(ents, list) or not all(isinstance(e, str) for e in ents):
            errors.append("affected_entities must be a list of strings")
        else:
            unknown = [e for e in ents if e not in entities]
            if unknown:
                errors.append(f"affected_entities not in the entity graph (write them as kind:name): {unknown}")
    ev = note.get("evidence")
    if "evidence" in note:
        if not isinstance(ev, list) or not all(isinstance(e, dict) and set(e) == {"tool_call_id", "finding"}
                                               and isinstance(e["finding"], str) for e in ev):
            errors.append('evidence must be a list of {"tool_call_id", "finding"}')
        else:
            bad = [e["tool_call_id"] for e in ev if e["tool_call_id"] not in call_ids]
            if bad:
                errors.append(f"evidence cites tool calls that were not made: {bad}; made: {sorted(call_ids)}")
            if not ev and note.get("verdict") in ("real", "noise"):
                errors.append("a real or noise verdict needs at least one piece of evidence")
    return errors


def _dump(block) -> dict:
    if hasattr(block, "model_dump"):
        return block.model_dump(mode="json", exclude_none=True)
    if isinstance(block, dict):
        return block
    return dict(vars(block))


def _field(block, name):
    return block.get(name) if isinstance(block, dict) else getattr(block, name, None)


# ------------------------------------------------------------------------------------ the agent loop
class Investigator:
    def __init__(self, config: Config | None = None, create=None, clock=time.monotonic, run_name: str = "adhoc",
                 sleep=time.sleep):
        self.config = config or Config.load()
        self._create = create
        self.clock, self.sleep = clock, sleep
        self.run_name = run_name

    @property
    def create(self):
        if self._create is None:
            self._create = anthropic_create(self.config)
        return self._create

    def investigate(self, ctx: Context) -> dict:
        cfg, budget = self.config, self.config.budget
        began = self.clock()
        deadline = began + budget.max_seconds
        tools = context_tools.definitions()
        messages = [{"role": "user", "content": f"Page {ctx.page} (page_id {ctx.page_id}) paged at "
                                                f"{ctx.now:%Y-%m-%d %H:%M} UTC. Investigate it."}]
        state = {"calls": [], "api": [], "tokens": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0},
                 "cost": 0.0, "priced": True, "models": [], "exhausted": None, "attempts": 0, "errors": [], "stop": None}
        final = False                   # tools switched off: the next answer must be the note
        added_since = 0                 # characters appended since the last request (tool results, retry text)
        last_input = 0
        note = None

        def tools_off(reason: str) -> None:
            """Switch tools off for a budget and say so in the agent's next turn (with tool_choice none
            alone, the model may simply end its turn without writing anything)."""
            nonlocal final, added_since
            final, state["exhausted"] = True, state["exhausted"] or reason
            text = {"type": "text", "text": f"The {reason.replace('_', ' ')} budget is used up and tools are now off. "
                                            "Write the note now: the JSON object only."}
            if messages[-1]["role"] == "user" and isinstance(messages[-1]["content"], list):
                messages[-1]["content"].append(text)
            else:
                messages.append({"role": "user", "content": [text]})
            added_since += len(text["text"])

        while True:
            if not final and len(state["calls"]) >= budget.max_tool_calls:
                tools_off("tool_calls")
            left = deadline - self.clock()
            if not final and left <= budget.final_reserve_seconds:
                tools_off("time")
            projected = sum(state["tokens"].values()) + last_input + added_since // 3 + cfg.max_tokens_per_call
            if projected > budget.max_tokens:
                if final:
                    state["exhausted"], state["stop"] = state["exhausted"] or "tokens", "token cap reached before the note"
                    break
                tools_off("tokens")
                continue
            if left <= 1:
                state["exhausted"], state["stop"] = state["exhausted"] or "time", "deadline reached before the note"
                break

            response, err = self._request(messages, tools, final, deadline, state)
            if response is None and err == "timeout" and not final:
                tools_off("time")                   # the reserve is still there: spend it on the note
                continue
            if response is None:
                state["stop"] = err
                break
            usage = _field(response, "usage")
            last_input = sum(int(getattr(usage, k, 0) or 0) for k in ("input_tokens", "cache_read_input_tokens",
                                                                      "cache_creation_input_tokens"))
            added_since = 0
            content = list(_field(response, "content") or [])
            messages.append({"role": "assistant", "content": content})
            stop_reason = _field(response, "stop_reason")
            if stop_reason == "refusal":
                state["stop"] = "the model declined the request (stop_reason refusal)"
                break
            uses = [b for b in content if _field(b, "type") == "tool_use"]
            if uses:
                results = []
                for b in uses:
                    if len(state["calls"]) >= budget.max_tool_calls:
                        text, is_error = context_tools.dumps({"error": "tool-call budget used up: write the note now"}), True
                        state["exhausted"] = state["exhausted"] or "tool_calls"
                    else:
                        call_id = f"c{len(state['calls']) + 1}"
                        name, args = _field(b, "name"), _field(b, "input") or {}
                        t0 = self.clock()
                        try:
                            result, is_error = context_tools.call(ctx, name, args)
                        except Exception as e:      # a bug in a tool must not end the investigation
                            result, is_error = {"error": f"the tool failed: {type(e).__name__}: {e}"}, True
                        text = context_tools.dumps({"call_id": call_id, **result})
                        state["calls"].append({"call_id": call_id, "tool": name, "input": args, "is_error": is_error,
                                               "result": result, "seconds": round(self.clock() - t0, 3)})
                    results.append({"type": "tool_result", "tool_use_id": _field(b, "id"), "content": text,
                                    **({"is_error": True} if is_error else {})})
                    added_since += len(text)
                messages.append({"role": "user", "content": results})
                continue

            text = "".join(_field(b, "text") or "" for b in content if _field(b, "type") == "text")
            state["attempts"] += 1
            obj, why = parse_note(text)
            errors = [why] if why else validate_note(obj, ctx.page_id, {c["call_id"] for c in state["calls"]},
                                                     ctx.catalog.entities)
            if stop_reason == "max_tokens":
                errors = ["the answer was cut off at the response length limit"] + errors
            if not errors:
                note = obj
                break
            state["errors"].append({"attempt": state["attempts"], "errors": errors, "text": text[:4000]})
            if state["attempts"] >= 2:
                state["stop"] = "no valid note after one retry"
                break
            retry = ("Your note is not valid: " + "; ".join(errors) + ". Reply with the corrected JSON object only.")
            messages.append({"role": "user", "content": retry})
            added_since += len(retry)
            final = True

        return self._finish(ctx, note, state, messages, began)

    def _request(self, messages, tools, final, deadline, state) -> tuple:
        """One API request, retried once on a rate limit, an overload or a connection error if time
        allows. (response, None) or (None, why); why is "timeout" if the request ran out of time.

        Timeout: a request with tools on may use the time left minus the reserve, so it can never eat
        the time kept for the note; the request for the note may use all that is left."""
        cfg = self.config
        kwargs = {"model": cfg.model, "max_tokens": cfg.max_tokens_per_call, "system": SYSTEM, "tools": tools,
                  "messages": messages, "tool_choice": {"type": "none" if final else "auto"},
                  "cache_control": {"type": "ephemeral"}}
        if cfg.effort:
            kwargs["output_config"] = {"effort": cfg.effort}
        if cfg.fallbacks:
            kwargs.update(betas=[FALLBACK_BETA], fallbacks=cfg.fallbacks)
        reserve = 0.0 if final else cfg.budget.final_reserve_seconds
        for attempt in (1, 2):
            left = deadline - self.clock() - reserve
            if left <= 1:
                return None, "timeout" if not final else "deadline reached during a request"
            t0 = self.clock()
            try:
                response = self.create(**kwargs, timeout=left)
            except Exception as e:
                state["api"].append({"ok": False, "error": f"{type(e).__name__}: {e}"[:500], "seconds": round(self.clock() - t0, 2)})
                if _is_timeout(e):
                    return None, "timeout" if not final else "the request for the note timed out at the deadline"
                if attempt == 1 and _retryable(e) and deadline - self.clock() - reserve > 10:
                    self.sleep(min(2.0, max(0.0, deadline - self.clock() - 9)))
                    continue
                return None, f"API request failed: {type(e).__name__}"
            self._account(response, state, self.clock() - t0, final)
            return response, None
        return None, "API request failed twice"

    def _account(self, response, state, seconds, final) -> None:
        usage = _field(response, "usage")
        got = {"input": int(getattr(usage, "input_tokens", 0) or 0), "output": int(getattr(usage, "output_tokens", 0) or 0),
               "cache_read": int(getattr(usage, "cache_read_input_tokens", 0) or 0),
               "cache_write": int(getattr(usage, "cache_creation_input_tokens", 0) or 0)}
        for k, v in got.items():
            state["tokens"][k] += v
        model = _field(response, "model") or self.config.model
        if model not in state["models"]:
            state["models"].append(model)
        price = self.config.prices_per_mtok.get(model)
        if price is None:
            state["priced"] = False
        else:
            state["cost"] += sum(got[k] * price[k] for k in got) / 1e6
        state["api"].append({"ok": True, "model": model, "stop_reason": _field(response, "stop_reason"), "tools_off": final,
                             "usage": got, "seconds": round(seconds, 2), "request_id": getattr(response, "_request_id", None)})

    def _finish(self, ctx: Context, note, state, messages, began) -> dict:
        calls = {c["call_id"]: c["tool"] for c in state["calls"]}
        valid = note is not None
        if not valid:
            note = {"page_id": ctx.page_id, "verdict": "unsure", "confidence": 0.0,
                    "suspected_cause": f"(no valid note: {state['stop']})", "affected_entities": [], "evidence": [],
                    "next_step": "Investigate the page by hand."}
        note = {k: note[k] for k in NOTE_FIELDS}
        note["evidence"] = [{**e, "tool": calls.get(e["tool_call_id"])} for e in note["evidence"]]
        out = {**note, "page": ctx.page, "paged_at": ctx.now.isoformat(), "advisory": True, "valid": valid,
               "model": self.config.model, "models_served": state["models"], "tool_calls": len(state["calls"]),
               "tools_used": [c["tool"] for c in state["calls"]], "api_requests": len(state["api"]),
               "tokens": {**state["tokens"], "total": sum(state["tokens"].values())},
               "cost_usd_estimate": round(state["cost"], 5) if state["priced"] else None,
               "seconds": round(self.clock() - began, 2), "attempts": state["attempts"],
               "budget_exhausted": state["exhausted"], "stop": state["stop"],
               "invalid_answers": [e["errors"] for e in state["errors"]]}
        out["transcript"] = self._save(ctx, out, state, messages)
        return out

    def _save(self, ctx: Context, note: dict, state, messages) -> str | None:
        if not self.config.runs_dir:
            return None
        path = Path(self.config.runs_dir) / self.run_name / f"page-{ctx.page:04d}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        transcript = {"config": self.config.public(), "page": ctx.page, "page_id": ctx.page_id, "now": ctx.now.isoformat(),
                      "system": SYSTEM, "tools": [t["name"] for t in context_tools.definitions()],
                      "messages": [{"role": m["role"], "content": m["content"] if isinstance(m["content"], str)
                                    else [_dump(b) for b in m["content"]]} for m in messages],
                      "tool_calls": state["calls"], "api_requests": state["api"], "invalid_answers": state["errors"],
                      "note": note}
        path.write_text(json.dumps(transcript, indent=1, default=str))
        return str(path)
