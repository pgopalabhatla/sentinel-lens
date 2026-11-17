"""The context tools as an MCP server, for one page.

    python -m sentinel_stream.mcp_server --data data_live --page 2 \\
        --telemetry-topic live-week-1-telemetry --pages-topic live-week-1-pages --alerts-topic live-week-1-alerts

It serves over stdio the same nine tools the investigator uses, with the same names, descriptions
and JSON schemas: both take them from `context_tools.TOOLS`, and both run them through
`context_tools.call`. So an MCP client (Claude Code, Claude Desktop, an MCP inspector) can look at a
page exactly the way the triage agent does.

One server is one page, as it was when it paged: `--page` is the page's number on the `pages` topic
(its first page-out is used; a page pages at most once). The server reads the three topics once at
start-up and then answers from memory; it is read-only.

Built on the low-level `Server` of the official `mcp` SDK rather than its decorator API, because the
decorator API derives each schema from a Python signature, and the point here is that the schemas are
the very same dicts the investigator sends.
"""
from __future__ import annotations

import argparse

import mcp_types as types
from mcp.server import Server

from sentinel_stream import context_tools

NAME = "sentinel-triage-context"


def build_server(ctx: context_tools.Context) -> Server:
    async def list_tools(_request, _params) -> types.ListToolsResult:
        return types.ListToolsResult(tools=[types.Tool(name=d["name"], description=d["description"],
                                                       input_schema=d["input_schema"])
                                            for d in context_tools.definitions()])

    async def call_tool(_request, params) -> types.CallToolResult:
        result, is_error = context_tools.call(ctx, params.name, params.arguments or {})
        return types.CallToolResult(content=[types.TextContent(type="text", text=context_tools.dumps(result))],
                                    is_error=is_error)

    return Server(NAME, instructions=f"Read-only context for page {ctx.page} ({ctx.page_id}), as it was when it "
                                     f"paged at {ctx.now:%Y-%m-%d %H:%M} UTC. Nothing after that moment is visible.",
                  on_list_tools=list_tools, on_call_tool=call_tool)


def context_for_page(data: str, bootstrap: str, telemetry: str, pages: str, alerts: str, page: int,
                     stream_version: int = 1) -> context_tools.Context:
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    store = context_tools.StreamStore.from_records(broker.read_topic(bootstrap, telemetry), broker.read_topic(bootstrap, pages),
                                                   broker.read_topic(bootstrap, alerts))
    index = next((i for i in store.paged() if store.pages[i]["page"] == page), None)
    if index is None:
        raise SystemExit(f"page {page} has not paged on {pages!r} (page-outs: {sorted({store.pages[i]['page'] for i in store.paged()})})")
    return context_tools.Context.at_page(store, DataSource(data), index, stream_version=stream_version)


def main(argv=None) -> int:
    import anyio
    from mcp.server.stdio import stdio_server

    ap = argparse.ArgumentParser(description="Serve the triage context tools for one page over MCP (stdio).")
    ap.add_argument("--data", required=True, help="data directory: catalog, topology, logs, Kubernetes events, changes")
    ap.add_argument("--page", required=True, type=int, help="the page's number on the pages topic")
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--telemetry-topic", default="telemetry")
    ap.add_argument("--pages-topic", default="pages")
    ap.add_argument("--alerts-topic", default="alerts")
    ap.add_argument("--stream-version", type=int, default=1, choices=(1, 2),
                    help="2: logs, Kubernetes events and changes are read from the telemetry topic")
    args = ap.parse_args(argv)
    from sentinel_stream import broker
    ctx = context_for_page(args.data, args.bootstrap or broker.DEFAULT_BOOTSTRAP, args.telemetry_topic, args.pages_topic,
                           args.alerts_topic, args.page, args.stream_version)
    server = build_server(ctx)

    async def serve():
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())

    anyio.run(serve)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
