#!/usr/bin/env python3
"""A REAL local MCP plugin entrypoint for the LIVE crux smoke.

This is what a `.cyanplugin` bundle ships as its `run` executable: a process that
speaks newline-delimited JSON-RPC over stdio (the contract `cyan_mcp`'s
`StdioTransport`/`Client` drive — see cyan-mcp/src/{transport,client}.rs):

  <- {"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}
  -> {"jsonrpc":"2.0","id":1,"result":{...}}
  <- {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":..,"arguments":..}}
  -> {"jsonrpc":"2.0","id":2,"result":{...}}     # result threads into the step

The tool result carries `cost_usd` — the partner/plugin's OWN billing — so the
host records it on the EXTERNAL cost rail, never against our vLLM token tally.
It is a pure (read-only) tool: no `side_effects`, so it runs without an approval
gate. Deterministic; exits on EOF. No network, no unbounded wait.
"""
import json
import sys


def respond(req_id, result):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\n")
    sys.stdout.flush()


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = msg.get("method")
        req_id = msg.get("id")
        if method == "initialize":
            respond(req_id, {"protocolVersion": "1.0", "serverInfo": {"name": "media-probe"}})
        elif method == "tools/call":
            args = (msg.get("params") or {}).get("arguments") or {}
            src = args.get("src", "asset.mov")
            respond(req_id, {
                "status": "ok",
                "tool": "probe_asset",
                "src": src,
                "codec": "h264",
                "width": 1920,
                "height": 1080,
                # The plugin's own external billing (pass-through $), NOT our tokens.
                "cost_usd": 0.07,
            })
        elif req_id is not None:
            respond(req_id, {})
        # notifications (no id) are ignored


if __name__ == "__main__":
    main()
