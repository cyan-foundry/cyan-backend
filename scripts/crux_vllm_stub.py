#!/usr/bin/env python3
"""Compile-aware vLLM stub for the LIVE crux smoke (CRUX_REAL=1).

The lens e2e `vllm_stub.py` only answers the enrichment/query prompts (it returns
a canned extraction object). The backend pipeline COMPILE path
(`pipeline.rs::compile_via_llm`) instead expects the model to return a JSON
**array** of pipeline step configs. Those two shapes disagree — a FOUND GAP the
fakes hid. This stub speaks the OpenAI `/v1/chat/completions` shape (same wire as
the real lens vLLM) and returns a step-config array for compile prompts, so the
crux exercises a real backend->HTTP->vLLM compile round-trip.

Usage:  python3 crux_vllm_stub.py [PORT]   (default 8001)

It always reports token usage so the obs/cost contract sees non-zero OUR-vLLM
tokens (kept separate from the external plugin cost rail).
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

# A compile prompt asks for "pipeline step configs"; return ONE manual step so
# `run_pipeline` skips executing it (no flaky network) while still proving the
# compile wire applied a real, parsed config to the cell.
COMPILE_CONFIGS = [
    {
        "step_id": "analyze_notes",
        "depends_on": [],
        "executor": "manual",
        "command": None,
        "timeout_seconds": 60,
    }
]

# Canned enrichment/query extraction (mirrors the lens stub) for any other prompt.
EXTRACTION = {
    "asks": [],
    "decisions": [],
    "references": [],
    "mentions": [],
    "topics": ["compile"],
    "summary": "crux compile stub",
    "urgency": 0.0,
    "sentiment": 0.0,
    "is_blocker": False,
    "needs_followup": False,
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length else "{}"
        is_compile = "pipeline step configs" in body
        payload = COMPILE_CONFIGS if is_compile else EXTRACTION
        content = json.dumps(payload)
        resp = {
            "choices": [{"message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": 40, "completion_tokens": 30, "total_tokens": 70},
        }
        data = json.dumps(resp).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
