"""Network fault filters over actual upstream SSE, never manufactured responses."""

import argparse
import json
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx


class EmptyOutputTransport(httpx.AsyncBaseTransport):
    def __init__(self, injected):
        self.upstream = httpx.AsyncHTTPTransport()
        self.injected = injected

    async def handle_async_request(self, request):
        response = await self.upstream.handle_async_request(request)
        if response.status_code != 200:
            return response
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            stream=EmptyOutputStream(response.stream, self.injected),
        )

    async def aclose(self):
        await self.upstream.aclose()


class EmptyOutputStream(httpx.AsyncByteStream):
    def __init__(self, upstream, injected):
        self.upstream, self.injected = upstream, injected

    async def __aiter__(self):
        buffer = b""
        try:
            async for chunk in self.upstream:
                buffer += chunk
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    lines = []
                    for line in frame.splitlines():
                        if line.startswith(b"data:") and line[5:].strip() != b"[DONE]":
                            value = json.loads(line[5:].strip())
                            for choice in value.get("choices", []):
                                delta = choice.get("delta") or {}
                                for field in ("content", "reasoning_content", "reasoning"):
                                    if delta.pop(field, None):
                                        self.injected.append("empty")
                            line = b"data: " + json.dumps(value).encode()
                        lines.append(line)
                    yield b"\n".join(lines) + b"\n\n"
            if buffer:
                yield buffer
        finally:
            await self.upstream.aclose()

    async def aclose(self):
        await self.upstream.aclose()


def serve_proxy(port, upstream, mode, evidence):
    """Forward real Qwen traffic; alter only the selected network fault boundary."""

    def record(value):
        with Path(evidence).open("a") as stream:
            stream.write(json.dumps(value) + "\n")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"OK")

        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 16_777_216:
                self.send_error(413)
                return
            body = json.loads(self.rfile.read(size))
            if mode == "truncated":
                body["max_tokens"] = 1
            headers = {"Content-Type": "application/json"}
            if "Authorization" in self.headers:
                headers["Authorization"] = self.headers["Authorization"]

            def emit(frame):
                data = frame.encode() + b"\n\n"
                self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
                self.wfile.flush()

            with httpx.Client(timeout=120, trust_env=False) as client:
                with client.stream(
                    "POST", upstream.rstrip("/") + "/chat/completions", json=body, headers=headers
                ) as response:
                    record(
                        {"mode": mode, "stage": "upstream_headers", "status": response.status_code}
                    )
                    if response.status_code != 200:
                        raw = response.read()
                        self.send_response(response.status_code)
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    frame = []
                    for line in response.iter_lines():
                        if line:
                            frame.append(line)
                            continue
                        if not frame:
                            continue
                        lines, frame = frame, []
                        content = False
                        for index, item in enumerate(lines):
                            if not item.startswith("data:") or item[5:].strip() == "[DONE]":
                                continue
                            value = json.loads(item[5:].strip())
                            for choice in value.get("choices", []):
                                delta = choice.get("delta") or {}
                                content |= bool(delta.get("content"))
                                if choice.get("finish_reason") is not None:
                                    record(
                                        {
                                            "mode": mode,
                                            "stage": "upstream_finish",
                                            "reason": choice["finish_reason"],
                                        }
                                    )
                                if mode == "empty":
                                    for field in ("content", "reasoning_content", "reasoning"):
                                        delta.pop(field, None)
                            if mode == "empty":
                                lines[index] = "data: " + json.dumps(value)
                        emit("\n".join(lines))
                        if content and mode in {"disconnect", "timeout"}:
                            record(
                                {
                                    "mode": mode,
                                    "stage": "after_real_content",
                                    "effect": "close_tcp" if mode == "disconnect" else "stall_tcp",
                                }
                            )
                            if mode == "timeout":
                                time.sleep(65)
                            self.connection.shutdown(socket.SHUT_RDWR)
                            self.connection.close()
                            return
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                    record({"mode": mode, "stage": "forward_complete"})

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument(
        "--mode", choices=("empty", "truncated", "disconnect", "timeout"), required=True
    )
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    serve_proxy(args.port, args.upstream, args.mode, args.evidence)
