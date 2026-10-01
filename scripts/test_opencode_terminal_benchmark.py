"""Offline contract and loopback credential-boundary tests for the benchmark."""

import asyncio
import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace


spec = importlib.util.spec_from_file_location("benchmark", Path(__file__).with_name("opencode-terminal-benchmark.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class ContractTests(unittest.TestCase):
    def test_origin_rejects_remote_and_credentials(self):
        for url in ["https://127.0.0.1", "http://example.com", "http://localhost", "http://user@127.0.0.1",
                    "http://127.0.0.1/path", "http://127.0.0.1?x=1"]:
            with self.subTest(url=url), self.assertRaises(argparse.ArgumentTypeError):
                benchmark.local_origin(url)

    def test_echo_cannot_fake_diagnostics(self):
        tool = {"state": {"input": {"command": "printf 'free -b /proc/meminfo ps -eo'"}, "output": "fake"}}
        with self.assertRaisesRegex(RuntimeError, "unexpected"):
            benchmark.diagnostic_results([tool], benchmark.DIAGNOSTICS)

    def test_successful_exit_without_memory_output_does_not_pass(self):
        tool = {"state": {"input": {"command": "free -b"}, "output": "no memory evidence"}}
        with self.assertRaisesRegex(RuntimeError, "total/available"):
            benchmark.diagnostic_results([tool], ["free -b"])

    def test_other_tool_cannot_pass_even_with_bash(self):
        events = [{"sessionID": "ses_fixture", "type": "tool_use", "part": {
            "tool": "write", "state": {"status": "completed"}}}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("\n".join(map(json.dumps, events)))
            with self.assertRaisesRegex(RuntimeError, "unexpected tool"):
                benchmark.evidence(path)

    def test_process_command_name_can_contain_spaces(self):
        command = "ps -eo pid,rss,comm --sort=-rss | head -11"
        tool = {"state": {"input": {"command": command}, "output": "PID RSS COMMAND\n123 456 Web Content\n"}}
        result = benchmark.diagnostic_results([tool], [command])
        self.assertEqual(result[command]["processes"], 1)

    def test_http_redirect_does_not_forward_cookie(self):
        hits = []

        class Target(BaseHTTPRequestHandler):
            def do_GET(self):
                hits.append(self.headers.get("Cookie"))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Target) as target:
            thread = threading.Thread(target=target.serve_forever, daemon=True)
            thread.start()

            class Redirect(Target):
                def do_GET(self):
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{target.server_port}/target")
                    self.end_headers()

            with ThreadingHTTPServer(("127.0.0.1", 0), Redirect) as redirect:
                thread2 = threading.Thread(target=redirect.serve_forever, daemon=True)
                thread2.start()
                neige = benchmark.Neige(SimpleNamespace(url=f"http://127.0.0.1:{redirect.server_port}", cookie_file=None))
                neige.cookie = "benchmark-test-cookie"
                try:
                    with self.assertRaisesRegex(RuntimeError, "redirect"):
                        neige.api("GET", "/probe")
                    self.assertEqual(hits, [])
                finally:
                    redirect.shutdown()
                    thread2.join()
            target.shutdown()
            thread.join()


class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_ws_redirect_does_not_forward_cookie(self):
        hits = []

        async def target(reader, writer):
            hits.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(target, "127.0.0.1", 0) as target_server:
            target_port = target_server.sockets[0].getsockname()[1]

            async def redirect(reader, writer):
                await reader.readuntil(b"\r\n\r\n")
                writer.write((f"HTTP/1.1 302 Found\r\nLocation: ws://127.0.0.1:{target_port}/target\r\n"
                              "Content-Length: 0\r\n\r\n").encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            async with await asyncio.start_server(redirect, "127.0.0.1", 0) as redirect_server:
                port = redirect_server.sockets[0].getsockname()[1]
                with self.assertRaises(Exception):
                    await benchmark.LocalWebSocket(f"ws://127.0.0.1:{port}/probe", proxy=None,
                                                    additional_headers={"Cookie": "benchmark-test-cookie"})
                self.assertEqual(hits, [], "a redirect must never receive the authentication cookie")


if __name__ == "__main__":
    unittest.main()
