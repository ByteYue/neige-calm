#!/usr/bin/env python3
"""Run the OpenCode memory/resume acceptance through a local Neige terminal.

Requires Python 3.11+ and websockets 15. Run on the Neige host; artifacts and
the selected OpenCode account stay on that host. Commands are sent once only.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


THEME = {"fg": [216, 219, 226], "bg": [15, 20, 24]}
# Matches calm-session's versioned terminal wire contract.
TERMINAL_PROTOCOL_VERSION = 4


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def local_origin(value):
    parsed = urllib.parse.urlsplit(value)
    # Numeric loopback only: no DNS, proxy, remote artifacts or credential redirects.
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "::1"}
            or parsed.username or parsed.password or parsed.path not in {"", "/"}
            or parsed.query or parsed.fragment):
        raise argparse.ArgumentTypeError("use a numeric loopback HTTP origin on the Neige host")
    return value.rstrip("/")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError(f"unexpected HTTP redirect ({code}); request not retried")


class LocalWebSocket(connect):
    def process_redirect(self, exc):
        # websockets otherwise follows cross-origin Location with Cookie intact.
        return exc


class Neige:
    def __init__(self, args):
        self.args = args
        self.cookie = args.cookie_file.read_text().strip() if args.cookie_file else ""
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def api(self, method, path, body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if self.cookie:
            headers["Cookie"] = self.cookie
        if key:
            headers["Idempotency-Key"] = key
        req = urllib.request.Request(self.args.url + path, method=method, headers=headers,
                                     data=json.dumps(body).encode() if body is not None else None)
        with self.http.open(req, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None


class Terminal:
    def __init__(self, neige, terminal_id, directory):
        self.neige = neige
        self.terminal_id = terminal_id
        self.directory = directory
        self.client_id = str(uuid.uuid4())
        self.sequence = 0
        self.session_id = None
        self.ws = None
        self.log = (directory / "terminal-wire.jsonl").open("a")

    async def receive(self):
        message = json.loads(await self.ws.recv())
        self.log.write(json.dumps({"time": time.time(), "received": message}) + "\n")
        self.log.flush()
        if "ProtocolError" in message:
            raise RuntimeError(f"terminal protocol error: {message['ProtocolError']}")
        if "RenderPatch" in message:
            patch = message["RenderPatch"]
            await self.ws.send(json.dumps({"RenderAck": {
                "render_rev": patch["render_rev"], "pty_seq": patch["pty_seq"]}}))
        return message

    async def attach(self):
        headers = {"Cookie": self.neige.cookie} if self.neige.cookie else {}
        url = self.neige.args.url.replace("http://", "ws://", 1)
        self.ws = await LocalWebSocket(url + "/api/terminals/" + self.terminal_id,
                                origin=self.neige.args.url, additional_headers=headers,
                                proxy=None, max_size=4 * 1024 * 1024)
        await self.ws.send(json.dumps(self.hello()))
        async with asyncio.timeout(30):
            while True:
                message = await self.receive()
                if "ServerHello" in message:
                    hello = message["ServerHello"]
                    if self.session_id and hello["session_id"] != self.session_id:
                        raise RuntimeError("reconnection changed the PTY session")
                    self.session_id = hello["session_id"]
                    if hello["client_role"] != "Owner":
                        raise RuntimeError("another client owns the benchmark terminal; refusing takeover")
                    if hello["is_child_ready"]:
                        return
                if "ChildReady" in message:
                    return

    def hello(self):
        return {"ClientHello": {
            "protocol_version": TERMINAL_PROTOCOL_VERSION,
            "terminal_id": self.terminal_id, "client_id": self.client_id,
            "desired_size": {"cols": 160, "rows": 40, "pixel_width": None, "pixel_height": None},
            "cell_size": None, "initial_scrollback": "All", "resume_from": None,
            "role_hint": "Owner", "capabilities": {
                "render_encodings": ["Vt"], "supports_scrollback": True,
                "supports_sixel": False, "supports_images": False}}}

    async def send_once(self, command):
        self.sequence += 1
        message = {"Input": {"data": list((command + "\n").encode()), "input_seq": self.sequence}}
        self.log.write(json.dumps({"time": time.time(), "sent": message}) + "\n")
        self.log.flush()
        await self.ws.send(json.dumps(message))
        # ACK is a write receipt, not a deduplication key or a job result.
        async with asyncio.timeout(30):
            while True:
                msg = await self.receive()
                if msg.get("InputAck", {}).get("input_seq") == self.sequence:
                    return
                if "TerminalExited" in msg:
                    raise RuntimeError("terminal exited before acknowledging input; outcome unknown")

    async def run(self, name, argv):
        out = self.directory / (name + ".jsonl")
        err = self.directory / (name + ".stderr")
        result = self.directory / (name + ".exit")
        cmd = (f"umask 077; {shlex.join(argv)} > {shlex.quote(str(out))} "
               f"2> {shlex.quote(str(err))}; benchmark_status=$?; "
               f"printf '%s\\n' \"$benchmark_status\" > {shlex.quote(str(result) + '.tmp')}; "
               f"mv {shlex.quote(str(result) + '.tmp')} {shlex.quote(str(result))}")
        save(self.directory / (name + "-command.json"), {"argv": argv, "terminal_input": cmd})
        await self.send_once(cmd)
        async with asyncio.timeout(self.neige.args.timeout):
            while not result.exists():
                # Drain the same connection; do not reconnect/resubmit a live operation.
                try:
                    msg = await asyncio.wait_for(self.receive(), timeout=0.5)
                except TimeoutError:
                    continue
                if "TerminalExited" in msg:
                    raise RuntimeError("terminal exited during OpenCode run; outcome unknown")
        code = int(result.read_text().strip())
        if code != 0:
            raise RuntimeError(f"{name} exited {code}; inspect {err}")
        return out

    async def close(self):
        if self.ws:
            await self.ws.close()
        self.log.close()


def evidence(path, expected_session=None):
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    sessions = {event.get("sessionID") for event in events}
    if len(sessions) != 1 or not next(iter(sessions), None):
        raise RuntimeError(f"{path.name}: no unique native session ID")
    session = next(iter(sessions))
    if expected_session and session != expected_session:
        raise RuntimeError("OpenCode silently changed the requested native session")
    if any(event.get("type") == "error" for event in events):
        raise RuntimeError(f"{path.name}: OpenCode error event")
    tools = [event["part"] for event in events if event.get("type") == "tool_use"]
    if any(tool.get("state", {}).get("status") != "completed" for tool in tools):
        raise RuntimeError(f"{path.name}: a tool failed or did not complete")
    if any(tool.get("tool") != "bash" for tool in tools):
        raise RuntimeError(f"{path.name}: unexpected tool outside the diagnostic contract")
    bash = [tool for tool in tools if tool.get("tool") == "bash"
            and tool.get("state", {}).get("output")
            and tool.get("state", {}).get("metadata", {}).get("exit") == 0]
    if not bash:
        raise RuntimeError(f"{path.name}: no successful bash tool command/output evidence")
    if len(bash) != len(tools) or any(tool["state"].get("metadata", {}).get("truncated") for tool in bash):
        raise RuntimeError(f"{path.name}: failed or truncated command output")
    if not any(event.get("type") == "step_finish"
               and event.get("part", {}).get("reason") == "stop" for event in events):
        raise RuntimeError(f"{path.name}: missing final stop event")
    answer = "\n".join(event["part"]["text"] for event in events if event.get("type") == "text")
    if not answer.strip():
        raise RuntimeError(f"{path.name}: missing assistant answer")
    return session, answer, bash


DIAGNOSTICS = {
    "free -b": ["free", "-b"],
    "cat /proc/meminfo": ["cat", "/proc/meminfo"],
    "ps -eo pid,rss,comm --sort=-rss | head -11":
        ["ps", "-eo", "pid,rss,comm", "--sort=-rss", "|", "head", "-11"],
}


def diagnostic_results(tools, expected):
    results = {}
    for tool in tools:
        command = tool["state"]["input"].get("command", "")
        tokens = shlex.split(command)
        name = next((name for name, allowed in DIAGNOSTICS.items() if tokens == allowed), None)
        if name not in expected or name in results:
            raise RuntimeError(f"unexpected or repeated diagnostic command: {command}")
        output = tool["state"]["output"]
        if name == "free -b":
            rows = [line.split()[1:] for line in output.splitlines() if len(line.split()) in {4, 7}]
            numbers = [[int(value) for value in row] for row in rows if all(value.isdigit() for value in row)]
            ram = next((row for row in numbers if len(row) == 6), None)
            swap = next((row for row in numbers if len(row) == 3), None)
            if not ram or not swap or ram[0] <= 0 or not 0 <= ram[5] <= ram[0]:
                raise RuntimeError("free did not return total/available RAM and swap in bytes")
            results[name] = {"total_bytes": ram[0], "available_bytes": ram[5], "swap_total_bytes": swap[0]}
        elif name == "cat /proc/meminfo":
            fields = {line.split(":", 1)[0]: line.split(":", 1)[1].split()
                      for line in output.splitlines() if ":" in line}
            parsed = {}
            for field in ["MemTotal", "MemAvailable", "SwapTotal"]:
                values = fields.get(field, [])
                if len(values) != 2 or not values[0].isdigit() or values[1] != "kB":
                    raise RuntimeError("meminfo did not return the required metrics in kB")
                parsed[field] = int(values[0]) * 1024
            results[name] = parsed
        else:
            lines = output.splitlines()
            if not lines or lines[0].split() != ["PID", "RSS", "COMMAND"] or len(lines) < 2:
                raise RuntimeError("ps did not return PID/RSS/command names")
            if len(lines) > 11 or any(len(line.split(maxsplit=2)) != 3
                                      or not all(x.isdigit() for x in line.split(maxsplit=2)[:2])
                                      for line in lines[1:]):
                raise RuntimeError("ps output is malformed or exceeds the bounded process count")
            results[name] = {"rss_unit": "KiB", "processes": len(lines) - 1}
    if set(results) != set(expected):
        raise RuntimeError("required diagnostic commands are missing")
    return results


def sample():
    commands = {"free_bytes": ["free", "-b"],
                "process_rss_kib": ["ps", "-eo", "pid,rss,comm", "--sort=-rss"]}
    result = {"time": time.time(), "hostname": socket.gethostname(),
              "meminfo": Path("/proc/meminfo").read_text()}
    for key, command in commands.items():
        output = subprocess.check_output(command, text=True)
        result[key] = "\n".join(output.splitlines()[:11]) if key == "process_rss_kib" else output
    # Resolve this observer's cgroup, rather than confusing root memory.max with its limit.
    cgroup = next((line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                   if line.startswith("0::")), None)
    if cgroup:
        path = Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "memory.max"
        result["observer_cgroup_memory_max"] = path.read_text().strip() if path.exists() else "unknown"
    return result


async def benchmark(args):
    args.artifacts.mkdir(mode=0o700, parents=True, exist_ok=False)
    neige = Neige(args)
    report = {"status": "incomplete", "url": args.url, "track_id": args.track_id,
              "artifacts": str(args.artifacts), "started": time.time()}
    terminal = None
    try:
        save(args.artifacts / "neige-version.json", neige.api("GET", "/api/version"))
        # A same-host random probe ensures a forwarded remote server cannot pass this benchmark.
        probe = str(uuid.uuid4())
        key = "opencode-memory-" + probe
        body = {"title": "OpenCode memory benchmark", "program": "exec /bin/sh -i",
                "cwd": str(args.cwd), "theme": THEME}
        if args.diagnostic_permissions:
            config = {"share": "disabled", "permission": {
                "*": "deny", "bash": {"*": "deny", "free -b": "allow",
                    "cat /proc/meminfo": "allow", "ps -eo pid,rss,comm --sort=-rss": "allow",
                    "head -11": "allow"}, "external_directory": {"*": "deny", "/proc/*": "allow"}}}
            config_path = args.artifacts / "diagnostic-opencode.json"
            save(config_path, config)
            body["env"] = {"OPENCODE_CONFIG": str(config_path)}
        save(args.artifacts / "create-request.json", {"body": body, "idempotency_key": key})
        card = neige.api("POST", "/api/tracks/" + urllib.parse.quote(args.track_id, safe="")
                         + "/terminal-cards", body, key)
        save(args.artifacts / "card.json", card)
        term = neige.api("GET", "/api/cards/" + card["id"] + "/terminal")
        save(args.artifacts / "terminal.json", term)
        report.update(card_id=card["id"], terminal_id=term["id"])
        save(args.artifacts / "result.json", report)
        terminal = Terminal(neige, term["id"], args.artifacts)
        await terminal.attach()
        probe_path = args.artifacts / "host-probe"
        await terminal.send_once(f"umask 077; {{ printf '%s\\n' {shlex.quote(probe)}; pwd -P; }} "
                                 f"> {shlex.quote(str(probe_path) + '.tmp')}; "
                                 f"mv {shlex.quote(str(probe_path) + '.tmp')} {shlex.quote(str(probe_path))}")
        async with asyncio.timeout(10):
            while not probe_path.exists():
                await asyncio.sleep(0.1)
        if probe_path.read_text().splitlines() != [probe, str(args.cwd)]:
            raise RuntimeError("terminal does not share the benchmark host and selected working directory")
        version = args.artifacts / "opencode-version.txt"
        await terminal.send_once(f"{shlex.quote(str(args.opencode_bin))} --version > {shlex.quote(str(version))}")
        async with asyncio.timeout(10):
            while not version.exists() or not version.read_text().strip():
                await asyncio.sleep(0.1)
        before = sample()
        save(args.artifacts / "independent-before.json", before)
        nonce = uuid.uuid4().hex
        prompt = (f"Remember the conversation marker {nonce} without saving it to any file. "
                  "Use separate bash tool calls to execute exactly these three read-only commands, "
                  "with no additional flags, compound commands or other tools: "
                  "free -b; cat /proc/meminfo; ps -eo pid,rss,comm --sort=-rss | head -11. "
                  "Report total/available host RAM and swap in bytes; process RSS is KiB. "
                  "Include raw command output and a short explanation. Do not read other files, "
                  "edit anything, delegate, or run ETL/audit scripts.")
        argv = [str(args.opencode_bin), "run", "--format", "json"]
        if args.model:
            argv += ["--model", args.model]
        first = await terminal.run("memory", argv + [prompt])
        session, answer, tools = evidence(first)
        metrics = diagnostic_results(tools, DIAGNOSTICS)
        total = int(before["meminfo"].split("MemTotal:", 1)[1].split()[0]) * 1024
        if metrics["free -b"]["total_bytes"] != total or metrics["cat /proc/meminfo"]["MemTotal"] != total:
            raise RuntimeError("OpenCode's host memory total differs from the independent sample")
        report.update(native_session_id=session, pty_session_id=terminal.session_id, memory=metrics["free -b"])
        save(args.artifacts / "result.json", report)
        (args.artifacts / "memory-answer.md").write_text(answer + "\n")
        save(args.artifacts / "independent-after.json", sample())
        # Reattach the actual browser WS path, then explicitly continue native history.
        await terminal.ws.close()
        await terminal.attach()
        continuation = ("Recall the exact conversation marker from my preceding message; "
                        "do not read files to find it. State the previous total AND available RAM "
                        "as exact byte counts, then "
                        "execute exactly this fresh read-only command in one bash tool call: free -b. "
                        "Use no other tools or flags. Show its output.")
        second = await terminal.run("resume", argv + ["--session", session, continuation])
        _, answer, tools = evidence(second, session)
        diagnostic_results(tools, ["free -b"])
        prior_available = metrics["free -b"]["available_bytes"]
        if (nonce not in answer or str(total) not in answer.replace(",", "")
                or str(prior_available) not in answer.replace(",", "")):
            raise RuntimeError("continuation did not retain context and execute the new command")
        (args.artifacts / "resume-answer.md").write_text(answer + "\n")
        await terminal.send_once("exit 0")
        async with asyncio.timeout(30):
            while True:
                try:
                    msg = await terminal.receive()
                except ConnectionClosed as exc:
                    if not exc.rcvd or exc.rcvd.code != 1000 or exc.rcvd.reason != "child-exited":
                        raise
                    save(args.artifacts / "exit-close.json", {"code": exc.rcvd.code, "reason": exc.rcvd.reason})
                    break
                if "TerminalExited" in msg:
                    if msg["TerminalExited"]["code"] != 0:
                        raise RuntimeError("shell did not report a clean exit")
                    break
            # Exit broadcasts can race the sidecar persistence; require the actual row.
            while True:
                exited = neige.api("GET", "/api/cards/" + card["id"] + "/terminal")
                if exited["exit_code"] is not None:
                    save(args.artifacts / "exited-terminal.json", exited)
                    if exited["exit_code"] != 0:
                        raise RuntimeError("the persisted terminal exit was not successful")
                    break
                await asyncio.sleep(0.1)
        await terminal.ws.close()
        # A dead child must produce the stored exit, rather than spawn a new shell.
        headers = {"Cookie": neige.cookie} if neige.cookie else {}
        url = args.url.replace("http://", "ws://", 1) + "/api/terminals/" + term["id"]
        async with LocalWebSocket(url, origin=args.url, additional_headers=headers, proxy=None) as ws:
            await ws.send(json.dumps(terminal.hello()))
            async with asyncio.timeout(30):
                while True:
                    msg = json.loads(await ws.recv())
                    if "ServerHello" in msg and msg["ServerHello"]["session_id"] != terminal.session_id:
                        raise RuntimeError("dead terminal reattachment spawned a new PTY")
                    if "TerminalExited" in msg:
                        save(args.artifacts / "exited-reattach.json", msg)
                        if msg["TerminalExited"]["code"] != 0:
                            raise RuntimeError("dead terminal reattachment did not return its recorded clean exit")
                        break
        report.update(status="passed", finished=time.time(),
                      checks=["memory tool output", "same-host sample", "same live PTY reconnect",
                              "exact native session resume", "retained random marker and memory result",
                              "new tool execution", "clean shell exit", "dead PTY reattach without respawn"])
    except Exception as exc:
        report.update(status="failed_or_unknown", error=f"{type(exc).__name__}: {exc}", finished=time.time())
        raise
    finally:
        save(args.artifacts / "result.json", report)
        if terminal:
            await terminal.close()
        print(json.dumps(report, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", type=local_origin, required=True)
    parser.add_argument("--track-id", required=True, help="existing operator-owned test track")
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--opencode-bin", type=Path, required=True)
    parser.add_argument("--model", help="optional OpenCode provider/model; otherwise use account default")
    parser.add_argument("--diagnostic-permissions", action="store_true",
                        help="explicitly overlay diagnostic command permissions for this benchmark terminal")
    parser.add_argument("--artifacts", type=Path, required=True, help="new directory on this host")
    parser.add_argument("--cookie-file", type=Path, help="private file containing the local Neige Cookie header")
    parser.add_argument("--timeout", type=int, default=180, help="per OpenCode invocation deadline")
    args = parser.parse_args()
    try:
        args.cwd = args.cwd.resolve(strict=True)
        args.opencode_bin = args.opencode_bin.resolve(strict=True)
    except OSError as exc:
        parser.error(str(exc))
    args.artifacts = args.artifacts.absolute()
    if not args.cwd.is_dir() or not os.access(args.opencode_bin, os.X_OK) or args.timeout <= 0:
        parser.error("cwd must be a directory, binary executable and timeout positive")
    os.umask(0o077)
    try:
        asyncio.run(benchmark(args))
    except Exception as exc:
        print(f"Benchmark stopped: {exc}. Inspect artifacts; inputs are never automatically replayed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
