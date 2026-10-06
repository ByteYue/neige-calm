#!/usr/bin/python3
"""Offline native HTTP/MCP protocol fixture; Neige owns admission and lifecycle."""
import base64
import http.server
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

if "--version" in sys.argv:
    print("1.18.34")
    raise SystemExit(0)

ROOT = Path(__file__).resolve().parent
SETTINGS = json.loads(Path(os.environ["OPENCODE_CONFIG"]).read_text())
PORT = int(sys.argv[sys.argv.index("--port") + 1])
LOCK = threading.RLock()
DB = ROOT / "native.sqlite"
with sqlite3.connect(DB) as db:
    db.execute("CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, directory TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY, id TEXT UNIQUE, sid TEXT, body TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS requests (seq INTEGER PRIMARY KEY, sid TEXT, payload TEXT)")

def record(name, value):
    with LOCK, (ROOT / name).open("a") as stream:
        stream.write(json.dumps(value) + "\n")

record("spawns.jsonl", {"pid": os.getpid(), "cwd": os.getcwd(), "settings": SETTINGS,
                        "marker": os.environ["NEIGE_CLAUDE_PLANNER"]})

def store(sid, body):
    with LOCK, sqlite3.connect(DB) as db:
        db.execute("INSERT OR REPLACE INTO messages(id,sid,body) VALUES (?,?,?)",
                   (body["info"]["id"], sid, json.dumps(body)))

def messages(sid):
    with LOCK, sqlite3.connect(DB) as db:
        return [json.loads(row[0]) for row in db.execute(
            "SELECT body FROM messages WHERE sid=? ORDER BY json_extract(body, '$.info.time.created'), id", (sid,))]

def mcp():
    env = SETTINGS["mcp"]["neige"]["environment"]
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(10)
        sock.connect(env["NEIGE_MCP_SOCKET"])
        stream = sock.makefile("rw")
        def call(request):
            stream.write(json.dumps(request) + "\n")
            stream.flush()
            return json.loads(stream.readline())
        init = call({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "fake-opencode", "version": "1.18.34"},
            "_meta": {"dev.neige/auth": {"token": env["NEIGE_MCP_TOKEN"]}}}})
        record("mcp.jsonl", init)
        if "error" in init:
            raise RuntimeError(init)
        stream.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        stream.flush()
        reply = call({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "neige_user_ask", "arguments": {"questions": [{"title": "native MCP question"}]}}})
        record("mcp.jsonl", reply)
        if "error" in reply or reply.get("result", {}).get("isError"):
            raise RuntimeError(reply)
        return json.dumps(reply["result"])

def assistant(sid, user, running=False, aborted=False, stop_step=False):
    now = int(time.time() * 1000)
    aid = "msg_" + uuid.uuid4().hex
    info = {"id": aid, "sessionID": sid, "role": "assistant", "parentID": user,
            "time": {"created": now}, "tokens": {"input": 10, "output": 5,
            "reasoning": 0, "cache": {"read": 0, "write": 0}}}
    state = {"status": "running" if running else "error" if aborted else "completed",
             "input": {"command": "free -b"}, "time": {"start": now},
             "output": "memory fixture output", "metadata": {"exit": 0}}
    if not running:
        info["time"]["completed"] = now + 1
        info["finish"] = "stop" if stop_step or aborted else "tool-calls"
        state["time"]["end"] = now + 1
    if aborted:
        info["error"] = {"name": "MessageAbortedError", "data": {"message": "aborted"}}
        state["error"] = "aborted"
    parts = [{"id": "prt_" + uuid.uuid4().hex, "messageID": aid, "sessionID": sid,
              "type": "tool", "tool": "bash", "state": state}]
    if not running and not aborted:
        parts.extend([
            {"id": "prt_" + uuid.uuid4().hex, "type": "tool", "tool": "neige_neige_user_ask",
             "state": {"status": "completed", "input": {"questions": [{"title": "native MCP question"}]},
                       "output": mcp(), "time": {"start": now, "end": now + 1}}}])
    return {"info": info, "parts": parts}

def final_reply(sid, user, after):
    now = max(int(time.time() * 1000), after + 2)
    aid = "msg_" + uuid.uuid4().hex
    return {"info": {"id": aid, "sessionID": sid, "role": "assistant", "parentID": user,
                     "time": {"created": now, "completed": now + 1}, "finish": "stop",
                     "tokens": {"input": 4, "output": 3, "reasoning": 0,
                                "cache": {"read": 0, "write": 0}}},
            "parts": [{"id": "prt_" + uuid.uuid4().hex, "messageID": aid, "sessionID": sid,
                       "type": "text", "text": "memory fixture answer",
                       "time": {"start": now, "end": now + 1}}]}

def finish_after_release(sid, user):
    while not (ROOT / "release").exists():
        time.sleep(0.05)
    step = assistant(sid, user)
    store(sid, step)
    store(sid, final_reply(sid, user, step["info"]["time"]["created"]))

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def authenticated(self):
        expected = "Basic " + base64.b64encode(
            ("opencode:" + os.environ["OPENCODE_SERVER_PASSWORD"]).encode()).decode()
        if self.headers.get("Authorization") != expected:
            self.reply(401, {"error": "unauthorized"})
            return False
        return True

    def reply(self, status, body, headers=None):
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if not self.authenticated():
            return
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        path = url.path.split("/")
        if url.path == "/global/health":
            self.reply(200, {"healthy": True, "version": "1.18.34"})
        elif url.path == "/provider":
            self.reply(200, {"connected": ["fixture"], "all": [{"id": "fixture", "models": {
                "model-a": {"name": "Fixture A", "variants": {"small": {}, "large": {}}},
                "model-b": {"name": "Fixture B", "variants": {"large": {}}}}}]})
        elif url.path == "/config":
            self.reply(200, {"model": "fixture/model-a"})
        elif url.path in ("/permission", "/question"):
            self.reply(200, [])
        elif len(path) == 3 and path[1] == "session":
            with sqlite3.connect(DB) as db:
                row = db.execute("SELECT directory FROM sessions WHERE id=?", (path[2],)).fetchone()
            self.reply(200 if row else 404, {"id": path[2], "directory": row[0]} if row else {})
        elif len(path) >= 4 and path[1] == "session" and path[3] == "message":
            rows = messages(path[2])
            if len(path) == 5:
                if (ROOT / "scenario").read_text().strip() == "loss-terminal" and not (ROOT / "release").exists():
                    self.reply(404, {})
                    return
                exact = next((row for row in rows if row["info"]["id"] == path[4]), None)
                self.reply(200 if exact else 404, exact or {})
            else:
                end = int(query.get("before", [str(len(rows))])[0])
                limit = int(query.get("limit", ["256"])[0])
                start = max(0, end - limit)
                self.reply(200, rows[start:end], {"X-Next-Cursor": str(start)} if start else {})
        else:
            self.reply(404, {})

    def do_POST(self):
        if not self.authenticated():
            return
        url = urllib.parse.urlsplit(self.path)
        path = url.path.split("/")
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or "{}")
        record("native-writes.jsonl", {"method": "POST", "path": url.path, "payload": payload})
        if url.path == "/session":
            sid = "ses_" + uuid.uuid4().hex
            directory = urllib.parse.parse_qs(url.query)["directory"][0]
            with LOCK, sqlite3.connect(DB) as db:
                db.execute("INSERT INTO sessions VALUES (?,?)", (sid, directory))
            self.reply(200, {"id": sid, "directory": directory})
        elif len(path) == 4 and path[1] == "session" and path[3] == "abort":
            for row in messages(path[2]):
                if row["info"]["role"] == "assistant" and "completed" not in row["info"]["time"]:
                    row["info"]["time"]["completed"] = int(time.time() * 1000)
                    row["info"]["error"] = {"name": "MessageAbortedError", "data": {"message": "aborted"}}
                    for part in row["parts"]:
                        part["state"]["status"] = "error"
                        part["state"]["error"] = "aborted"
                    store(path[2], row)
            self.reply(200, True)
        elif len(path) == 4 and path[1] == "session" and path[3] == "message":
            sid = path[2]
            with LOCK, sqlite3.connect(DB) as db:
                db.execute("INSERT INTO requests(sid,payload) VALUES (?,?)", (sid, json.dumps(payload)))
            record("requests.jsonl", {"session": sid, "payload": payload})
            mode = (ROOT / "scenario").read_text().strip()
            user = {"info": {"id": payload["messageID"], "sessionID": sid, "role": "user",
                             "time": {"created": int(time.time() * 1000)}, "model": payload.get("model")},
                    "parts": payload["parts"]}
            store(sid, user)
            if mode in ("loss", "loss-terminal"):
                if mode == "loss-terminal":
                    threading.Thread(target=finish_after_release,
                                     args=(sid, payload["messageID"]), daemon=True).start()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            result = assistant(sid, payload["messageID"], running=mode == "busy",
                               stop_step=mode == "held-stop")
            store(sid, result)
            if mode == "busy":
                child = subprocess.Popen(["/bin/sleep", "300"], start_new_session=True)
                record("children.jsonl", {"pid": child.pid})
                while any(row["info"]["id"] == result["info"]["id"] and
                          "completed" not in row["info"]["time"] for row in messages(sid)):
                    time.sleep(0.05)
            else:
                if mode == "held-stop":
                    while not (ROOT / "release").exists():
                        time.sleep(0.05)
                result = final_reply(sid, payload["messageID"], result["info"]["time"]["created"])
                store(sid, result)
            self.reply(200, result)
        else:
            self.reply(404, {})

server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
print(f"opencode server listening on http://127.0.0.1:{server.server_port}", flush=True)
server.serve_forever()
