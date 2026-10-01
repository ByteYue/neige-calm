# OpenCode operational sessions

OpenCode can run inside an ordinary Neige terminal card. The first increment
adds a reproducible memory/continuation benchmark using the existing HTTP and
WebSocket terminal entry points. Planner, worker providers and storage stay as
defined by the [design](architecture/opencode-ops-sessions.md).

## Interactive operation

Open a terminal in your Neige track and run the executable installed for your
account. If it is absent from PATH, enter its absolute path. On the acceptance
host that was:

```bash
/home/jingyue/.nvm/versions/node/v20.20.0/lib/node_modules/opencode-ai/bin/opencode.exe
```

Enter operational questions after the TUI becomes ready. To load a specific
quiescent conversation, launch with `--session <native-session-id>` in the same
directory/account/configuration. Use an explicit native ID, rather than
`--continue`. Browser reconnection attaches the existing PTY; after process exit,
open another terminal and explicitly load the conversation. Do not automatically
retry an interrupted ETL/audit prompt.

## Reproduce the benchmark

Run on the Neige host, against a local test instance and an existing test track.
Python 3.11+ and `websockets` 15 are required. The selected OpenCode account needs
a working model subscription/API credential. Install dependencies only after
authorizing their outbound traffic, as required by the repository's operator
rules. No credential is copied into the repository.

```bash
python3 scripts/opencode-terminal-benchmark.py \
  --url http://127.0.0.1:19404 \
  --track-id <test-track-id> \
  --cwd /absolute/test/workspace \
  --opencode-bin /absolute/path/to/opencode \
  --artifacts /absolute/new/artifact-directory \
  --diagnostic-permissions
```

An authenticated instance also needs `--cookie-file /private/cookie-header.txt`;
the file contains the existing local Neige Cookie header. For an explicit model,
add `--model provider/model`. The helper refuses remote origins and HTTP/WS
redirects. It creates only its own terminal card, retains evidence, and exits its
shell after success. Failed/unknown runs can leave a shell or OpenCode child
alive; inspect the recorded card before manually stopping it. Deleting this
benchmark card through Neige uses normal terminal cleanup. Each retry needs a
new artifact directory and creates a fresh conversation; no submitted prompt is
automatically replayed.

`--diagnostic-permissions` explicitly passes a generated `OPENCODE_CONFIG` file
through the existing terminal environment contract. It denies other tools and
allows the exact three diagnostic commands, with the external directory guard
for `/proc/*`. It disables sharing, retains the normal HOME/history/auth account,
and does not use `--auto`. This is OpenCode policy, not an OS sandbox; project,
agent or managed configuration can affect effective policy. The checker rejects
unexpected tools/commands and incomplete or truncated results. Without this flag,
the helper uses the existing account policy and reports denied diagnostics as a
failure. The initial live test demonstrated that `/proc` needs explicit access.

The helper waits for terminal input readiness, sends each command once, and
records write ACKs without treating them as completion or deduplication. Complete
JSONL tool outputs are saved on the host because terminal replay is bounded.
Continuation uses the actual native session ID from JSON events and must recall
a random first-turn marker and previous memory byte counts, then execute a new
`free -b`. Reconnection must preserve the PTY ID; clean exit and reattachment
must return the stored exit without spawning another child.

## Recorded acceptance

The complete run started at **2026-10-01 18:23:03 Asia/Shanghai** using the local
branch based on `ee449f5e`, a newly compiled Neige kernel, OpenCode **1.18.34** and
the account's default **deepseek/deepseek-flash** (verified from a read-only native
session export). The test instance used a separate SQLite database, runtime,
workspace and process supervisor on loopback. Real Codex E2E was not enabled.

| Evidence | Value |
| --- | --- |
| Neige card | `db39b9dd827244db9f489757a616d6a7` |
| Terminal | `a78ecd6238614cedb0392cb701fef8be` |
| PTY session | `6301b43d-9e17-4eba-9e3c-6a0bd6915f93` |
| OpenCode native session | `ses_f0902c53effeARaRzAvaYiwRrv` |
| Host RAM total | 134,916,562,944 bytes |
| Host RAM available at first sample | 54,940,860,416 bytes |
| Swap total | 1,023,406,080 bytes |
| Swap free | 0 bytes |
| Benchmark observer cgroup memory.max | 64,424,509,440 bytes (60 GiB) |

The cgroup value belongs to the benchmark observer, not a measurement of the
OpenCode process's own limit. Process RSS output is recorded in KiB and describes
the host's top processes; it does not isolate the newly launched OpenCode process.
RAM availability changes between samples; host total matched independent
`/proc/meminfo` measurements. This is functional acceptance, not a performance
comparison.

Local complete artifacts are at
`/tmp/neige-opencode-s1-20261001/acceptance-complete/`: `result.json`, both command
JSONL streams/answers, independent before/after samples, terminal wire receipts,
version and exit/reattachment evidence. This temporary directory is local and is
not included in the GitHub branch. Earlier failed runs are retained alongside it.
The final script was rerun successfully with fresh receipts at
`/tmp/neige-opencode-s1-20261001/acceptance-delivery/`, including the nonce and
physical-cwd probe added during review.

Focused verification:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts -p test_opencode_terminal_benchmark.py -v
scripts/local-ratchet-gates.sh
git diff --check
```

Seven focused tests passed, including real loopback HTTP/WS redirects that must not
forward a Cookie. A single production mutation allowing WS redirects caused
exactly `test_ws_redirect_does_not_forward_cookie` to fail; byte-for-byte
restoration returned the then-current six tests to green. A seventh regression
accepts process command names containing spaces. The final script also checks
the shell's physical cwd against the selected directory.

Real Chromium preview loaded this exact native session in OpenCode's TUI through
Neige and displayed the prior memory results. Browser reload retained the PTY
`65bcc996-0478-482d-adf7-584953a36884`; no page errors were observed. Local evidence:
`browser-state.json` and `browser-opencode-resume.png` in the test root.

Separate negative checks used the same Neige terminal path: missing executable
returned 127; explicit missing native session returned 1 with `Session not found`;
a localhost provider fixture returning 401 produced OpenCode's authentication
error event and exit 1. This proves error propagation, not an external provider's
current credential state. The helper rejected a nonexistent cwd before any card
was created. The existing terminal API accepts that cwd and may start elsewhere,
so verify `pwd -P` before manual use; its broader cwd behavior is outside this
benchmark change. Deleting the negative-test card made its terminal lookup return
404. Fresh receipts are at `negative-revalidated/results.json` in the test root.

S2 launch affordances remain conditional on an actual usability need.
