# OpenCode operational sessions through Neige

Status: scoped design; implementation and live acceptance are pending.

Tracking: [fork issue #1](https://github.com/ByteYue/neige-calm/issues/1).
Original scope: [upstream issue #1928](https://github.com/keanji-x/neige-calm/issues/1928).

## Outcome

An operator opens OpenCode in Neige, selects an explicit existing native session
when needed, and enters operational questions, log/progress checks, ETL commands
or audit prompts. The first acceptance asks OpenCode to inspect host memory and
then continues that exact conversation.

## Decision

Start with the existing terminal surface. OpenCode runs as an ordinary developer
CLI inside a Neige terminal card. Neige owns the card, PTY, input receipts and
process lifecycle; OpenCode owns conversation history, tools, models and native
session IDs. This is terminal integration, not a new Planner provider.

The production path already exists:

`terminal-cards` route -> `TerminalAdapter` -> proc supervisor/renderer ->
`TerminalCardView` and terminal input/observe/control.

The route accepts `program`, `cwd`, `env`, `title`, theme and an optional
idempotency key. The frontend API contract already includes these fields. A
human can also create an ordinary shell terminal and run OpenCode inside it.

Startup contains only a shell or a prompt-free OpenCode TUI. The terminal-create
saga can retry a launch after a crash; an operational prompt must not be embedded
in its program or startup flags. Wait for successful creation and terminal
readiness, then enter the task. Automated input uses the existing stable request
receipts and checks them after a lost response rather than submitting a new ID.
Run a bounded JSON command inside that established shell, not as its startup
program. This restriction avoids changing terminal recovery for this increment.

The initial implementation does not change PlannerBackend, the Harness state
machine, scheduler, task kinds, agent/worker provider enums, actor identities,
MCP credentials, database migrations or transcript schemas. Keep the card kind
and execution provider as `terminal`; changing their labels would expand
projection, cleanup and recovery responsibilities.

## Session behavior

- New interactive session: start the selected OpenCode executable in the chosen
  working directory. The native TUI handles prompts, permissions and questions.
- Existing session: use `--session <native-id>` with the same account and
  configuration that own its history. Never substitute `--continue` or silently
  create another session when the requested ID is missing.
- Live reattachment: browser reconnection joins the existing PTY. A surviving
  supervisor may also allow Neige to reattach after a kernel restart.
- Process exit: reconnect does not respawn a dead child. An ordinary shell stays
  usable when its OpenCode child exits; otherwise open a new terminal and
  explicitly resume the recorded native session.
- Interrupted operation: retain output and report uncertainty. Do not rerun a
  prompt automatically, especially when it may have invoked an ETL/audit script.
- Concurrency: avoid multiple writers to one native session. A fresh attachment
  is for an explicitly selected, operator-owned, quiescent conversation.

The TUI does not provide a reliable machine-readable session-created receipt.
Do not extract session IDs from ANSI screen bytes. A bounded
`opencode run --format json` can expose the native ID in output events; a run
that ends before such an event may leave that ID unknown. A typed native
session-list binding is a later convenience if manual selection is insufficient.

`run` reads one complete prompt from non-TTY stdin and is not a continuing chat
stream. Its unattended permission/question behavior differs from the TUI; do
not enable `--auto` by default. The interactive TUI is the daily operations path.

## First acceptance: memory query and exact continuation

1. Select a configured local Neige test instance and the operator's OpenCode
   executable/account. Record Neige build, OpenCode version and working directory.
2. Create an ordinary shell terminal through Neige and wait for the create
   operation to succeed and the shell to become ready. Then launch OpenCode via
   terminal input. Do not put the diagnostic prompt in terminal startup or launch
   it directly from the test driver and call that Neige integration.
3. Ask it to execute bounded read-only diagnostics: `free -b`, `/proc/meminfo`,
   and a short process listing containing PID, RSS and command name. Do not run
   production ETL/audit scripts for this benchmark.
4. Preserve command/tool output, answer, timestamps, Neige card/terminal IDs and
   the actual native session ID when available. A response without command
   evidence does not pass.
5. Independently sample the same memory metrics on the same host. Distinguish
   host total/available memory and swap from Neige/OpenCode process RSS and any
   cgroup limit. Samples taken at different times need not be exactly equal.
6. Explicitly resume the recorded conversation, ask about the preceding result
   and run a second read-only command. Verify both retained context and new work.
7. Reconnect the browser and verify the same live PTY. Separately stop/exit the
   process and verify that resume requires an explicit launch and native ID.

Terminal screen/replay and persisted output tails are bounded; a capture may be
truncated. Save complete benchmark command output as an explicit artifact before
that bound is reached, and retain truncation/unknown markers. Terminal running
state describes the shell/TUI process, not an OpenCode turn or an ETL job. Inspect
job progress and results through their actual commands and artifacts.

This proves functional integration. Startup time and process RSS can be recorded
as observations; they are not performance claims without comparable samples.

## Roadmap

| Stage | Change | Acceptance |
| --- | --- | --- |
| S0 | This design, independent reviews, tracking issue and branch | Scope and lifecycle boundaries are explicit |
| S1 | Reproducible launch and memory/continuation acceptance through existing terminal entry points | Real OpenCode tool output and exact native session continuation |
| S2 | A narrow launch affordance, only if S1 exposes a usability gap | Executable/cwd/session choices reach existing terminal creation without shell interpolation |
| Later | Structured per-turn output or HTTP/SSE when actually required | Separate design for submission uncertainty, snapshots and session authorization |

S1 needs no new kernel protocol. A benchmark helper, if needed, only uses the
existing terminal entry points and saves receipts/evidence; it must not copy
terminal lifecycle logic or hide launch failures.

For S2, determine the smallest UI contract from the baseline. The current add
menu exposes a fieldless terminal action, while the API already accepts program
and cwd. Reuse the existing form, directory picker, API operation and terminal
renderer. Avoid a new `opencode` card identity or a general provider framework.
Validate shell argument construction in the owning launch feature and forward
errors. The existing one-click shell workflow should remain available.

## Authority and verification

Use the existing human-terminal environment contract for this increment. Do not
silently switch HOME/configuration, copy credentials, inject a Neige MCP token
or claim that OpenCode permissions provide an OS sandbox. If a dedicated
credential-sensitive adapter becomes necessary, it needs typed configuration and
an explicit child environment allowlist as a separate boundary change.

Before delivery of S1/S2, check launch, exact resume, missing binary, invalid cwd,
missing session, authentication failure, browser reattachment, process exit and
card/track cleanup. Unknown outcomes must not cause automatic prompt replay.
Preserve existing ordinary terminal, Codex and Claude behavior.

Use the real production entry points for tests. Any frontend change requires its
applicable layer guidance, lint/build/unit gates, real-browser preview and
relevant browser tests. Run text ratchets for every repository change and review
non-mechanical changes through two independent channels. Do not run real Codex
E2E on the shared host.

## Evidence and current limits

- Baseline: Neige `a4a16f14`; no runtime implementation changed for this design.
- Installed OpenCode reports `1.18.34`; this shell does not find it on PATH.
  The launch must resolve the operator's selected executable explicitly.
- Current terminal lifecycle was checked in
  `crates/calm-server/src/routes/terminal_cards.rs`,
  `crates/calm-server/src/operation/terminal_adapter.rs`, and
  `crates/calm-server/src/ws/terminal.rs`.
- The real Neige/OpenCode memory benchmark is pending; no result or performance
  improvement is claimed.
- Protocol reference: [OpenCode CLI documentation](https://opencode.ai/docs/cli/)
  and pinned [v1.18.34 run implementation](https://github.com/anomalyco/opencode/blob/aec0b9a6d8898f68f923aaf08b7306d931fd9d76/packages/opencode/src/cli/cmd/run.ts).
