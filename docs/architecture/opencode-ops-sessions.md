# OpenCode operational conversations in Neige

Status: revised design; native conversation integration is not implemented.
The existing terminal diagnostic is a baseline, not completion of this feature.

Tracking: [fork issue #1](https://github.com/ByteYue/neige-calm/issues/1).
Original discussion: [upstream issue #1928](https://github.com/keanji-x/neige-calm/issues/1928).

## Outcome

An operator binds an existing OpenCode native session in Neige, reads its history,
and uses the Neige composer to ask about logs, progress, ETL or audits. Neige
shows assistant replies, tool input/output and actual submission state. Reopening
or refreshing returns to that exact session without resending earlier prompts.

Opening OpenCode in an ordinary terminal already works. A launch button or a
terminal memory benchmark does not supply this outcome. The previous design
removed the essential integration while trying to reduce implementation scope.
The [terminal runbook](../opencode-operational-sessions.md) remains diagnostic
reference only; its passed checks do not validate the proposed conversation path.

## Decision and alternatives

Use a dedicated OpenCode conversation adapter with the existing chat presentation.
Initially connect to an operator-managed loopback `opencode serve`, using its
existing account, native history and project configuration. Bind existing sessions
only, in operator-owned external directories. Neige does not start or supervise
this server in the first increment.

Use synchronous HTTP message submission plus bounded snapshot polling. SSE and
its reconnection machinery are unnecessary for the first usable version. A local
receipt can return before the provider response: it acknowledges durable local
admission, not native execution or completion.

| Alternative | What source inspection establishes | Decision |
| --- | --- | --- |
| Existing terminal | Runs OpenCode but supplies no native binding, composer or structured transcript | Baseline only |
| Per-turn CLI JSON | Provides output but needs separate history commands, lacks caller-selected message IDs and has unattended permission limitations | Do not build a second control path |
| Extend PlainChat/Planner | PlainChat and Assistant select Codex; existing turn-start failures can be retried | Too much shared lifecycle and authority for this outcome |
| Dedicated HTTP adapter | Native history, messages, pending requests and abort already exist | Smallest complete conversation path |

Two design rounds inform this choice. The first removes terminal launch and asks
whether the required Neige workflow remains: it does not. The second removes
Planner, worker scheduling, server supervision, SSE, model catalog and Neige MCP
from the native-conversation proposal: binding, sending, history and control still
remain. These are source-level design comparisons, not runtime experiments or
measured performance results.

## Product contract

- Select an existing native session from a bounded, directory-scoped list or
  provide its exact ID. Show the selected directory and native ID.
- Load a bounded native history snapshot with explicit truncation/pagination.
  An absent session is an error, never a fresh session or a latest-session fallback.
- Send plain text from the Neige composer. Native messages and parts supply the
  transcript; Neige records its own submissions separately.
- Show assistant text and expandable tool input/output, including running,
  completed and error states. Preserve unsupported parts visibly.
- Expose waiting permissions and questions. Allow one-time permission approval
  or rejection and typed question answers. Do not enable blanket auto-approval
  or instance-wide remembered permission grants.
- Show submitting, running, waiting, failed, cancelling and unknown distinctly.
  Allow one unresolved submission per native session; no queue or steer.
- Stop is an explicit request against the bound session. Confirm native terminal
  evidence before reporting cancellation; an abort response is not proof that
  external jobs stopped or earlier side effects were reversed.

Initial binding is read-only until the operator transfers the session's sole
writing responsibility to Neige. Same-server busy checks are useful but are not
an atomic lock. Another TUI process using the same native database may be active
without appearing in this server's status. Concurrent TUI/Neige writing is outside
the first contract. Detect external user messages or uncertain ownership and stop
sending; do not abort another client's work.

## Ownership and persistence

Configure the connection on the server through a typed loopback endpoint and
credential reference. The browser selects a configured connection key, never an
arbitrary URL or credential. Disable redirects and automatic POST retries.
Every native request carries the validated directory scope. A changed connection
scope invalidates the binding rather than silently targeting another account.

Keep a server-owned card binding: connection/scope reference, canonical directory,
exact native session ID and binding generation. Create it through a dedicated
entry point and reuse workspace freezing. Generic card creation/PATCH cannot
forge or retarget this binding. Bind admission uses the existing canonical
workspace ownership policy to reject Neige-managed directory scopes, including
other tracks, paths within managed workspace roots, and kernel-created Attached
track/lease worktrees and their descendants outside those roots. Track/area deletion can
recycle those paths, so checking only the card's own track kind is insufficient.
Managed-workspace support needs a later registered workspace-use guard.
Register its identity and disposal behavior in the
owning card extension surface rather than hardcoding OpenCode in generic policy.

One new submission journal is sufficient initially. Its unresolved claim is unique
across cards and tracks by canonical connection scope, directory and native session
ID, excluding card ID and binding generation. Multiple read-only views may coexist;
only one view holds writing responsibility. Store the binding identity,
request key, content fingerprint, prompt, preallocated native message ID and
submission state before dispatch. A durable claim permits one send attempt. The
same request key and content returns the original receipt; different content
conflicts. Native message IDs correlate history; they do not make OpenCode POSTs
idempotent.

After response loss, timeout or Neige restart, an attempted submission becomes
unknown and is only reconciled by exact native message lookup and related parts.
No automatic resend occurs. Missing messages or default idle cannot prove that
nothing executed. Unknown blocks new sends until correlated terminal evidence resolves it; it never
quietly becomes a retryable failure. Acknowledging or abandoning the receipt does
not declare it unsent or clear the native-session fence. Without such evidence,
the operator can detach and select a different existing session.

Completion requires a correlated final assistant message, persisted completion
and qualified finish/error semantics, with related tools settled. Idle alone is
insufficient. Protocol completion and operational success remain distinct: an
ETL or audit succeeds according to its actual command result and artifacts.

Card, track and area deletion share the registered disposal path. Removal detaches
Neige and does not delete native history, abort native work or stop the user's
server. Retain unresolved journal records independently of card deletion; the
same scope/session remains fenced if it is rebound. Cancel undispatched claims
atomically; attempted claims retain their uncertainty and reconciliation record.
Removing a read-only view cannot cancel another view's submission claim.

Pending permission/question lists contain other sessions. Filter by the binding
and revalidate request ownership before replying. Stale requests produce visible
conflicts; they never cause approval of another request or automatic retries.

## Implementation boundaries

| Owner | Minimal change |
| --- | --- |
| Server OpenCode feature | Typed HTTP client, snapshots, binding and submission controller, reconciliation, pending replies and abort |
| REST/configuration | Human-user authorization, configured connection selection, bind/read/send/stop/reply entry points and OpenAPI wiring |
| Truth persistence | New journal migration and typed repository operations; server-owned binding and unresolved-session fence |
| Card lifecycle | Registered identity, protected binding fields, workspace freeze, disposal through card/track/area paths and boot reconciliation |
| Frontend core | Typed API contracts and native-to-conversation projection, generated artifacts where required |
| Frontend app/chat | Existing composer/thread presentation, a narrow OpenCode data/actions port, session selection and tool output disclosure |

Do not manufacture a worker session, Codex notification or MCP principal for this
feature. Keep PlannerBackend, WorkerProvider, scheduler and task kinds unchanged.
Native snapshots remain the transcript authority; do not add a second full message
store or copy the Harness queue/state machine.

Respect frontend layers: systems registers a headless conversation identity;
app composes the data port with chat presentation. Systems must not import chat
features, and a separate feature must not import another feature horizontally.
The current app conversation store is Harness-specific and cannot be reused as-is.
Existing activity `detail` is an error summary: add typed tool input/output
presentation rather than putting successful output into that field.

## Roadmap and acceptance

| Stage | Deliverable | Exit check |
| --- | --- | --- |
| P1 | Bind an existing session and render native history through Neige | Exact identity and directory; missing session fails; two sessions never mix |
| P2 | Durable send, snapshot progress and structured replies through the Neige composer | Two consecutive turns use the same session; tool input/output is visible; busy blocks another send |
| P3 | Pending replies, explicit stop, restart/unknown reconciliation and disposal | Permission/question round trip; refresh/restart restore history; lost response never resends; deletion/rebind retains unresolved fences |

P1/P2 are implementation slices; P3 completes the first operational release.
New-session creation, automatic server launch, model-selection UI, SSE and full
Planner/provider support require a later demonstrated need.

The principal automated acceptance uses production Neige REST entry points with a
local fake OpenCode server: record one tool-side effect, drop the response, then
retry the browser request and restart Neige. Dispatch count remains one and the
submission stays unknown until positive native evidence resolves it. Also cover
request-key conflicts, two cards concurrently sending to the same native session
(total dispatch count one), foreign-session pending requests, idle
false positives, invalid bindings, managed-directory admission rejection
(including another track's managed path and generated Attached worktree), stale
replies and deletion/rebind. Read-only
views cannot stop work; an external turn after our completed request cannot be
aborted through that old request.

After those checks, use the real Neige composer to ask for host memory as one
read-only example. Inspect structured command output, refresh, then ask a related
second question in the same session. This validates the new product workflow; a
terminal-only result does not pass. Use a controlled script marker for execution
and cancellation checks; do not run production ETL merely to demonstrate support.

Run focused Rust tests, critical assertion mutations in an exclusive worktree,
relevant Codex/Claude regressions, frontend lint/build/tests, real-browser preview
and integrated browser checks. Run text ratchets and two independent diff reviews.
Never enable real Codex E2E on the shared production host.

## Evidence and limitations

Source baseline: Neige `17cfb595`; OpenCode `1.18.34`, pinned commit
`aec0b9a6d8898f68f923aaf08b7306d931fd9d76`. Important Neige paths are
`harness/profile.rs`, `harness/run_loop.rs`, `routes/track_conversations.rs`,
`routes/cards.rs`, and frontend chat/thread plus app conversation composition.
The [OpenCode server API](https://opencode.ai/docs/server/) supplies native
sessions/messages/abort. Pinned message submission, completion and pending-request
behavior were checked in OpenCode's session prompt and instance HTTP handlers.

No proposed HTTP integration, recovery or cancellation test has run yet. Existing
terminal diagnostic evidence must not be reported as native-feature acceptance.
