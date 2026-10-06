# OpenCode Planner

OpenCode can run a track's Planner alongside the existing Codex and Claude
backends. Choose **OpenCode** in the new-track dialog, then use the existing
Planner chat to ask questions, inspect command and MCP tool output, stop work,
and continue the same native session. Task Workers, Assistant and PlainChat do
not use this backend yet.

## Configuration

Install OpenCode **1.18.34**. Create a dedicated profile; Neige never imports the
normal user's credentials or configuration automatically. The profile root is
its `HOME`, and its `config`, `data`, `cache` and `state` children are its XDG
roots. Configure authentication and a default model using this profile:

```bash
neige_profile="$HOME/.local/share/neige-opencode-planner"
mkdir -p "$neige_profile"/{config,data,cache,state}
chmod 700 "$neige_profile" "$neige_profile"/{config,data,cache,state}
env HOME="$neige_profile" \
  XDG_CONFIG_HOME="$neige_profile/config" \
  XDG_DATA_HOME="$neige_profile/data" \
  XDG_CACHE_HOME="$neige_profile/cache" \
  XDG_STATE_HOME="$neige_profile/state" \
  /absolute/path/to/opencode auth login
```

Put the profile's normal OpenCode configuration, including its `model` setting,
at `<profile>/config/opencode/opencode.json`. Keep provider credentials in the
profile rather than passing ambient environment secrets. Write the following
Neige configuration with absolute paths:

```json
{
  "opencode_binary": "/absolute/path/to/opencode",
  "opencode_version": "1.18.34",
  "config_dir": "/home/owner/.local/share/neige-opencode-planner"
}
```

Start `calm-server --opencode-planner-config /absolute/path/planner.json`.
For a `neige-app` installation, add these two arguments to `child.extra_args`:

```toml
[child]
extra_args = ["--opencode-planner-config", "/home/owner/.config/neige-app/opencode-planner.json"]
```

Restart the host after changing its configuration. Settings reports provider
readiness, and the model menu uses the dedicated profile's connected providers.
A selected model is the full native `providerID/modelID`; **Variant** shows the
variants declared by that model. Clearing the model follows the configured
profile default. Select an explicit model when that default is unavailable.

The dedicated profile's credentials, model and native permission rules are read
when a managed server starts. Allow the specific external directories needed by
your operational commands in that profile. For example, a read-only memory
check that reads `/proc/meminfo` needs an `external_directory` rule for `/proc/*`.
Requests requiring an interactive approval are rejected by this first increment.

## Behavior and boundaries

Neige creates a private, authenticated loopback OpenCode server for a Planner's
native session. Each instance receives that Planner's MCP credential. Workspace
instruction files are included explicitly; workspace OpenCode configuration is
disabled so it cannot replace the managed credential or process configuration.
Ordinary operating-system tools remain available. This is not an OS sandbox.
The serve process and its MCP shim receive only the current Planner's scoped
Neige credential; its shell can use `neige state`, `neige ls` and `neige cat`.

The existing Harness owns pending input and transcript storage. Native message
and part identities reconcile assistant text, command execution and tool output
across refresh and restart. Snapshot polling supplies progress; the adapter does
not treat idle, a successful abort response or a tool step finishing as proof of
successful turn completion. Pending native permission/question requests are
rejected and shown as failed activity; this increment does not add interactive
request forms.

Managed sessions disable native automatic compaction and pruning to preserve the
original submission's message correlation and tool evidence. Context overflow
fails visibly; reset or create a session explicitly when more context is needed.
This first increment supports text input. Its attachment controls are disabled;
historical image attachments remain readable.

A durable submission record is written before one native prompt POST. A lost
response retains the original request and blocks retransmission while Neige
queries its native evidence. A confirmed never-sent record is distinguished from
an uncertain attempted request. Reset refuses an unresolved submission rather
than moving its observer to a new thread. Stop requests cancellation and cleans
up owned processes; an uncertain outcome remains uncertain. Boot, reset, deletion
and workspace repoint revoke the relevant Planner credentials and stop owned
processes. Deletion retains the submission journal.

Submission generation `0` identifies this first adapter configuration format;
it is not a profile-content digest or a credential revocation epoch. The recorded
submission freezes its resolved model, variant, instructions and input. Reloading
the private profile never authorizes resending an uncertain submission.

## Why HTTP first

The pinned `opencode acp` implementation uses a stdio ACP facade over an internal
HTTP server. Its caller message identifier is not passed into native prompt
admission, and recovery still needs native message queries. Using direct HTTP
keeps one transport for submission and reconciliation while reusing Neige's
Planner Harness. The provider boundary leaves ACP as a later transport option.

See the [OpenCode ACP documentation](https://opencode.ai/docs/acp/) and the pinned
[ACP implementation](https://github.com/anomalyco/opencode/blob/aec0b9a6d8898f68f923aaf08b7306d931fd9d76/packages/opencode/src/acp/service.ts).

## Upstream rebase and development databases

Provider migrations 0152 and 0153 follow upstream migration 0151. The upstream
migration files are unchanged. Earlier fork-only development databases used
0130–0131 or 0148–0149 for these different migrations; their SQLx checksums conflict with
upstream's migration history. Preserve those databases and use a fresh isolated
Neige database for this rebased development build. Do not rewrite a migration
ledger. This boundary does not change native OpenCode session history.
