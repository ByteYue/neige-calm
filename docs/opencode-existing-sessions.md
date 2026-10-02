# Existing OpenCode sessions

## Outcome

Connect an existing native OpenCode session to a Neige track, read its original
history and changing tool output, and send later operational questions to the
same session. Opening, refreshing and restarting Neige must not send a prompt.
The existing managed Planner provider remains a separate creation path.

## Setup

Run OpenCode **1.18.34** as an authenticated loopback server in the original
session directory, using the same user's original profile. Set its native
`OPENCODE_SERVER_PASSWORD` and store that password in a private file with mode
`0600`. For example, the server command is:

```bash
cd /home/owner/project
/absolute/path/opencode serve --hostname 127.0.0.1 --port 4096 --mdns=false
```

The command expects the password to be set in its environment. Do not publish
that password or the model-provider credentials. Register the connection in a
Neige configuration file:

```json
{
  "connections": [
    {
      "id": "operations",
      "label": "Operations OpenCode",
      "directory": "/home/owner/project",
      "generation": 1,
      "port": 4096,
      "password_file": "/home/owner/.config/neige/opencode-password"
    }
  ]
}
```

Start Neige with `--opencode-connections-config /absolute/path/connections.json`.
Connection passwords stay on the server; the browser receives only the
registration ID, label and directory. Increment the generation when changing
the connection's endpoint or execution context. Existing bindings do not
silently follow that change.

Open a track's conversation panel, choose **Connect OpenCode session**, select
the registered connection and enter the original `ses_...` ID. The first view
loads existing history. Send a new question to perform a fresh progress check.
For terminal access to the same running server, use native `opencode attach`
with the original directory and session ID rather than starting a separate
execution instance.

## Connection and authority

An operator registers an authenticated loopback OpenCode server, its original
directory and a connection generation. The browser selects a registration and
supplies a native session ID; it never supplies a URL, password or profile path.
The server verifies the native identity, directory and supported version before
binding it. Credentials remain in the operator's configuration.

The attachment uses the existing no-MCP PlainChat role. It does not replace the
native agent, system instructions, model, permissions or MCP configuration with
Neige Planner policy. Its native directory is shown explicitly and can differ
from the track's workspace: the track groups the conversation, while the
operator's connection registration declares its execution directory.

Neige observes an external turn without adopting it as a Neige submission. A
foreign running turn blocks new input. Native status belongs to the connected
server's memory; another process sharing the database does not share that turn.
Multiple clients do not have a native exclusive-write lease. Use one input
client when continuing the session from Neige.

## Persistence and lifecycle

Persist the registered connection, generation and original session identity.
Reuse native message and part IDs to reconcile history and progress. A failed
connection does not create a replacement native session. Changes to the
registered connection do not silently repoint an existing binding.

Only new messages explicitly sent from Neige use its durable submission journal.
Unknown admission is reconciled without automatic retransmission. Closing or
deleting the attachment, restarting Neige or losing its connection does not
abort the external turn, reject its pending interactive requests, stop its
server or tool processes, or delete its native history.

An ordinary OpenCode TUI may have no externally reachable HTTP listener. Such a
running turn cannot be transferred by loading its session ID in a second
process. For shared access, run an authenticated loopback server with the
original profile and directory and connect both clients to it. Move an existing
TUI conversation only after its current turn has settled; do not copy its live
database into Neige's private Planner profile.

## Acceptance

- Connect through the real Neige UI and see pre-existing native history with no
  native prompt POST during attachment, refresh or Neige restart.
- Explicitly send two read-only progress questions separated by a Neige restart;
  both use the same original native session, with one native POST per question.
- Attach during an external turn, see later tool output, and keep that turn and
  server alive after the Neige attachment is removed or Neige stops.
- Exercise missing sessions, directory and generation mismatches, unavailable
  servers, duplicate attachment, pagination and unknown submission admission.
- Verify the no-abort/no-retransmit assertions by single-factor mutations of
  production code in an exclusive worktree.
- Capture the real attachment form, original history and continued response for
  the pull request, with secrets and unrelated session content excluded.
