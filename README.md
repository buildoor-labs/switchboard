# switchboard

A cross-harness live handoff bus and durable session memory for coding agents.

Relays tail the transcript files that agents already write (Claude Code, Codex, and
anything else that appends JSONL) and stream them into one shared event store. From there
you can ask, from any machine, what every agent session has been doing, hand context from
one session to the next, and coordinate work across sessions with a small task queue.

It is a single stdlib-only Python file plus a verb registry. No framework, no external
dependencies, one SQLite database.

## Why

Agents forget across sessions, and worse, they forget across *harnesses* and *machines* —
what Codex did on the VM is invisible to Claude Code on your laptop. This puts all of it in
one place, keyed by workspace, so a new session can pick up where any other one left off.

## Commands

```
switchboard serve            # run the receiver (HTTP + SQLite event store)
switchboard relay <paths>    # tail agent transcript files and POST events to the receiver
switchboard latest           # most recent events, filterable by workspace / harness / machine
switchboard sessions         # recent sessions across every harness
switchboard digest           # a compact cross-harness summary
switchboard transcript <id>  # replay one session
switchboard task-create      # enqueue a task for a worker to lease
switchboard task-lease        # lease the next task (multi-agent coordination)
switchboard worker            # run a task worker
switchboard systemd-generate # write systemd units for the receiver + relay
```

`switchboard --help` lists them all.

## Quickstart

```bash
# 1. run the receiver
python3 switchboard.py serve            # listens on 127.0.0.1:17888

# 2. from each machine, relay your agent transcripts into it
python3 switchboard.py relay ~/.claude/projects ~/.codex/sessions \
    --url http://<receiver-host>:17888

# 3. ask what's been happening, anywhere
python3 switchboard.py digest --workspace code
python3 switchboard.py sessions --harness codex --limit 10
```

## Configuration

All via environment variables:

| Var | Default |
|---|---|
| `SWITCHBOARD_DB` | `~/.switchboard/events.db` |
| `SWITCHBOARD_URL` | `http://127.0.0.1:17888` |
| `SWITCHBOARD_HOST` / `SWITCHBOARD_PORT` | `127.0.0.1` / `17888` |
| `SWITCHBOARD_WORKSPACE_MAP` | `{}` — JSON mapping a cwd prefix to a workspace slug, e.g. `{"/home/you/code/acme": "acme"}` |
| `SWITCHBOARD_CMD_TOKEN` | unset — the receiver is open on localhost; set a bearer token to require auth once you expose it |

## Notes

This is the shareable core of a larger personal multi-agent system, extracted and
genericized: the live bus, the session/transcript store, and the task queue. The
project-specific automation on top of it (a Linear→PR loop, review gates, notification
relays) is intentionally left out.

## License

MIT — see [LICENSE](LICENSE).
