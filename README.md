# switchboard

A cross-harness live handoff bus and durable session memory for coding agents.

Relays tail the transcript files that agents already write (Claude Code, Codex, and
anything else that appends JSONL) and stream them into one shared event store. From there
you can ask, from any machine, what every agent session has been doing, hand context from
one session to the next, and coordinate work across sessions with a small task queue.

It is a single stdlib-only Python file plus a verb registry. No framework, no external
dependencies, one SQLite database. It also speaks **MCP**, so agents can recall past work
in-loop, and can **score and distill** its own sessions with a local model.

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
switchboard search <query>   # keyword search over learnings + session events
switchboard learnings        # list distilled, promoted learnings
switchboard eval             # score sessions with a local model, promote good ones to learnings
switchboard mcp              # run an MCP server (stdio) exposing recall to any agent
switchboard hook <event>     # ingest one harness hook event from stdin (live capture)
switchboard hooks install    # wire switchboard's capture hooks into a harness settings file
switchboard task-create      # enqueue a task for a worker to lease
switchboard task-lease       # lease the next task (multi-agent coordination)
switchboard worker           # run a task worker
switchboard systemd-generate # write systemd units for the receiver + relay
```

`switchboard --help` lists them all.

## Recall (MCP)

Point any MCP-capable agent at switchboard and it can recall what every past session did,
in-loop, before starting related work. Add it to a client's MCP config as an stdio server:

```json
{
  "mcpServers": {
    "switchboard": {
      "command": "python3",
      "args": ["/path/to/switchboard.py", "mcp", "--url", "http://<receiver-host>:17888"]
    }
  }
}
```

Drop `--url` to read the local SQLite database directly; with it, an unreachable receiver
falls back to local. Tools exposed: `switchboard_search`, `switchboard_digest`,
`switchboard_sessions`, `switchboard_latest`, `switchboard_transcript`.

## Learning layer

`switchboard eval` reviews recent sessions and — with `--promote` — distills the good ones
into **learnings**: short, titled notes that `switchboard search` and the MCP `search` tool
surface *above* raw events (also mirrored to markdown).

It follows the cheap-gate/expensive-work pattern: a **decision** step (should this session be
saved at all?) runs first and cheaply; only sessions that pass get an **expensive synthesis**
call to write the note. Promotion also has to clear three gates — the decision itself, an
objective outcome check (a session whose last exit code was non-zero is not promoted), and
**non-duplication** (a near-identical existing learning blocks it).

The decision is *pluggable*. By default the same generative model makes it, but point
`--decision-url` at a **non-generative typed-decision endpoint** speaking TypeSafe's
`/v1/systemone` API and the gate becomes a calibrated, split-second classifier while the
generative model is used only to write notes for the winners. That endpoint can be hosted
(the [LangSmith Gateway](https://docs.langchain.com/langsmith/llm-gateway-decision-models),
serving models like `semif-qwen3.5-4b` or `typesafe/jev-*`) or self-hosted
([Ollaya](https://github.com/ollaya-dev/ollaya) serving a Rev/Laya head).

```bash
export SWITCHBOARD_EVAL_BASE_URL=http://localhost:8080/v1   # generative model: note synthesis (+ decision fallback)
# optional: a fast typed-decision model as the gate (hosted example)
export SWITCHBOARD_DECISION_URL=https://gateway.smith.langchain.com
export SWITCHBOARD_DECISION_MODEL=semif-qwen3.5-4b
export SWITCHBOARD_DECISION_TOKEN=$LANGSMITH_API_KEY
switchboard eval --workspace acme --limit 10 --promote --threshold 7
switchboard search "connection pool exhaustion"
```

## Live capture (hooks)

Relays tail transcripts *after the fact*, which misses approval/permission decisions, is a
few seconds behind, and captures nothing if a session is killed before it flushes. For
harnesses that support hooks, switchboard can also capture **live**:

```bash
# wire capture hooks into Claude Code (idempotent; --dry-run to preview, uninstall to remove)
switchboard hooks install --harness claude --url http://<receiver-host>:17888
```

Each hook runs `switchboard hook <event>`, which reads the hook payload on stdin, records a
`collection_method=hook` event (tool calls, prompts, and allow/deny approval decisions), and
prints a pass response — switchboard **observes, it never gates** a tool call. When both the
tail and the hook capture the same action, they are de-duplicated (by tool-call id, else a
short cross-path time window), keeping the higher-fidelity hook row. Every event carries
`collection_method` (poll/hook) and `fidelity` so you can tell an observed action from an
inferred one.

## Privacy

Ingest scrubs secrets (bearer tokens, API keys, `password:`/`token=` values, `sk-…` keys)
from stored text before writing — on by default, and a `sha256` + true byte length of the
*original* is kept so truncation and duplicates are still detectable. Retention is a knob:

- `SWITCHBOARD_RETENTION=full` (default) — store secret-redacted text.
- `SWITCHBOARD_RETENTION=redacted` — force redaction on even if `SWITCHBOARD_REDACT=0`.
- `SWITCHBOARD_RETENTION=metadata_only` — keep actions, models, tool/file names, paths and
  provenance, but no message text.

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
| `SWITCHBOARD_EVAL_BASE_URL` | unset — OpenAI-compatible endpoint for `switchboard eval` (e.g. a local model) |
| `SWITCHBOARD_EVAL_MODEL` | `local` — the synthesis model name |
| `SWITCHBOARD_DECISION_URL` | unset — a TypeSafe `/v1/systemone` typed-decision endpoint used as the promotion gate; falls back to the generative model when unset |
| `SWITCHBOARD_DECISION_MODEL` | `semif-qwen3.5-4b` — decision model name at that endpoint |
| `SWITCHBOARD_DECISION_KIND` | `systemone` — endpoint contract; set `classify` for a classifier.dev-style `/v1/classify` (its free tier fronts Jev, no key) |
| `SWITCHBOARD_DECISION_TOKEN` | unset — bearer token for the decision endpoint (e.g. a LangSmith API key) |
| `SWITCHBOARD_EVAL_API_KEY` | unset — sent as a bearer token to the eval endpoint if set |
| `SWITCHBOARD_NOTES_DIR` | `~/.switchboard/notes` — where promoted learnings are mirrored as markdown |
| `SWITCHBOARD_REDACT` | `1` — scrub secrets from stored text at ingest; set `0`/`false` to disable |
| `SWITCHBOARD_RETENTION` | `full` — `full` / `redacted` / `metadata_only` (see Privacy) |
| `SWITCHBOARD_NO_FTS5` | unset — force the `LIKE` search path even when SQLite has FTS5 |

## Notes

This is the shareable core of a larger personal multi-agent system, extracted and
genericized: the live bus, the session/transcript store, and the task queue. The
project-specific automation on top of it (a Linear→PR loop, review gates, notification
relays) is intentionally left out.

## License

MIT — see [LICENSE](LICENSE).
