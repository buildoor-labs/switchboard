#!/usr/bin/env python3
"""Tiny live handoff bus for cross-switchboard events.

No dependencies. Provides:
- receiver HTTP server: POST /events, GET /latest, GET /sessions, GET /session/<id>
- relay tailer for Claude JSONL transcripts
- CLI query/ingest helpers
"""
from __future__ import annotations
import re

import argparse, hashlib, hmac, html, http.server, json, os, re, signal, sqlite3, subprocess, sys, tempfile, threading, time, urllib.error, urllib.parse, urllib.request, uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from verb_registry import VALID_COMMAND_VERBS, VERB_REGISTRY

DEFAULT_DB = os.environ.get(
    "SWITCHBOARD_DB",
    str(Path.home() / ".switchboard" / "events.db"),
)
DEFAULT_URL = os.environ.get("SWITCHBOARD_URL", "http://127.0.0.1:17888")
DEFAULT_HOST = os.environ.get("SWITCHBOARD_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("SWITCHBOARD_PORT", "17888"))
DEFAULT_CMD_LEASE_SECONDS = int(os.environ.get("SWITCHBOARD_CMD_LEASE_SECONDS", "1800"))
DEFAULT_CMD_TTL_SECONDS = int(os.environ.get("SWITCHBOARD_CMD_TTL_SECONDS", "86400"))
OUTPUT_TAIL_BYTES = 64 * 1024

VALID_COMMAND_STATUSES = {"queued", "claimed", "running", "done", "failed", "expired"}
CAPSULE_STATES = {"working", "waiting-input", "blocked", "done", "abandoned", "stale"}
AGENT_SESSION_ACTIVE_SECONDS = int(os.environ.get("SWITCHBOARD_AGENT_ACTIVE_SECONDS", "120"))
TASK_STATES = {"queued", "leased", "running", "waiting", "verifying", "done", "blocked", "failed", "cancelled"}
TASK_TERMINAL_STATES = {"done", "blocked", "failed", "cancelled"}
TASK_TRANSITIONS = {
    "queued": {"leased", "blocked", "failed", "cancelled"},
    "leased": {"running", "queued", "blocked", "failed", "cancelled"},
    "running": {"waiting", "verifying", "done", "blocked", "failed", "cancelled"},
    "waiting": {"running", "verifying", "blocked", "failed", "cancelled"},
    "verifying": {"running", "done", "blocked", "failed", "cancelled"},
    "done": set(),
    "blocked": set(),
    "failed": set(),
    "cancelled": set(),
}
HARNESS_ROUTES = {
    "claude-code": {"aliases": {"claude", "claude-code"}, "binary": "claude", "env": "SWITCHBOARD_CLAUDE_CMD"},
    "codex": {"aliases": {"codex"}, "binary": "codex", "env": "SWITCHBOARD_CODEX_CMD"},
    "omp": {"aliases": {"omp", "opencode", "qwopus"}, "binary": "omp", "env": "SWITCHBOARD_OMP_CMD"},
    "hermes": {"aliases": {"hermes"}, "binary": "hermes", "env": "SWITCHBOARD_HERMES_CMD"},
    "orca": {"aliases": {"orca"}, "binary": "orca", "env": "SWITCHBOARD_ORCA_CMD"},
    "local": {"aliases": {"local"}, "binary": "sh", "env": "SWITCHBOARD_LOCAL_CMD"},
}


class WorkerShutdown(BaseException):
    def __init__(self, signum: int):
        super().__init__(signum)
        self.signum = signum


def install_worker_signal_handlers():
    def shutdown(signum, _frame):
        raise WorkerShutdown(signum)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  received_at TEXT NOT NULL,
  source_machine TEXT NOT NULL,
  harness TEXT NOT NULL,
  workspace TEXT,
  cwd TEXT,
  session_id TEXT NOT NULL,
  role TEXT,
  event_type TEXT,
  text TEXT,
  tool_name TEXT,
  command TEXT,
  exit_code INTEGER,
  file_path TEXT,
  raw_json TEXT NOT NULL,
  source_path TEXT,
  source_line INTEGER,
  source_offset INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_ws_ts ON events(workspace, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_session_ts ON events(session_id, ts DESC);
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  source_machine TEXT NOT NULL,
  harness TEXT NOT NULL,
  workspace TEXT,
  cwd TEXT,
  started_at TEXT,
  last_seen TEXT NOT NULL,
  last_role TEXT,
  last_text TEXT,
  event_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_ws_last ON sessions(workspace, last_seen DESC);
CREATE TABLE IF NOT EXISTS workspace_map (
  prefix TEXT PRIMARY KEY,
  workspace TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS command_queue (
  id           TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  verb         TEXT NOT NULL,
  args         TEXT NOT NULL,
  harness      TEXT NOT NULL,
  model        TEXT,
  target       TEXT,
  cwd          TEXT NOT NULL,
  status       TEXT NOT NULL,
  created_by   TEXT NOT NULL,
  claimed_by   TEXT,
  exit_code    INTEGER,
  output       TEXT,
  session_id   TEXT,
  created_at   TEXT NOT NULL,
  claimed_at   TEXT,
  finished_at  TEXT,
  expires_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_command_queue_claim ON command_queue(status, harness, target, created_at);
CREATE INDEX IF NOT EXISTS idx_command_queue_expires ON command_queue(status, expires_at);
CREATE INDEX IF NOT EXISTS idx_command_queue_idempotency ON command_queue(idempotency_key);
CREATE TABLE IF NOT EXISTS command_audit (
  seq        INTEGER PRIMARY KEY,
  command_id TEXT NOT NULL,
  event      TEXT NOT NULL,
  actor      TEXT NOT NULL,
  detail     TEXT,
  at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_command_audit_command ON command_audit(command_id, seq);
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  title TEXT NOT NULL,
  description TEXT,
  state TEXT NOT NULL,
  harness TEXT,
  model TEXT,
  target TEXT,
  cwd TEXT,
  priority INTEGER NOT NULL DEFAULT 0,
  created_by TEXT NOT NULL,
  owner TEXT,
  current_attempt_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  lease_owner TEXT,
  lease_expires_at TEXT,
  metadata TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_state_priority ON tasks(state, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_route ON tasks(harness, target, state, created_at);
CREATE TABLE IF NOT EXISTS task_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  lease_owner TEXT NOT NULL,
  harness TEXT,
  model TEXT,
  target TEXT,
  session_id TEXT,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  started_at TEXT,
  heartbeat_at TEXT,
  lease_expires_at TEXT,
  finished_at TEXT,
  metadata TEXT,
  UNIQUE(task_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_task_attempts_task ON task_attempts(task_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_task_attempts_lease ON task_attempts(lease_owner, lease_expires_at);
CREATE TABLE IF NOT EXISTS task_events (
  seq INTEGER PRIMARY KEY,
  event_id TEXT UNIQUE,
  task_id TEXT NOT NULL,
  attempt_id TEXT,
  event_type TEXT NOT NULL,
  actor TEXT NOT NULL,
  detail TEXT,
  at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, seq);
CREATE TABLE IF NOT EXISTS task_artifacts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  attempt_id TEXT,
  kind TEXT NOT NULL,
  uri TEXT NOT NULL,
  name TEXT,
  metadata TEXT,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_artifacts_task ON task_artifacts(task_id, created_at);
CREATE TABLE IF NOT EXISTS task_decisions (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  question TEXT NOT NULL,
  options TEXT,
  status TEXT NOT NULL,
  resolution TEXT,
  created_by TEXT NOT NULL,
  decided_by TEXT,
  created_at TEXT NOT NULL,
  decided_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_decisions_task ON task_decisions(task_id, status);
CREATE TABLE IF NOT EXISTS agent_sessions (
  session_id TEXT PRIMARY KEY,
  worker TEXT NOT NULL,
  harness TEXT NOT NULL,
  model TEXT,
  target TEXT,
  cwd TEXT,
  capabilities TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  metadata TEXT
);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_worker ON agent_sessions(worker, heartbeat_at);
CREATE TABLE IF NOT EXISTS session_status (
  session_id TEXT PRIMARY KEY,
  goal TEXT,
  now TEXT,
  state TEXT NOT NULL,
  summary TEXT,
  branch_points TEXT,
  decisions TEXT,
  question TEXT,
  suggested_reply TEXT,
  last_event_ts TEXT,
  summarized_event_count INTEGER NOT NULL DEFAULT 0,
  state_changed_at TEXT,
  notified_waiting_at TEXT,
  danger_alert_at TEXT,
  danger_flag TEXT,
  harvested_at TEXT,
  judge TEXT,
  error TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_status_state ON session_status(state, updated_at DESC);
CREATE TABLE IF NOT EXISTS session_links (
  session_id TEXT PRIMARY KEY,
  terminal_handle TEXT,
  worktree_id TEXT,
  slug TEXT,
  doc TEXT,
  parent_session TEXT,
  source TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_links_slug ON session_links(slug);
"""

# Map an absolute working-directory prefix to a workspace slug (longest prefix wins).
# Configure for your machines via SWITCHBOARD_WORKSPACE_MAP (a JSON object), e.g.
#   {"/home/you/code/acme": "acme"}
DEFAULT_MAP: dict[str, str] = json.loads(os.environ.get("SWITCHBOARD_WORKSPACE_MAP", "{}"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect(db: str = DEFAULT_DB) -> sqlite3.Connection:
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    # Existing receivers predate command idempotency. Add columns referenced by
    # new indexes before executescript attempts to create those indexes.
    legacy_tables = {
        row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if "command_queue" in legacy_tables:
        command_columns = {
            row[1] for row in con.execute("PRAGMA table_info(command_queue)")
        }
        if "idempotency_key" not in command_columns:
            con.execute("ALTER TABLE command_queue ADD COLUMN idempotency_key TEXT")
    con.executescript(SCHEMA)
    for col, typ in [("source_path", "TEXT"), ("source_line", "INTEGER"), ("source_offset", "INTEGER")]:
        try:
            con.execute(f"ALTER TABLE events ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    for table, col, typ in [
        ("command_queue", "idempotency_key", "TEXT"),
        ("tasks", "metadata", "TEXT"),
        ("agent_sessions", "metadata", "TEXT"),
        ("session_status", "danger_flag", "TEXT"),
    ]:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    for prefix, ws in DEFAULT_MAP.items():
        con.execute("INSERT OR IGNORE INTO workspace_map(prefix, workspace) VALUES (?,?)", (prefix, ws))
    con.commit()
    return con


def parse_iso_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def expires_after(seconds: int | float) -> str:
    return datetime.fromtimestamp(time.time() + float(seconds), timezone.utc).isoformat().replace("+00:00", "Z")


def json_dumps_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def canonical_harness(harness: str | None) -> str | None:
    if not harness:
        return None
    h = str(harness).strip().lower()
    for canonical, spec in HARNESS_ROUTES.items():
        if h == canonical or h in spec["aliases"]:
            return canonical
    return h


def route_matches(requested: str, offered: list[str]) -> bool:
    return canonical_harness(requested) in {canonical_harness(h) for h in offered}


def canonical_workspace(con: sqlite3.Connection, cwd: str | None, explicit: str | None = None) -> str | None:
    if explicit:
        return explicit
    if not cwd:
        return None
    rows = con.execute("SELECT prefix, workspace FROM workspace_map ORDER BY length(prefix) DESC").fetchall()
    for r in rows:
        if cwd == r["prefix"] or cwd.startswith(r["prefix"].rstrip("/") + "/"):
            return r["workspace"]
    # Claude project dirs are dash-escaped absolute paths, e.g. -Users-<user>-code.
    if re.search(r"-(?:Users|home)-[^-]+-code", cwd):
        return "code"
    return Path(cwd).name or None


def text_from_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                typ = item.get("type")
                if typ == "text" and item.get("text"):
                    parts.append(str(item["text"]))
                elif typ in {"tool_use", "toolCall"}:
                    name = item.get("name", "")
                    inp = item.get("input", item.get("arguments", {}))
                    parts.append(f"[tool_use {name}] {json.dumps(inp, ensure_ascii=False)[:1000]}")
                elif typ in {"tool_result", "toolResult"}:
                    val = item.get("content", item.get("text", ""))
                    parts.append(f"[tool_result] {text_from_content(val)[:1000]}")
            else:
                parts.append(str(item))
        return "\n".join([p for p in parts if p])
    if content is None:
        return ""
    return str(content)


def infer_cwd_from_transcript_path(file_path: str | None, source_machine: str) -> str | None:
    if not file_path:
        return None
    p = Path(file_path)
    parts = p.parts
    home = str(Path.home())
    if "sessions" in parts:
        try:
            slug = parts[parts.index("sessions") + 1]
        except Exception:
            slug = ""
        # OMP stores cwd-ish slugs such as -code-myproject.
        if slug.startswith("-"):
            rest = slug[1:].replace("-", "/")
            if rest.startswith("code"):
                return f"{home}/{rest}"
            if rest.startswith("home/") or rest.startswith("Users/"):
                return "/" + rest
    # Claude project dirs use dash-escaped absolute paths.
    for part in parts:
        if re.match(r"^-(?:Users|home)-[^-]+-", part):
            return "/" + part[1:].replace("-", "/")
    return None


def session_id_from_path(file_path: str | None) -> str:
    if not file_path:
        return "unknown"
    p = Path(file_path)
    m = re.search(r"_([0-9a-f]{8,}(?:-[0-9a-f]{4,}){2,})", p.stem, re.I)
    if m:
        return m.group(1).rstrip(".")
    m = re.search(r"([0-9a-f]{8,}(?:-[0-9a-f]{4,}){2,})", p.name, re.I)
    if m:
        return m.group(1).rstrip(".")
    return p.stem or "unknown"


def extract_path(obj: Any, dotted: str) -> Any:
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def normalize_claude_line(obj: dict[str, Any], source_machine: str, fallback_harness="claude-code", source_path: str | None = None) -> dict[str, Any] | None:
    # Handles Claude, OMP/Pi, Codex, Hermes, Cursor Agent stream JSON, and simple JSONL-like agent logs.
    ts = obj.get("timestamp") or obj.get("created_at") or obj.get("time") or now_iso()
    if isinstance(ts, dict):
        # opencode-style {"created": <ms>, ...}
        ms = ts.get("created") or ts.get("start") or ts.get("updated")
        ts = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat() if ms else now_iso()
    sid = obj.get("sessionId") or obj.get("session_id") or obj.get("sessionID") or obj.get("conversationId") or obj.get("chatId")
    if not sid and obj.get("type") == "session":
        sid = obj.get("id")
    if not sid and fallback_harness in {"omp", "pi"}:
        sid = session_id_from_path(source_path)
    if not sid:
        sid = obj.get("id")
    cwd = obj.get("cwd") or obj.get("tool_cwd") or extract_path(obj, "payload.cwd") or extract_path(obj, "payload.metadata.cwd") or infer_cwd_from_transcript_path(source_path, source_machine)
    if not cwd and fallback_harness == "hermes":
        cwd = str(Path.home() / "code")
    role = None
    text = ""
    event_type = obj.get("type") or obj.get("event") or "event"
    tool_name = None
    command = None
    exit_code = None
    file_path = None

    # Codex JSONL: {type, payload:{...}}
    if "payload" in obj and isinstance(obj.get("payload"), dict):
        payload = obj["payload"]
        if not sid:
            sid = payload.get("id") or payload.get("session_id") or payload.get("sessionId") or payload.get("turn_id") or session_id_from_path(source_path)
        if obj.get("type") == "session_meta":
            role = "system"; event_type = "session_meta"; cwd = payload.get("cwd") or cwd
            text = f"session {sid} cwd={cwd} source={payload.get('source')} model={payload.get('model')}"
        elif obj.get("type") == "turn_context":
            role = "system"; event_type = "turn_context"; cwd = payload.get("cwd") or cwd
            text = f"turn_context cwd={cwd} model={payload.get('model')} approval={payload.get('approval_policy')}"
        elif obj.get("type") == "event_msg":
            role = "event"; event_type = payload.get("type") or "event_msg"; text = json.dumps(payload, ensure_ascii=False)[:4000]
        elif obj.get("type") == "response_item":
            role = payload.get("role") or payload.get("type") or "event"
            event_type = payload.get("type") or "response_item"
            text = text_from_content(payload.get("content") or payload.get("message") or payload.get("text") or payload)[:4000]
            if payload.get("type") in {"function_call", "custom_tool_call", "exec_command"}:
                event_type = "tool_use"; role = "tool"; tool_name = payload.get("name") or payload.get("type"); command = payload.get("command")
            elif payload.get("type") in {"function_call_output", "custom_tool_call_output", "exec_command_output"}:
                event_type = "tool_result"; role = "tool"; text = text_from_content(payload.get("output") or payload)[:4000]
        else:
            role = payload.get("role") or "event"
            text = text_from_content(payload.get("message") or payload.get("content") or payload.get("text") or payload)[:4000]

    elif obj.get("type") in {"user", "assistant", "message"} and isinstance(obj.get("message"), dict):
        msg = obj["message"]
        role = msg.get("role") or obj.get("type")
        text = text_from_content(msg.get("content"))
        tool_name = msg.get("toolName") or msg.get("tool_name")
        if role in {"toolResult", "tool_result"}:
            event_type = "tool_result"
        if isinstance(msg.get("content"), list):
            for item in msg["content"]:
                if isinstance(item, dict) and item.get("type") in {"toolCall", "tool_use"}:
                    event_type = "tool_use"
                    tool_name = item.get("name") or tool_name
                    inp = item.get("arguments") or item.get("input") or {}
                    command = inp.get("command") if isinstance(inp, dict) else None
                    file_path = inp.get("file_path") or inp.get("path") if isinstance(inp, dict) else None
                    break
    elif obj.get("type") == "tool_use":
        role = "tool"
        tool_name = obj.get("name") or obj.get("toolName")
        event_type = "tool_use"
        inp = obj.get("input") or {}
        command = inp.get("command") if isinstance(inp, dict) else None
        file_path = inp.get("file_path") or inp.get("path") if isinstance(inp, dict) else None
        text = json.dumps(inp, ensure_ascii=False)[:4000]
    elif obj.get("type") == "tool_result":
        role = "tool"
        event_type = "tool_result"
        exit_code = obj.get("exit_code") or obj.get("exitCode")
        text = text_from_content(obj.get("content") or obj.get("result"))[:4000]
    elif isinstance(obj.get("sessionID"), str) and str(obj.get("id", "")).startswith(("msg_", "prt_")):
        # opencode storage documents: one JSON file per message/part.
        role = obj.get("role") or "event"
        event_type = obj.get("type") or ("message" if obj.get("role") else "part")
        summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
        text = obj.get("text") or summary.get("title") or ""
        tool_name = obj.get("tool")
        if obj.get("type") == "tool":
            event_type = "tool_use"
            state = obj.get("state") if isinstance(obj.get("state"), dict) else {}
            text = text or json.dumps(state.get("input", {}), ensure_ascii=False)[:1000]
    elif obj.get("role") in {"user", "assistant", "tool", "system"}:
        # Hermes JSONL and similar OpenAI-chat-shaped logs.
        role = obj.get("role")
        event_type = obj.get("event_type") or role
        text = text_from_content(obj.get("content") or obj.get("message") or obj.get("text") or obj.get("tool_calls"))[:4000]
        if obj.get("tool_call_id"):
            event_type = "tool_result"
        if obj.get("tool_calls"):
            event_type = "tool_use"
            try:
                tc = obj["tool_calls"][0]
                tool_name = extract_path(tc, "function.name") or tc.get("name")
                command = extract_path(tc, "function.arguments")
            except Exception:
                pass
    elif obj.get("type") == "queue-operation":
        # Useful because claude-mem observer sessions contain observed_from_primary_session.
        role = "system"
        event_type = "queue_operation"
        content = obj.get("content") or ""
        m = re.search(r"<observed_from_primary_session>(.*?)</observed_from_primary_session>", content, re.S)
        if m:
            text = re.sub(r"\s+", " ", m.group(1)).strip()
        else:
            text = f"queue {obj.get('operation','')}"
    elif obj.get("attachment"):
        role = "system"
        event_type = "attachment"
        text = json.dumps(obj.get("attachment"), ensure_ascii=False)[:1000]
    else:
        role = obj.get("role") or "event"
        text = text_from_content(obj.get("content") or obj.get("text") or obj)[:4000]

    if not sid:
        sid = session_id_from_path(source_path)
    if not text and event_type not in {"tool_use", "tool_result"}:
        return None
    base = {
        "ts": ts,
        "source_machine": source_machine,
        "harness": obj.get("harness") or fallback_harness,
        "workspace": obj.get("workspace"),
        "cwd": cwd,
        "session_id": str(sid),
        "role": role,
        "event_type": event_type,
        "text": text[:8000],
        "tool_name": tool_name,
        "command": command,
        "exit_code": exit_code,
        "file_path": file_path,
        "raw": obj,
        "source_path": source_path,
        "source_line": obj.get("_source_line"),
        "source_offset": obj.get("_source_offset"),
    }
    key_src = json.dumps([source_machine, base["harness"], sid, obj.get("uuid"), obj.get("promptId"), ts, role, event_type, text[:200]], sort_keys=True, ensure_ascii=False)
    base["event_id"] = obj.get("uuid") or hashlib.sha256(key_src.encode()).hexdigest()[:32]
    return base


def ingest_events(con: sqlite3.Connection, events: Iterable[dict[str, Any]]) -> tuple[int, int]:
    inserted = 0; skipped = 0
    for e in events:
        if not e: continue
        ws = canonical_workspace(con, e.get("cwd"), e.get("workspace"))
        raw_json = json.dumps(e.get("raw", e), ensure_ascii=False, sort_keys=True)[:65536]
        try:
            con.execute(
                """INSERT INTO events(event_id, ts, received_at, source_machine, harness, workspace, cwd, session_id, role, event_type, text, tool_name, command, exit_code, file_path, raw_json, source_path, source_line, source_offset)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (e["event_id"], e.get("ts") or now_iso(), now_iso(), e.get("source_machine") or "unknown", e.get("harness") or "unknown", ws, e.get("cwd"), e["session_id"], e.get("role"), e.get("event_type"), (e.get("text") or "")[:8000], e.get("tool_name"), e.get("command"), e.get("exit_code"), e.get("file_path"), raw_json, e.get("source_path"), e.get("source_line"), e.get("source_offset")),
            )
            inserted += 1
            con.execute(
                """INSERT INTO sessions(session_id, source_machine, harness, workspace, cwd, started_at, last_seen, last_role, last_text, event_count)
                   VALUES (?,?,?,?,?,?,?,?,?,1)
                   ON CONFLICT(session_id) DO UPDATE SET
                     last_seen=excluded.last_seen,
                     last_role=excluded.last_role,
                     last_text=excluded.last_text,
                     workspace=COALESCE(excluded.workspace, sessions.workspace),
                     cwd=COALESCE(excluded.cwd, sessions.cwd),
                     event_count=event_count+1""",
                (e["session_id"], e.get("source_machine") or "unknown", e.get("harness") or "unknown", ws, e.get("cwd"), e.get("ts") or now_iso(), e.get("ts") or now_iso(), e.get("role"), (e.get("text") or "")[:1000]),
            )
        except sqlite3.IntegrityError:
            skipped += 1
    con.commit()
    return inserted, skipped


def rowdict(r: sqlite3.Row) -> dict[str, Any]: return {k: r[k] for k in r.keys()}


def command_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    try:
        d["args"] = json.loads(d.get("args") or "{}")
    except Exception:
        pass
    return d


def decode_json_field(d: dict[str, Any], name: str, fallback: Any) -> None:
    try:
        d[name] = json.loads(d.get(name) or "")
    except Exception:
        d[name] = fallback


def task_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "metadata", {})
    return d


def attempt_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "metadata", {})
    return d


def artifact_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "metadata", {})
    return d


def decision_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "options", [])
    return d


def agent_session_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "capabilities", [])
    decode_json_field(d, "metadata", {})
    heartbeat = parse_iso_seconds(d.get("heartbeat_at"))
    d["active"] = bool(
        d.get("status") != "offline"
        and heartbeat is not None
        and heartbeat >= time.time() - AGENT_SESSION_ACTIVE_SECONDS
    )
    d["effective_status"] = d.get("status") if d["active"] or d.get("status") == "offline" else "stale"
    return d


def append_task_event(con: sqlite3.Connection, task_id: str, attempt_id: str | None, event_type: str, actor: str, detail: Any = None, event_id: str | None = None) -> None:
    if detail is not None and not isinstance(detail, str):
        detail = json_dumps_compact(detail)
    con.execute(
        """INSERT OR IGNORE INTO task_events(seq, event_id, task_id, attempt_id, event_type, actor, detail, at)
           SELECT COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ?, ?, ? FROM task_events""",
        (event_id or str(uuid.uuid4()), task_id, attempt_id, event_type, actor, detail, now_iso()),
    )


def create_task(con: sqlite3.Connection, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    title = str(payload.get("title") or payload.get("name") or payload.get("summary") or "").strip()
    if not title:
        raise ValueError("title is required")
    state = str(payload.get("state") or "queued")
    if state != "queued":
        raise ValueError("new tasks must start queued")
    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")
    idempotency_key = payload.get("idempotency_key")
    if idempotency_key:
        row = con.execute("SELECT * FROM tasks WHERE idempotency_key=?", (str(idempotency_key),)).fetchone()
        if row:
            return task_rowdict(row)
    task_id = str(payload.get("id") or uuid.uuid4())
    now = now_iso()
    try:
        con.execute(
            """INSERT INTO tasks(id, idempotency_key, title, description, state, harness, model, target, cwd, priority, created_by, created_at, updated_at, metadata)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                task_id,
                str(idempotency_key) if idempotency_key else None,
                title,
                payload.get("description"),
                "queued",
                canonical_harness(payload.get("harness")),
                payload.get("model"),
                payload.get("target"),
                payload.get("cwd"),
                int(payload.get("priority") or 0),
                payload.get("created_by") or actor or "cli",
                now,
                now,
                json_dumps_compact(metadata),
            ),
        )
    except sqlite3.IntegrityError:
        if idempotency_key:
            row = con.execute("SELECT * FROM tasks WHERE idempotency_key=?", (str(idempotency_key),)).fetchone()
            if row:
                return task_rowdict(row)
        raise
    append_task_event(con, task_id, None, "task_created", actor, {"state": "queued", "harness": canonical_harness(payload.get("harness"))}, payload.get("event_id"))
    con.commit()
    row = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    return task_rowdict(row)


def reap_stale_task_leases(con: sqlite3.Connection, actor: str = "reaper") -> int:
    now = now_iso()
    rows = con.execute(
        """SELECT * FROM tasks
           WHERE state IN ('leased','running','waiting','verifying')
             AND lease_expires_at IS NOT NULL
             AND lease_expires_at < ?""",
        (now,),
    ).fetchall()
    reaped = 0
    for row in rows:
        attempt_id = row["current_attempt_id"]
        con.execute(
            """UPDATE tasks
               SET state='queued', owner=NULL, lease_owner=NULL, lease_expires_at=NULL, current_attempt_id=NULL, updated_at=?
               WHERE id=? AND current_attempt_id=?""",
            (now, row["id"], attempt_id),
        )
        con.execute(
            "UPDATE task_attempts SET state='failed', finished_at=? WHERE id=? AND state NOT IN ('done','blocked','failed','cancelled')",
            (now, attempt_id),
        )
        append_task_event(con, row["id"], attempt_id, "lease_expired", actor, {"lease_owner": row["lease_owner"]})
        reaped += 1
    if reaped:
        con.commit()
    return reaped


def list_tasks(con: sqlite3.Connection, filters: dict[str, str | None], limit: int = 100) -> list[dict[str, Any]]:
    reap_stale_task_leases(con, actor="list-tasks")
    limit = max(1, min(int(limit), 500))
    where = []
    args: list[Any] = []
    for col in ("state", "harness", "target", "owner"):
        val = filters.get(col)
        if val:
            where.append(f"{col}=?")
            args.append(canonical_harness(val) if col == "harness" else val)
    sql = "SELECT * FROM tasks"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY priority DESC, created_at ASC LIMIT ?"
    args.append(limit)
    return [task_rowdict(r) for r in con.execute(sql, args).fetchall()]


def get_task(con: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    reap_stale_task_leases(con, actor="get-task")
    row = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    return task_rowdict(row) if row else None


def lease_task(con: sqlite3.Connection, payload: dict[str, Any], actor: str) -> dict[str, Any] | None:
    worker = str(payload.get("worker") or actor or "").strip()
    if not worker:
        raise ValueError("worker is required")
    harnesses = [canonical_harness(h) for h in payload.get("harnesses", []) if canonical_harness(h)]
    if not harnesses and payload.get("harness"):
        harnesses = [canonical_harness(payload.get("harness")) or str(payload.get("harness"))]
    if not harnesses:
        raise ValueError("harnesses is required")
    target = payload.get("target")
    requested_task_id = payload.get("task_id")
    lease_seconds = int(payload.get("lease_seconds") or DEFAULT_CMD_LEASE_SECONDS)
    reap_stale_task_leases(con, actor="lease-task")
    now = now_iso()
    lease_expires = expires_after(lease_seconds)
    placeholders = ",".join(["?"] * len(harnesses))
    query_args: list[Any] = [*harnesses]
    task_sql = ""
    if requested_task_id:
        task_sql = "AND id=?"
        query_args.append(str(requested_task_id))
    target_sql = ""
    if target:
        target_sql = "AND (target IS NULL OR target=?)"
        query_args.append(target)
    row = con.execute(
        f"""SELECT * FROM tasks
            WHERE state='queued'
              AND (harness IS NULL OR harness IN ({placeholders}))
              {task_sql}
              {target_sql}
            ORDER BY priority DESC, created_at ASC
            LIMIT 1""",
        query_args,
    ).fetchone()
    if not row:
        return None
    task_id = row["id"]
    attempt_no = int(con.execute("SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM task_attempts WHERE task_id=?", (task_id,)).fetchone()[0])
    attempt_id = str(uuid.uuid4())
    selected_harness = row["harness"] or harnesses[0]
    con.execute(
        """INSERT INTO task_attempts(id, task_id, attempt_no, lease_owner, harness, model, target, state, created_at, heartbeat_at, lease_expires_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (attempt_id, task_id, attempt_no, worker, selected_harness, row["model"], row["target"], "leased", now, now, lease_expires),
    )
    cur = con.execute(
        """UPDATE tasks
           SET state='leased', owner=?, lease_owner=?, lease_expires_at=?, current_attempt_id=?, updated_at=?
           WHERE id=? AND state='queued'""",
        (worker, worker, lease_expires, attempt_id, now, task_id),
    )
    if cur.rowcount != 1:
        con.rollback()
        return None
    append_task_event(con, task_id, attempt_id, "task_leased", worker, {"lease_seconds": lease_seconds, "harness": selected_harness})
    con.commit()
    task = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    attempt = con.execute("SELECT * FROM task_attempts WHERE id=?", (attempt_id,)).fetchone()
    return {"task": task_rowdict(task), "attempt": attempt_rowdict(attempt)}


def require_task_owner(task: sqlite3.Row, attempt: sqlite3.Row | None, actor: str, attempt_id: str | None) -> None:
    if task["state"] in TASK_TERMINAL_STATES:
        raise ValueError(f"task is terminal: {task['state']}")
    if task["current_attempt_id"]:
        if not attempt_id:
            raise PermissionError("attempt_id is required")
        if attempt_id != task["current_attempt_id"]:
            raise PermissionError("stale attempt")
        if not attempt:
            raise PermissionError("attempt not found")
        if actor != attempt["lease_owner"]:
            raise PermissionError("wrong owner")
        expires = parse_iso_seconds(attempt["lease_expires_at"])
        if expires is not None and expires < time.time():
            raise PermissionError("lease expired")


def update_task_state(con: sqlite3.Connection, task_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    state = str(payload.get("state") or payload.get("status") or "")
    if state not in TASK_STATES:
        raise ValueError(f"invalid state: {state}")
    worker = str(payload.get("worker") or actor or "").strip()
    attempt_id = payload.get("attempt_id")
    task = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not task:
        raise KeyError(task_id)
    attempt = con.execute("SELECT * FROM task_attempts WHERE id=?", (attempt_id,)).fetchone() if attempt_id else None
    require_task_owner(task, attempt, worker, attempt_id)
    if state not in TASK_TRANSITIONS[task["state"]]:
        raise ValueError(f"bad transition: {task['state']} -> {state}")
    now = now_iso()
    finished_at = now if state in TASK_TERMINAL_STATES else None
    lease_owner = None if state in TASK_TERMINAL_STATES else task["lease_owner"]
    lease_expires = None if state in TASK_TERMINAL_STATES else task["lease_expires_at"]
    current_attempt_id = None if state in TASK_TERMINAL_STATES else task["current_attempt_id"]
    con.execute(
        """UPDATE tasks
           SET state=?, updated_at=?, lease_owner=?, lease_expires_at=?, current_attempt_id=?
           WHERE id=?""",
        (state, now, lease_owner, lease_expires, current_attempt_id, task_id),
    )
    if attempt_id:
        con.execute(
            """UPDATE task_attempts
               SET state=?, started_at=COALESCE(started_at, ?), finished_at=COALESCE(?, finished_at),
                   session_id=COALESCE(?, session_id), heartbeat_at=COALESCE(?, heartbeat_at)
               WHERE id=?""",
            (state, now if state == "running" else None, finished_at, payload.get("session_id"), now, attempt_id),
        )
    append_task_event(con, task_id, attempt_id, f"state:{state}", worker, payload.get("detail") or {})
    con.commit()
    row = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    return task_rowdict(row)


def heartbeat_task(con: sqlite3.Connection, task_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    worker = str(payload.get("worker") or actor or "").strip()
    attempt_id = str(payload.get("attempt_id") or "")
    if not attempt_id:
        raise ValueError("attempt_id is required")
    lease_seconds = int(payload.get("lease_seconds") or DEFAULT_CMD_LEASE_SECONDS)
    task = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    attempt = con.execute("SELECT * FROM task_attempts WHERE id=?", (attempt_id,)).fetchone()
    if not task:
        raise KeyError(task_id)
    require_task_owner(task, attempt, worker, attempt_id)
    now = now_iso()
    lease_expires = expires_after(lease_seconds)
    con.execute("UPDATE tasks SET lease_expires_at=?, updated_at=? WHERE id=?", (lease_expires, now, task_id))
    con.execute(
        "UPDATE task_attempts SET heartbeat_at=?, lease_expires_at=?, session_id=COALESCE(?, session_id) WHERE id=?",
        (now, lease_expires, payload.get("session_id"), attempt_id),
    )
    append_task_event(con, task_id, attempt_id, "heartbeat", worker, {"lease_seconds": lease_seconds})
    con.commit()
    row = con.execute("SELECT * FROM task_attempts WHERE id=?", (attempt_id,)).fetchone()
    return attempt_rowdict(row)


def add_task_artifact(con: sqlite3.Connection, task_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    if not con.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone():
        raise KeyError(task_id)
    kind = str(payload.get("kind") or "").strip()
    uri = str(payload.get("uri") or payload.get("path") or "").strip()
    if not kind or not uri:
        raise ValueError("kind and uri are required")
    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")
    artifact_id = str(payload.get("id") or uuid.uuid4())
    con.execute(
        """INSERT OR IGNORE INTO task_artifacts(id, task_id, attempt_id, kind, uri, name, metadata, created_by, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (artifact_id, task_id, payload.get("attempt_id"), kind, uri, payload.get("name"), json_dumps_compact(metadata), payload.get("created_by") or actor, now_iso()),
    )
    append_task_event(con, task_id, payload.get("attempt_id"), "artifact", actor, {"kind": kind, "uri": uri})
    con.commit()
    row = con.execute("SELECT * FROM task_artifacts WHERE id=?", (artifact_id,)).fetchone()
    return artifact_rowdict(row)


def create_or_resolve_decision(con: sqlite3.Connection, task_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    if not con.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone():
        raise KeyError(task_id)
    decision_id = payload.get("id")
    if payload.get("resolution") and decision_id:
        now = now_iso()
        cur = con.execute(
            """UPDATE task_decisions
               SET status='resolved', resolution=?, decided_by=?, decided_at=?
               WHERE id=? AND task_id=? AND status!='resolved'""",
            (str(payload["resolution"]), payload.get("decided_by") or actor, now, str(decision_id), task_id),
        )
        if cur.rowcount == 0:
            raise KeyError(str(decision_id))
        append_task_event(con, task_id, None, "decision_resolved", actor, {"decision_id": decision_id})
        con.commit()
        row = con.execute("SELECT * FROM task_decisions WHERE id=?", (str(decision_id),)).fetchone()
        return decision_rowdict(row)
    question = str(payload.get("question") or "").strip()
    if not question:
        raise ValueError("question is required")
    options = payload.get("options") or []
    if not isinstance(options, list):
        raise ValueError("options must be an array")
    decision_id = str(decision_id or uuid.uuid4())
    con.execute(
        """INSERT INTO task_decisions(id, task_id, question, options, status, created_by, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (decision_id, task_id, question, json_dumps_compact(options), "open", payload.get("created_by") or actor, now_iso()),
    )
    append_task_event(con, task_id, None, "decision_opened", actor, {"decision_id": decision_id})
    con.commit()
    row = con.execute("SELECT * FROM task_decisions WHERE id=?", (decision_id,)).fetchone()
    return decision_rowdict(row)


def upsert_agent_session(con: sqlite3.Connection, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    worker = str(payload.get("worker") or actor or "").strip()
    harness = canonical_harness(payload.get("harness"))
    if not session_id or not worker or not harness:
        raise ValueError("session_id, worker, and harness are required")
    capabilities = payload.get("capabilities") or []
    if not isinstance(capabilities, list):
        raise ValueError("capabilities must be an array")
    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")
    now = now_iso()
    con.execute(
        """INSERT INTO agent_sessions(session_id, worker, harness, model, target, cwd, capabilities, status, started_at, heartbeat_at, metadata)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(session_id) DO UPDATE SET
             worker=excluded.worker,
             harness=excluded.harness,
             model=excluded.model,
             target=excluded.target,
             cwd=excluded.cwd,
             capabilities=excluded.capabilities,
             status=excluded.status,
             heartbeat_at=excluded.heartbeat_at,
             metadata=excluded.metadata""",
        (
            session_id,
            worker,
            harness,
            payload.get("model"),
            payload.get("target"),
            payload.get("cwd"),
            json_dumps_compact(capabilities),
            str(payload.get("status") or "online"),
            payload.get("started_at") or now,
            now,
            json_dumps_compact(metadata),
        ),
    )
    con.commit()
    row = con.execute("SELECT * FROM agent_sessions WHERE session_id=?", (session_id,)).fetchone()
    return agent_session_rowdict(row)


def parse_json_body(handler: http.server.BaseHTTPRequestHandler) -> dict[str, Any]:
    n = int(handler.headers.get("content-length", "0"))
    body = handler.rfile.read(n)
    try:
        payload = json.loads(body or b"{}")
    except Exception as e:
        raise ValueError(str(e))
    if not isinstance(payload, dict):
        raise ValueError("expected JSON object")
    return payload


def command_token() -> str | None:
    token = os.environ.get("SWITCHBOARD_CMD_TOKEN")
    if token:
        return token
    token_file = Path(
        os.environ.get("SWITCHBOARD_CMD_TOKEN_FILE", "~/.config/switchboard/token")
    ).expanduser()
    try:
        token = token_file.read_text().strip()
    except OSError:
        return None
    return token or None


def command_actor() -> str:
    return os.environ.get("SWITCHBOARD_CMD_TOKEN_ID") or "token"


def audit_command(con: sqlite3.Connection, command_id: str, event: str, actor: str, detail: Any = None) -> None:
    if detail is not None and not isinstance(detail, str):
        detail = json.dumps(detail, ensure_ascii=False, sort_keys=True)
    con.execute(
        """INSERT INTO command_audit(seq, command_id, event, actor, detail, at)
           SELECT COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ? FROM command_audit""",
        (command_id, event, actor, detail, now_iso()),
    )


def require_command_auth(handler: http.server.BaseHTTPRequestHandler) -> bool:
    token = command_token()
    if not token:
        return True  # no token configured: open (localhost / self-host default)
    got = handler.headers.get("authorization", "")
    if not hmac.compare_digest(got, f"Bearer {token}"):
        handler._send(401, {"error": "unauthorized"})
        return False
    return True


def view_token() -> str | None:
    token = os.environ.get("SWITCHBOARD_VIEW_TOKEN")
    if token:
        return token
    token_file = Path(
        os.environ.get("SWITCHBOARD_VIEW_TOKEN_FILE", "~/.config/switchboard/view-token")
    ).expanduser()
    try:
        token = token_file.read_text().strip()
    except OSError:
        return None
    return token or None


def require_dashboard_auth(handler: http.server.BaseHTTPRequestHandler, query: dict[str, list[str]]) -> bool:
    """Dashboard GETs only: read-only ?view=<token> OR the full Bearer token.
    The view token never authorizes any other route."""
    cmd = command_token()
    if not cmd and not view_token():
        return True  # no tokens configured: open
    got = handler.headers.get("authorization", "")
    if cmd and hmac.compare_digest(got, f"Bearer {cmd}"):
        return True
    view = view_token()
    supplied = query.get("view", [""])[0]
    if view and supplied and hmac.compare_digest(supplied, view):
        return True
    handler._send(401, {"error": "unauthorized"})
    return False


def output_tail(text: str | bytes | None, limit: int = OUTPUT_TAIL_BYTES) -> str | None:
    if text is None:
        return None
    if isinstance(text, bytes):
        data = text[-limit:]
        return data.decode(errors="replace")
    encoded = text.encode(errors="replace")
    if len(encoded) <= limit:
        return text
    return encoded[-limit:].decode(errors="replace")


def enqueue_command(con: sqlite3.Connection, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    verb = str(payload.get("verb") or "")
    if verb not in VALID_COMMAND_VERBS:
        raise ValueError(f"unknown verb: {verb}")
    args = payload.get("args") if "args" in payload else {}
    if args is None:
        args = {}
    if not isinstance(args, dict):
        raise ValueError("args must be an object")
    if verb == "prompt" and "prompt" not in args and payload.get("prompt"):
        args["prompt"] = str(payload["prompt"])
    harness = canonical_harness(payload.get("harness")) or ""
    if not harness:
        raise ValueError("harness is required")
    cwd = str(payload.get("cwd") or "")
    if not cwd:
        raise ValueError("cwd is required")
    created_at = now_iso()
    expires_at = datetime.fromtimestamp(time.time() + int(payload.get("ttl_seconds") or DEFAULT_CMD_TTL_SECONDS), timezone.utc).isoformat().replace("+00:00", "Z")
    idempotency_key = payload.get("idempotency_key")
    if idempotency_key:
        row = con.execute("SELECT * FROM command_queue WHERE idempotency_key=?", (str(idempotency_key),)).fetchone()
        if row:
            return command_rowdict(row)
    command_id = str(uuid.uuid4())
    try:
        con.execute(
            """INSERT INTO command_queue(id, idempotency_key, verb, args, harness, model, target, cwd, status, created_by, created_at, expires_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                command_id,
                str(idempotency_key) if idempotency_key else None,
                verb,
                json.dumps(args, ensure_ascii=False, sort_keys=True),
                harness,
                payload.get("model"),
                payload.get("target"),
                cwd,
                "queued",
                payload.get("created_by") or actor or "cli",
                created_at,
                expires_at,
            ),
        )
    except sqlite3.IntegrityError:
        if idempotency_key:
            row = con.execute("SELECT * FROM command_queue WHERE idempotency_key=?", (str(idempotency_key),)).fetchone()
            if row:
                return command_rowdict(row)
        raise
    audit_command(con, command_id, "enqueued", actor, {"verb": verb, "harness": harness, "target": payload.get("target")})
    con.commit()
    row = con.execute("SELECT * FROM command_queue WHERE id=?", (command_id,)).fetchone()
    return command_rowdict(row)


def reap_commands(con: sqlite3.Connection, actor: str = "reaper") -> int:
    now = now_iso()
    rows = con.execute(
        "SELECT * FROM command_queue WHERE expires_at IS NOT NULL AND expires_at < ? AND status IN ('queued','claimed','running') ORDER BY created_at",
        (now,),
    ).fetchall()
    reaped = 0
    for row in rows:
        command_id = row["id"]
        if row["status"] == "queued":
            con.execute(
                "UPDATE command_queue SET status='expired', finished_at=? WHERE id=? AND status='queued'",
                (now, command_id),
            )
            audit_command(con, command_id, "reaped", actor, {"from": "queued", "to": "expired"})
        else:
            con.execute(
                """UPDATE command_queue
                   SET status='queued', claimed_by=NULL, claimed_at=NULL, expires_at=?
                   WHERE id=? AND status IN ('claimed','running')""",
                (
                    datetime.fromtimestamp(time.time() + DEFAULT_CMD_TTL_SECONDS, timezone.utc).isoformat().replace("+00:00", "Z"),
                    command_id,
                ),
            )
            audit_command(con, command_id, "reaped", actor, {"from": row["status"], "to": "queued"})
        reaped += 1
    if reaped:
        con.commit()
    return reaped


def claim_command(con: sqlite3.Connection, harnesses: list[str], worker: str, target: str | None = None, lease_seconds: int = DEFAULT_CMD_LEASE_SECONDS) -> dict[str, Any] | None:
    if not harnesses:
        raise ValueError("harnesses is required")
    if not worker:
        raise ValueError("worker is required")
    reap_commands(con, actor="claim")
    now = now_iso()
    expires_at = datetime.fromtimestamp(time.time() + lease_seconds, timezone.utc).isoformat().replace("+00:00", "Z")
    target_match = target or worker
    harnesses = [canonical_harness(h) or h for h in harnesses]
    placeholders = ",".join(["?"] * len(harnesses))
    sql = f"""
        UPDATE command_queue
        SET status='claimed', claimed_by=?, claimed_at=?, expires_at=?
        WHERE id = (
          SELECT id FROM command_queue
          WHERE status='queued'
            AND harness IN ({placeholders})
            AND (target IS NULL OR target=?)
          ORDER BY created_at
          LIMIT 1
        )
        RETURNING *
    """
    row = con.execute(sql, [worker, now, expires_at, *harnesses, target_match]).fetchone()
    if not row:
        con.commit()
        return None
    audit_command(con, row["id"], "claimed", worker, {"harnesses": harnesses, "target": target_match})
    con.commit()
    return command_rowdict(row)


def update_command_status(con: sqlite3.Connection, command_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    status = str(payload.get("status") or "")
    if status not in VALID_COMMAND_STATUSES - {"queued"}:
        raise ValueError(f"invalid status: {status}")
    now = now_iso()
    finished_at = now if status in {"done", "failed", "expired"} else None
    row = con.execute("SELECT * FROM command_queue WHERE id=?", (command_id,)).fetchone()
    if not row:
        raise KeyError(command_id)
    if row["claimed_by"] and actor != row["claimed_by"]:
        raise PermissionError("wrong owner")
    if row["expires_at"] and row["status"] in {"claimed", "running"}:
        expires = parse_iso_seconds(row["expires_at"])
        if expires is not None and expires < time.time():
            raise PermissionError("lease expired")
    allowed = {
        "queued": {"claimed", "running", "done", "failed", "expired"},
        "claimed": {"running", "done", "failed", "expired"},
        "running": {"done", "failed", "expired"},
        "done": set(),
        "failed": set(),
        "expired": set(),
    }
    if status not in allowed.get(row["status"], set()):
        raise ValueError(f"bad transition: {row['status']} -> {status}")
    cur = con.execute(
        """UPDATE command_queue
           SET status=?, exit_code=COALESCE(?, exit_code), output=COALESCE(?, output),
               session_id=COALESCE(?, session_id), finished_at=COALESCE(?, finished_at)
           WHERE id=?""",
        (
            status,
            payload.get("exit_code"),
            output_tail(payload.get("output")),
            payload.get("session_id"),
            finished_at,
            command_id,
        ),
    )
    if cur.rowcount == 0:
        raise KeyError(command_id)
    audit_command(
        con,
        command_id,
        f"status:{status}",
        actor,
        {k: payload[k] for k in ("exit_code", "session_id") if k in payload},
    )
    con.commit()
    row = con.execute("SELECT * FROM command_queue WHERE id=?", (command_id,)).fetchone()
    if not row:
        raise KeyError(command_id)
    return command_rowdict(row)


def get_command(con: sqlite3.Connection, command_id: str) -> dict[str, Any] | None:
    reap_commands(con, actor="get")
    row = con.execute("SELECT * FROM command_queue WHERE id=?", (command_id,)).fetchone()
    return command_rowdict(row) if row else None


def list_commands(con: sqlite3.Connection, filters: dict[str, str | None], limit: int = 100) -> list[dict[str, Any]]:
    reap_commands(con, actor="list")
    limit = max(1, min(int(limit), 500))
    where = []
    args: list[Any] = []
    for col in ("status", "verb", "harness", "target"):
        val = filters.get(col)
        if val:
            where.append(f"{col}=?")
            args.append(val)
    sql = "SELECT * FROM command_queue"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    args.append(limit)
    return [command_rowdict(r) for r in con.execute(sql, args).fetchall()]


def command_stats(con: sqlite3.Connection) -> dict[str, Any]:
    reap_commands(con, actor="stats")
    counts = {status: 0 for status in sorted(VALID_COMMAND_STATUSES)}
    rows = con.execute("SELECT status, COUNT(*) AS n FROM command_queue GROUP BY status").fetchall()
    for row in rows:
        counts[row["status"]] = row["n"]
    cutoff = datetime.fromtimestamp(time.time() - 3600, timezone.utc).isoformat().replace("+00:00", "Z")
    done_last_hour = con.execute(
        "SELECT COUNT(*) AS n FROM command_queue WHERE status='done' AND finished_at IS NOT NULL AND finished_at>=?",
        (cutoff,),
    ).fetchone()["n"]
    total = sum(counts.values())
    return {
        "counts_by_status": counts,
        "in_flight": counts.get("claimed", 0) + counts.get("running", 0),
        "throughput": {"done_last_hour": done_last_hour},
        "total": total,
    }


def project_where(project: str | None, cwd_prefix: str | None, args: list[Any]) -> list[str]:
    where=[]
    if project:
        like=f"%/{project.strip('/')}%"
        where.append("(workspace=? OR cwd LIKE ?)")
        args.extend([project, like])
    if cwd_prefix:
        prefix=cwd_prefix.rstrip('/')
        where.append("(cwd=? OR cwd LIKE ?)")
        args.extend([prefix, prefix + '/%'])
    return where


def fetch_remote(url: str, path: str, params: dict[str, Any], token: str | None = None) -> Any:
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    full = url.rstrip('/') + path + (('?' + qs) if qs else '')
    token = token or command_token()
    headers = {"authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(full, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def add_filter_args(parser):
    parser.add_argument('--workspace')
    parser.add_argument('--project', help='Project slug/name; matches workspace or cwd path segment, e.g. myproject')
    parser.add_argument('--cwd-prefix', help='Only include sessions/events whose cwd is at or under this path')
    parser.add_argument('--url', default=DEFAULT_URL, help='Query a remote switchboard receiver instead of local sqlite')
    parser.add_argument('--token', help='Bearer token; defaults to SWITCHBOARD_CMD_TOKEN')


def print_latest(args):
    if args.url:
        data=fetch_remote(args.url, '/latest', {'workspace': args.workspace, 'project': args.project, 'cwd_prefix': args.cwd_prefix, 'limit': args.limit}, args.token)
        print(json.dumps(data.get('events', []), indent=2, ensure_ascii=False)); return
    print(json.dumps(latest(connect(args.db), args.workspace, args.limit, project=args.project, cwd_prefix=args.cwd_prefix), indent=2, ensure_ascii=False))


def print_sessions(args):
    if args.url:
        data=fetch_remote(args.url, '/sessions', {'workspace': args.workspace, 'project': args.project, 'cwd_prefix': args.cwd_prefix, 'limit': args.limit}, args.token)
        print(json.dumps(data.get('sessions', []), indent=2, ensure_ascii=False)); return
    print(json.dumps(sessions(connect(args.db), args.workspace, args.limit, project=args.project, cwd_prefix=args.cwd_prefix), indent=2, ensure_ascii=False))


def print_digest(args):
    if args.url:
        data=fetch_remote(args.url, '/digest', {'workspace': args.workspace, 'project': args.project, 'cwd_prefix': args.cwd_prefix, 'limit': args.limit}, args.token)
        print(data.get('digest', '')); return
    print(digest_text(connect(args.db), args.workspace, args.limit, project=args.project, cwd_prefix=args.cwd_prefix))


def print_transcript(args):
    if args.url:
        data=fetch_remote(args.url, '/transcript/' + urllib.parse.quote(args.session_id, safe=''), {'limit': args.limit}, args.token)
        print(data.get('transcript', '')); return
    print(transcript(connect(args.db), args.session_id, args.limit))


def latest(con, workspace=None, limit=20, since_seconds=None, project=None, cwd_prefix=None):
    args=[]; where=[]
    if workspace: where.append("workspace=?"); args.append(workspace)
    where.extend(project_where(project, cwd_prefix, args))
    if since_seconds: where.append("ts>=?"); args.append(datetime.fromtimestamp(time.time()-since_seconds, timezone.utc).isoformat().replace('+00:00','Z'))
    sql="SELECT * FROM events" + (" WHERE "+" AND ".join(where) if where else "") + " ORDER BY ts DESC LIMIT ?"
    args.append(limit)
    return [rowdict(r) for r in con.execute(sql,args).fetchall()]


def sessions(con, workspace=None, limit=10, project=None, cwd_prefix=None):
    args=[]; where=[]
    if workspace: where.append("workspace=?"); args.append(workspace)
    where.extend(project_where(project, cwd_prefix, args))
    sql="SELECT * FROM sessions" + (" WHERE "+" AND ".join(where) if where else "") + " ORDER BY last_seen DESC LIMIT ?"
    args.append(limit)
    return [rowdict(r) for r in con.execute(sql,args).fetchall()]




def capsule_rowdict(r: sqlite3.Row) -> dict[str, Any]:
    d = rowdict(r)
    decode_json_field(d, "branch_points", [])
    decode_json_field(d, "decisions", [])
    return d


CAPSULE_WRITE_FIELDS = {
    "goal", "now", "state", "summary", "branch_points", "decisions", "question",
    "suggested_reply", "last_event_ts", "summarized_event_count", "state_changed_at",
    "notified_waiting_at", "danger_alert_at", "danger_flag", "harvested_at", "judge", "error",
}


def upsert_status_capsule(con: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        raise ValueError("session_id is required")
    state = payload.get("state")
    if state is not None and state not in CAPSULE_STATES:
        raise ValueError(f"invalid capsule state: {state}")
    fields = {k: payload[k] for k in CAPSULE_WRITE_FIELDS if k in payload}
    for k in ("branch_points", "decisions"):
        if k in fields and not isinstance(fields[k], str):
            fields[k] = json_dumps_compact(fields[k])
    now = now_iso()
    existing = con.execute("SELECT * FROM session_status WHERE session_id=?", (session_id,)).fetchone()
    if existing is None:
        if not fields.get("state"):
            fields["state"] = "working"
        fields.setdefault("state_changed_at", now)
        cols = ["session_id", *fields.keys(), "updated_at"]
        con.execute(
            f"INSERT INTO session_status({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
            [session_id, *fields.values(), now],
        )
    else:
        if fields.get("state") and fields["state"] != existing["state"]:
            fields.setdefault("state_changed_at", now)
        sets = ", ".join(f"{k}=?" for k in fields)
        con.execute(
            f"UPDATE session_status SET {sets}{', ' if sets else ''}updated_at=? WHERE session_id=?",
            [*fields.values(), now, session_id],
        )
    con.commit()
    return capsule_rowdict(con.execute("SELECT * FROM session_status WHERE session_id=?", (session_id,)).fetchone())


def list_status_capsules(con, workspace=None, state=None, session_id=None, limit=100):
    args: list[Any] = []
    where = []
    if session_id:
        where.append("st.session_id=?"); args.append(session_id)
    if workspace:
        where.append("s.workspace=?"); args.append(workspace)
    if state:
        where.append("st.state=?"); args.append(state)
    sql = (
        "SELECT st.*, s.source_machine, s.harness, s.workspace, s.cwd, s.started_at, "
        "s.last_seen, s.last_role, s.last_text, s.event_count, "
        "l.terminal_handle, l.worktree_id, l.slug, l.doc "
        "FROM session_status st "
        "LEFT JOIN sessions s ON s.session_id = st.session_id "
        "LEFT JOIN session_links l ON l.session_id = st.session_id "
        + (" WHERE " + " AND ".join(where) if where else "")
        + " ORDER BY s.last_seen DESC LIMIT ?"
    )
    args.append(limit)
    return [capsule_rowdict(r) for r in con.execute(sql, args).fetchall()]


def resolve_session_for_doc(con: sqlite3.Connection, doc: str, parent_session: str | None = None,
                            around_ts: str | None = None) -> str | None:
    """Find the transcript session whose opening user prompt embeds the handoff
    doc path (handoff spawns children with 'Read <doc> ...'). Candidate sessions
    are bounded by started_at near the spawn ts so this never scans 4M events."""
    if not doc:
        return None
    args: list[Any] = []
    where = ["session_id != ?"]
    args.append(parent_session or "")
    ts = parse_iso_seconds(around_ts)
    if ts is not None:
        lo = datetime.fromtimestamp(ts - 3600, timezone.utc).isoformat().replace("+00:00", "Z")
        hi = datetime.fromtimestamp(ts + 72 * 3600, timezone.utc).isoformat().replace("+00:00", "Z")
        where.append("last_seen >= ?"); args.append(lo)
        where.append("COALESCE(started_at, last_seen) <= ?"); args.append(hi)
    candidates = con.execute(
        f"SELECT session_id FROM sessions WHERE {' AND '.join(where)} ORDER BY last_seen DESC LIMIT 200",
        args,
    ).fetchall()
    for row in candidates:
        hit = con.execute(
            "SELECT 1 FROM events WHERE session_id=? AND role='user' AND text LIKE ? "
            "ORDER BY ts ASC LIMIT 1",
            (row["session_id"], f"%{doc}%"),
        ).fetchone()
        if hit:
            return row["session_id"]
    return None


def upsert_session_link(con: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any] | None:
    """Upsert a transcript-session↔pane join. If session_id is not supplied,
    resolve it from the handoff doc needle; return None when unresolvable (the
    caller retries on a later beat)."""
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        session_id = resolve_session_for_doc(
            con,
            str(payload.get("doc") or ""),
            payload.get("parent_session"),
            payload.get("spawned_at") or payload.get("ts"),
        ) or ""
    if not session_id:
        return None
    fields = {
        "terminal_handle": payload.get("terminal_handle"),
        "worktree_id": payload.get("worktree_id"),
        "slug": payload.get("slug"),
        "doc": payload.get("doc"),
        "parent_session": payload.get("parent_session"),
        "source": payload.get("source"),
    }
    existing = con.execute("SELECT * FROM session_links WHERE session_id=?", (session_id,)).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO session_links(session_id, terminal_handle, worktree_id, slug, doc, parent_session, source, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (session_id, *fields.values(), now_iso()),
        )
    else:
        merged = {k: (v if v is not None else existing[k]) for k, v in fields.items()}
        con.execute(
            "UPDATE session_links SET terminal_handle=?, worktree_id=?, slug=?, doc=?, parent_session=?, source=?, updated_at=? "
            "WHERE session_id=?",
            (*merged.values(), now_iso(), session_id),
        )
    con.commit()
    return rowdict(con.execute("SELECT * FROM session_links WHERE session_id=?", (session_id,)).fetchone())


def transcript(con, session_id: str, limit: int = 200) -> str:
    rows=[rowdict(r) for r in con.execute('SELECT * FROM events WHERE session_id=? ORDER BY ts ASC LIMIT ?', (session_id, limit)).fetchall()]
    if not rows:
        return f"No events for session {session_id}"
    first=rows[0]
    lines=[f"TRANSCRIPT session={session_id} source={first.get('source_machine')}/{first.get('harness')} workspace={first.get('workspace')} cwd={first.get('cwd')}"]
    if first.get('source_path'):
        lines.append(f"source_path={first.get('source_path')}")
    for e in rows:
        loc=''
        if e.get('source_line'):
            loc=f" line={e.get('source_line')}"
        txt=(e.get('text') or '').replace('\r','').strip()
        lines.append(f"\n[{e.get('ts')}] {e.get('role')}/{e.get('event_type')}{loc}")
        lines.append(txt[:4000])
    return "\n".join(lines)

def digest_text(con, workspace=None, limit=8, project=None, cwd_prefix=None) -> str:
    ss=sessions(con, workspace, limit=5, project=project, cwd_prefix=cwd_prefix)
    if not ss: return "No live handoff sessions found."
    lines=["LIVE HANDOFF DIGEST"]
    for s in ss:
        lines.append(f"- {s['source_machine']}/{s['harness']} workspace={s['workspace']} session={s['session_id']} last_seen={s['last_seen']} events={s['event_count']}")
        evs=[rowdict(r) for r in con.execute(
            'SELECT * FROM events WHERE session_id=? ORDER BY ts DESC LIMIT ?',
            (s['session_id'], limit),
        ).fetchall()]
        for e in reversed(evs):
            txt=(e.get('text') or '').replace('\n',' ')
            if len(txt)>220: txt=txt[:217]+'...'
            lines.append(f"  {e['ts']} {e.get('role')}/{e.get('event_type')}: {txt}")
    return "\n".join(lines)


# ---------- dashboard (M2: read-only fleet status HTML) ----------

STATE_COLORS = {
    "working": "#22c55e", "waiting-input": "#f59e0b", "blocked": "#ef4444",
    "done": "#3b82f6", "stale": "#94a3b8", "abandoned": "#64748b",
}
DASH_CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b1020;color:#e2e8f0;margin:0;padding:16px}
a{color:#7dd3fc;text-decoration:none} a:hover{text-decoration:underline}
h1{font-size:18px;margin:0 0 4px} h2{font-size:14px;color:#94a3b8;margin:20px 0 8px;border-bottom:1px solid #1e293b;padding-bottom:4px}
.meta{color:#64748b;font-size:12px}
.chip{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600;color:#0b1020}
.card{background:#111a2e;border:1px solid #1e293b;border-radius:8px;padding:10px 12px;margin:8px 0}
.card .goal{font-weight:600;font-size:13px;margin:2px 0}
.card .now{font-size:12px;color:#cbd5e1;margin:2px 0}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:12px;color:#94a3b8}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:8px}
pre{background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:8px;font-size:12px;white-space:pre-wrap;word-break:break-word;overflow-x:auto}
table{border-collapse:collapse;font-size:12px;width:100%}
td,th{border-bottom:1px solid #1e293b;padding:4px 8px;text-align:left;vertical-align:top}
.dim{color:#64748b}
"""


def _esc(s: Any) -> str:
    return html.escape(str(s if s is not None else ""))


def _age(ts: str | None, now_s: float | None = None) -> str:
    t = parse_iso_seconds(ts)
    if t is None:
        return "?"
    d = (now_s or time.time()) - t
    if d < 0:
        d = 0
    if d < 90: return f"{int(d)}s"
    if d < 5400: return f"{int(d // 60)}m"
    if d < 172800: return f"{d / 3600:.1f}h"
    return f"{int(d // 86400)}d"


def _chip(state: str | None) -> str:
    state = state or "no capsule"
    color = STATE_COLORS.get(state, "#334155")
    return f'<span class="chip" style="background:{color}">{_esc(state)}</span>'


def _html_page(title: str, body: str, refresh: int | None = 10) -> str:
    meta = f'<meta http-equiv="refresh" content="{refresh}">' if refresh else ""
    return (f"<!doctype html><html><head><meta charset='utf-8'>{meta}"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>{_esc(title)}</title><style>{DASH_CSS}</style></head><body>{body}</body></html>")


def dashboard_rows(con, workspace=None, lookback_seconds: int = 7 * 86400, limit: int = 200) -> list[dict[str, Any]]:
    """Recent sessions joined with capsules + links (capsule may be absent)."""
    cutoff = datetime.fromtimestamp(time.time() - lookback_seconds, timezone.utc).isoformat().replace("+00:00", "Z")
    args: list[Any] = [cutoff]
    ws = ""
    if workspace:
        ws = " AND s.workspace=?"; args.append(workspace)
    args.append(limit)
    sql = (
        "SELECT s.*, st.goal, st.now, st.state, st.summary, st.branch_points, st.decisions, "
        "st.question, st.suggested_reply, st.danger_flag, st.updated_at AS capsule_updated_at, st.last_event_ts, "
        "l.terminal_handle, l.worktree_id, l.slug, l.doc "
        "FROM sessions s "
        "LEFT JOIN session_status st ON st.session_id = s.session_id "
        "LEFT JOIN session_links l ON l.session_id = s.session_id "
        f"WHERE s.last_seen >= ?{ws} ORDER BY s.last_seen DESC LIMIT ?"
    )
    return [rowdict(r) for r in con.execute(sql, args).fetchall()]


def _session_url(session_id: str, view: str | None) -> str:
    q = f"?view={urllib.parse.quote(view)}" if view else ""
    return f"/dashboard/session/{urllib.parse.quote(session_id)}{q}"


def _fleet_card(r: dict[str, Any], view: str | None, now_s: float) -> str:
    sid = r["session_id"]
    goal = r.get("goal") or r.get("last_text") or "(no goal yet)"
    now_line = r.get("now") or ""
    slug = f" · <span class='dim'>{_esc(r['slug'])}</span>" if r.get("slug") else ""
    fresh = _age(r.get("capsule_updated_at"), now_s) if r.get("capsule_updated_at") else "—"
    danger = (f" <span class='chip' style='background:#f2c14e' title='danger keyword observed "
              f"(observe-only)'>⚠ {_esc(str(r.get('danger_flag'))[:24])}</span>"
              if r.get("danger_flag") else "")
    return (
        f"<div class='card'>{_chip(r.get('state'))}{danger} "
        f"<a href='{_session_url(sid, view)}'><code>{_esc(sid[:8])}</code></a>"
        f" <span class='dim'>{_esc(r.get('harness'))}</span>{slug}"
        f"<div class='goal'>{_esc(str(goal)[:160])}</div>"
        + (f"<div class='now'>▸ {_esc(str(now_line)[:200])}</div>" if now_line else "")
        + f"<div class='row'><span>last event {_age(r.get('last_seen'), now_s)} ago</span>"
        f"<span>capsule {fresh}</span><span>{r.get('event_count') or 0} events</span></div></div>"
    )


def dashboard_fleet_html(con, workspace=None, view: str | None = None) -> str:
    rows = dashboard_rows(con, workspace=workspace)
    now_s = time.time()
    counts: dict[str, int] = {}
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        counts[r.get("state") or "no capsule"] = counts.get(r.get("state") or "no capsule", 0) + 1
        key = f"{r.get('source_machine') or '?'} · {r.get('workspace') or r.get('cwd') or '?'}"
        groups.setdefault(key, []).append(r)
    header = " ".join(f"{_chip(s)} {n}" for s, n in sorted(counts.items(), key=lambda kv: -kv[1]))
    body = [f"<h1>Agent fleet</h1><div class='meta'>{len(rows)} sessions (7d) · {now_iso()}</div><div style='margin:8px 0'>{header}</div>"]
    pickup = [r for r in rows if r.get("state") in ("stale", "abandoned")]
    if pickup:
        body.append("<h2>Pick up or kill</h2><div class='grid'>")
        body.extend(_fleet_card(r, view, now_s) for r in pickup)
        body.append("</div>")
    for key in sorted(groups, key=lambda k: -max(parse_iso_seconds(r.get("last_seen")) or 0 for r in groups[k])):
        live = [r for r in groups[key] if r.get("state") not in ("stale", "abandoned")]
        if not live:
            continue
        body.append(f"<h2>{_esc(key)}</h2><div class='grid'>")
        body.extend(_fleet_card(r, view, now_s) for r in live)
        body.append("</div>")
    return _html_page("Agent fleet", "".join(body), refresh=10)


def dashboard_session_html(con, session_id: str, view: str | None = None) -> str:
    s = con.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
    st = con.execute("SELECT * FROM session_status WHERE session_id=?", (session_id,)).fetchone()
    link = con.execute("SELECT * FROM session_links WHERE session_id=?", (session_id,)).fetchone()
    if s is None and st is None:
        return _html_page("Not found", f"<h1>Unknown session</h1><p class='meta'>{_esc(session_id)}</p>", refresh=None)
    s = rowdict(s) if s else {}
    cap = capsule_rowdict(st) if st else {}
    link = rowdict(link) if link else {}
    now_s = time.time()
    back = f"/dashboard{'?view=' + urllib.parse.quote(view) if view else ''}"
    body = [f"<div class='meta'><a href='{back}'>← fleet</a></div>"]
    body.append(f"<h1>{_chip(cap.get('state'))} <code>{_esc(session_id[:12])}</code> "
                f"<span class='dim'>{_esc(s.get('harness'))} @ {_esc(s.get('source_machine'))}</span></h1>")
    body.append(f"<div class='meta'>{_esc(s.get('workspace'))} · {_esc(s.get('cwd'))} · "
                f"last event {_age(s.get('last_seen'), now_s)} ago · {s.get('event_count') or 0} events"
                + (f" · capsule updated {_age(cap.get('updated_at'), now_s)} ago" if cap else " · no capsule yet") + "</div>")
    if cap:
        body.append(f"<h2>Goal</h2><div>{_esc(cap.get('goal'))}</div>")
        if cap.get("now"):
            body.append(f"<h2>Now</h2><div>{_esc(cap.get('now'))}</div>")
        body.append(f"<h2>Summary</h2><div>{_esc(cap.get('summary'))}</div>")
        if cap.get("question"):
            body.append(f"<h2>Waiting on</h2><div>{_esc(cap.get('question'))}</div>")
            if cap.get("suggested_reply"):
                body.append(f"<div class='meta'>suggested: {_esc(cap.get('suggested_reply'))}</div>")
        if cap.get("branch_points"):
            body.append("<h2>Branch points</h2><ul>" + "".join(f"<li>{_esc(b)}</li>" for b in cap["branch_points"]) + "</ul>")
        if cap.get("decisions"):
            rows_html = "".join(
                f"<tr><td>{_esc(d.get('decision') if isinstance(d, dict) else d)}</td>"
                f"<td>{_esc(d.get('confidence', '') if isinstance(d, dict) else '')}</td>"
                f"<td>{_esc(d.get('why', '') if isinstance(d, dict) else '')}</td>"
                f"<td class='dim'>{_esc(d.get('ts', '') if isinstance(d, dict) else '')}</td></tr>"
                for d in cap["decisions"])
            body.append("<h2>Decision ledger</h2><table><tr><th>decision</th><th>confidence</th><th>why</th><th>at</th></tr>"
                        + rows_html + "</table>")
    resume = []
    if link.get("slug"):
        resume.append(f"handoff resume {link['slug']}")
    if (s.get("harness") or "").startswith("claude"):
        resume.append(f"claude --resume {session_id}")
    lines = "\n".join(_esc(x) for x in resume) or "(no resume path registered)"
    focus = (f"<div class='meta'><a href='orca://focus?terminal={_esc(link['terminal_handle'])}'>"
             f"open pane {_esc(link['terminal_handle'])}</a></div>") if link.get("terminal_handle") else ""
    body.append(f"<h2>Resume this</h2><pre>{lines}</pre>{focus}")
    evs = [rowdict(r) for r in con.execute(
        "SELECT * FROM events WHERE session_id=? ORDER BY ts DESC LIMIT 30", (session_id,)).fetchall()]
    tail = []
    for e in reversed(evs):
        txt = (e.get("text") or "").strip().replace("\r", "")
        if len(txt) > 700:
            txt = txt[:700] + " …"
        tail.append(f"[{_esc(e.get('ts'))}] {_esc(e.get('role'))}/{_esc(e.get('event_type'))}\n{_esc(txt)}")
    body.append("<h2>Recent transcript</h2><pre>" + "\n\n".join(tail) + "</pre>")
    return _html_page(f"session {session_id[:8]}", "".join(body), refresh=15)


class Handler(http.server.BaseHTTPRequestHandler):
    db_path = DEFAULT_DB
    def _send(self, code:int, payload:Any):
        data=json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code); self.send_header('content-type','application/json'); self.send_header('content-length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def _send_empty(self, code:int):
        self.send_response(code); self.send_header('content-length','0'); self.end_headers()
    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), fmt%args))
    def do_POST(self):
        u=urllib.parse.urlparse(self.path)
        if u.path == '/events':
            if not require_command_auth(self): return
            n=int(self.headers.get('content-length','0')); body=self.rfile.read(n)
            try: payload=json.loads(body or b'{}')
            except Exception as e: self._send(400,{"error":str(e)}); return
            evs=payload if isinstance(payload,list) else payload.get('events',[payload])
            con=connect(self.db_path); ins,sk=ingest_events(con, evs); con.close()
            self._send(200,{"inserted":ins,"skipped":sk}); return
        if u.path == '/commands':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    command=enqueue_command(con, payload, command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                self._send(200,{"id":command["id"],"status":command["status"]}); return
            finally: con.close()
        m=re.match(r"^/commands/([^/]+)/status$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    update_command_status(con, urllib.parse.unquote(m.group(1)), payload, payload.get("worker") or command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                except PermissionError as e:
                    self._send(403,{"error":str(e)}); return
                except KeyError:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"ok":True}); return
            finally: con.close()
        if u.path == '/tasks':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    task=create_task(con, payload, command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                self._send(200,{"task":task}); return
            finally: con.close()
        if u.path == '/tasks/lease':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    leased=lease_task(con, payload, payload.get("worker") or command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                if not leased:
                    self._send_empty(204); return
                self._send(200,leased); return
            finally: con.close()
        m=re.match(r"^/tasks/([^/]+)/lease$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            payload["task_id"] = urllib.parse.unquote(m.group(1))
            con=connect(self.db_path)
            try:
                try:
                    leased=lease_task(con, payload, payload.get("worker") or command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                if not leased:
                    self._send(409,{"error":"task is not queued or does not match worker capabilities"}); return
                self._send(200,leased); return
            finally: con.close()
        m=re.match(r"^/tasks/([^/]+)/status$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    task=update_task_state(con, urllib.parse.unquote(m.group(1)), payload, payload.get("worker") or command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                except PermissionError as e:
                    self._send(403,{"error":str(e)}); return
                except KeyError:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"task":task}); return
            finally: con.close()
        m=re.match(r"^/tasks/([^/]+)/heartbeat$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    attempt=heartbeat_task(con, urllib.parse.unquote(m.group(1)), payload, payload.get("worker") or command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                except PermissionError as e:
                    self._send(403,{"error":str(e)}); return
                except KeyError:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"attempt":attempt}); return
            finally: con.close()
        m=re.match(r"^/tasks/([^/]+)/artifacts$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    artifact=add_task_artifact(con, urllib.parse.unquote(m.group(1)), payload, command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                except KeyError:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"artifact":artifact}); return
            finally: con.close()
        m=re.match(r"^/tasks/([^/]+)/decisions$", u.path)
        if m:
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    decision=create_or_resolve_decision(con, urllib.parse.unquote(m.group(1)), payload, command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                except KeyError:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"decision":decision}); return
            finally: con.close()
        if u.path == '/agent-sessions':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    session=upsert_agent_session(con, payload, command_actor())
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                self._send(200,{"session":session}); return
            finally: con.close()
        if u.path == '/session-links':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                link=upsert_session_link(con, payload)
                if link is None:
                    self._send(200,{"pending":True}); return
                self._send(200,{"link":link}); return
            finally: con.close()
        if u.path == '/status-capsules':
            if not require_command_auth(self): return
            try: payload=parse_json_body(self)
            except ValueError as e: self._send(400,{"error":str(e)}); return
            con=connect(self.db_path)
            try:
                try:
                    capsule=upsert_status_capsule(con, payload)
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                self._send(200,{"capsule":capsule}); return
            finally: con.close()
        self._send(404,{"error":"not found"})
    def _send_html(self, code:int, text:str):
        data=text.encode()
        self.send_response(code); self.send_header('content-type','text/html; charset=utf-8'); self.send_header('content-length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        u=urllib.parse.urlparse(self.path); q=urllib.parse.parse_qs(u.query)
        if u.path == '/health':
            self._send(200,{"ok":True}); return
        if u.path == '/dashboard' or u.path.startswith('/dashboard/'):
            # read-only HTML; accepts the scoped view token (never valid elsewhere)
            if not require_dashboard_auth(self, q): return
            view=q.get('view',[None])[0]
            con=connect(self.db_path)
            try:
                if u.path == '/dashboard':
                    self._send_html(200, dashboard_fleet_html(con, workspace=q.get('workspace',[None])[0], view=view)); return
                if u.path.startswith('/dashboard/session/'):
                    sid=urllib.parse.unquote(u.path.split('/dashboard/session/',1)[1])
                    if sid:
                        self._send_html(200, dashboard_session_html(con, sid, view=view)); return
                self._send(404,{"error":"not found"}); return
            finally: con.close()
        if not require_command_auth(self): return
        con=connect(self.db_path)
        try:
            if u.path == '/commands/claim':
                con.close(); con=connect(self.db_path)
                harnesses=[h.strip() for h in q.get('harnesses', [''])[0].split(',') if h.strip()]
                worker=q.get('worker', [''])[0]
                target=q.get('target', [None])[0]
                lease=int(q.get('lease_seconds', [DEFAULT_CMD_LEASE_SECONDS])[0])
                try:
                    command=claim_command(con, harnesses, worker, target=target, lease_seconds=lease)
                except ValueError as e:
                    self._send(400,{"error":str(e)}); return
                if not command:
                    self._send_empty(204); return
                self._send(200,{"command":command}); return
            if u.path == '/commands/stats':
                if not require_command_auth(self): return
                self._send(200,command_stats(con)); return
            if u.path == '/status':
                if not require_command_auth(self): return
                task_counts = {state: 0 for state in sorted(TASK_STATES)}
                for row in con.execute("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state").fetchall():
                    task_counts[row["state"]] = row["n"]
                active_cutoff = datetime.fromtimestamp(
                    time.time() - AGENT_SESSION_ACTIVE_SECONDS, timezone.utc
                ).isoformat().replace("+00:00", "Z")
                active_sessions = con.execute(
                    "SELECT COUNT(*) AS n FROM agent_sessions WHERE status!='offline' AND heartbeat_at>=?",
                    (active_cutoff,),
                ).fetchone()["n"]
                self._send(200,{"ok":True,"commands":command_stats(con),"tasks":{"counts_by_state":task_counts,"total":sum(task_counts.values())},"agent_sessions":{"active":active_sessions}}); return
            if u.path == '/commands':
                if not require_command_auth(self): return
                try:
                    limit=int(q.get('limit',[100])[0])
                except ValueError:
                    self._send(400,{"error":"limit must be an integer"}); return
                commands=list_commands(
                    con,
                    {
                        "status": q.get('status',[None])[0],
                        "verb": q.get('verb',[None])[0],
                        "harness": q.get('harness',[None])[0],
                        "target": q.get('target',[None])[0],
                    },
                    limit=limit,
                )
                self._send(200,{"commands":commands}); return
            m=re.match(r"^/commands/([^/]+)$", u.path)
            if m:
                if not require_command_auth(self): return
                command=get_command(con, urllib.parse.unquote(m.group(1)))
                if not command:
                    self._send(404,{"error":"not found"}); return
                self._send(200,{"command":command}); return
            if u.path == '/tasks':
                if not require_command_auth(self): return
                try:
                    limit=int(q.get('limit',[100])[0])
                except ValueError:
                    self._send(400,{"error":"limit must be an integer"}); return
                tasks=list_tasks(
                    con,
                    {
                        "state": q.get('state',[None])[0],
                        "harness": q.get('harness',[None])[0],
                        "target": q.get('target',[None])[0],
                        "owner": q.get('owner',[None])[0],
                    },
                    limit=limit,
                )
                self._send(200,{"tasks":tasks}); return
            m=re.match(r"^/tasks/([^/]+)$", u.path)
            if m:
                if not require_command_auth(self): return
                task_id=urllib.parse.unquote(m.group(1))
                task=get_task(con, task_id)
                if not task:
                    self._send(404,{"error":"not found"}); return
                attempts=[attempt_rowdict(r) for r in con.execute("SELECT * FROM task_attempts WHERE task_id=? ORDER BY attempt_no", (task_id,)).fetchall()]
                artifacts=[artifact_rowdict(r) for r in con.execute("SELECT * FROM task_artifacts WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()]
                decisions=[decision_rowdict(r) for r in con.execute("SELECT * FROM task_decisions WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()]
                events=[rowdict(r) for r in con.execute("SELECT * FROM task_events WHERE task_id=? ORDER BY seq", (task_id,)).fetchall()]
                self._send(200,{"task":task,"attempts":attempts,"artifacts":artifacts,"decisions":decisions,"events":events}); return
            if u.path == '/agent-sessions':
                if not require_command_auth(self): return
                rows=[agent_session_rowdict(r) for r in con.execute("SELECT * FROM agent_sessions ORDER BY heartbeat_at DESC LIMIT ?", (int(q.get('limit',[100])[0]),)).fetchall()]
                self._send(200,{"sessions":rows}); return
            if u.path == '/status-capsules':
                self._send(200,{"capsules": list_status_capsules(
                    con,
                    workspace=q.get('workspace',[None])[0],
                    state=q.get('state',[None])[0],
                    session_id=q.get('session_id',[None])[0],
                    limit=int(q.get('limit',[100])[0]),
                )}); return
            if u.path == '/session-links':
                args=[]; where=[]
                for key in ('session_id','slug'):
                    val=q.get(key,[None])[0]
                    if val: where.append(f"{key}=?"); args.append(val)
                sql="SELECT * FROM session_links" + (" WHERE "+" AND ".join(where) if where else "") + " ORDER BY updated_at DESC LIMIT ?"
                args.append(int(q.get('limit',[100])[0]))
                self._send(200,{"links":[rowdict(r) for r in con.execute(sql,args).fetchall()]}); return
            if u.path == '/latest':
                since=q.get('since_seconds',[None])[0]
                self._send(200,{"events": latest(con, q.get('workspace',[None])[0], int(q.get('limit',[20])[0]), since_seconds=float(since) if since else None, project=q.get('project',[None])[0], cwd_prefix=q.get('cwd_prefix',[None])[0])}); return
            if u.path == '/sessions': self._send(200,{"sessions": sessions(con, q.get('workspace',[None])[0], int(q.get('limit',[10])[0]), project=q.get('project',[None])[0], cwd_prefix=q.get('cwd_prefix',[None])[0])}); return
            if u.path == '/digest': self._send(200,{"digest": digest_text(con, q.get('workspace',[None])[0], int(q.get('limit',[8])[0]), project=q.get('project',[None])[0], cwd_prefix=q.get('cwd_prefix',[None])[0])}); return
            if u.path.startswith('/transcript/'):
                sid=urllib.parse.unquote(u.path.split('/',2)[2])
                self._send(200,{"transcript": transcript(con, sid, int(q.get('limit',[200])[0]))}); return
            if u.path.startswith('/session/'):
                sid=urllib.parse.unquote(u.path.split('/',2)[2])
                rows=[rowdict(r) for r in con.execute('SELECT * FROM events WHERE session_id=? ORDER BY ts DESC LIMIT ?', (sid,int(q.get('limit',[30])[0]))).fetchall()]
                self._send(200,{"events":rows}); return
            self._send(404,{"error":"not found"})
        finally: con.close()


def serve(args):
    Handler.db_path=args.db
    # Complete schema creation/migrations before accepting concurrent requests.
    # Otherwise the first worker claim can race the first enqueue on a fresh DB.
    connect(args.db).close()
    httpd=http.server.ThreadingHTTPServer((args.host,args.port), Handler)
    print(f"switchboard receiver listening on http://{args.host}:{args.port} db={args.db}", flush=True)
    httpd.serve_forever()


def post_events(url: str, events: list[dict[str, Any]], token: str | None = None):
    token = token or command_token()
    data=json.dumps({"events":events}, ensure_ascii=False).encode()
    headers={'content-type':'application/json'}
    if token:
        headers['authorization'] = f'Bearer {token}'
    req=urllib.request.Request(url.rstrip('/')+'/events', data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def command_http(url: str, method: str, path: str, payload: dict[str, Any] | None = None, token: str | None = None, timeout: float = 30) -> tuple[int, Any | None]:
    token = token or command_token()
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode()
    headers = {"authorization": f"Bearer {token}"} if token else {}
    if data is not None:
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url.rstrip("/") + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return r.status, json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        body = e.read()
        detail = body.decode(errors="replace") if body else e.reason
        raise RuntimeError(f"{method} {path} failed: HTTP {e.code} {detail}") from e


def build_trigger_payload(args) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "verb": args.verb,
        "args": {},
        "harness": args.harness,
        "cwd": args.cwd or os.getcwd(),
        "created_by": args.created_by,
    }
    if args.model:
        payload["model"] = args.model
    if args.target:
        payload["target"] = args.target
    if args.prompt is not None:
        payload["args"]["prompt"] = args.prompt
    for item in args.arg or []:
        if "=" not in item:
            raise SystemExit(f"--arg must be key=value: {item}")
        k, v = item.split("=", 1)
        try:
            payload["args"][k] = json.loads(v)
        except Exception:
            payload["args"][k] = v
    return payload


def trigger(args):
    payload = build_trigger_payload(args)
    _, res = command_http(args.url, "POST", "/commands", payload, token=args.token)
    if args.wait:
        command_id = res["id"]
        deadline = time.time() + args.wait_timeout
        while time.time() < deadline:
            _, body = command_http(args.url, "GET", f"/commands/{urllib.parse.quote(command_id, safe='')}", token=args.token)
            command = body["command"]
            if command["status"] in {"done", "failed", "expired"}:
                res = command
                break
            time.sleep(args.interval)
        else:
            raise SystemExit(f"timed out waiting for command {command_id}")
    if args.json:
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        print(f"{res['id']} {res['status']}")
        if args.wait:
            if res.get("exit_code") is not None:
                print(f"exit_code={res['exit_code']}")
        if res.get("output"):
            print(res["output"], end="" if str(res["output"]).endswith("\n") else "\n")


def task_create_cli(args):
    payload: dict[str, Any] = {
        "title": args.title,
        "description": args.description,
        "idempotency_key": args.idempotency_key,
        "harness": args.harness,
        "model": args.model,
        "target": args.target,
        "cwd": args.cwd,
        "priority": args.priority,
        "created_by": args.created_by,
    }
    metadata = {}
    for item in args.meta or []:
        if "=" not in item:
            raise SystemExit(f"--meta must be key=value: {item}")
        k, v = item.split("=", 1)
        try:
            metadata[k] = json.loads(v)
        except Exception:
            metadata[k] = v
    if metadata:
        payload["metadata"] = metadata
    _, body = command_http(args.url, "POST", "/tasks", payload, token=args.token)
    print(json.dumps(body["task"] if args.json else {"id": body["task"]["id"], "state": body["task"]["state"]}, indent=2, ensure_ascii=False))


def task_lease_cli(args):
    payload = {
        "worker": args.worker,
        "harnesses": [h.strip() for h in args.harnesses.split(",") if h.strip()],
        "target": args.target,
        "lease_seconds": args.lease_seconds,
    }
    path = f"/tasks/{urllib.parse.quote(args.task_id, safe='')}/lease" if args.task_id else "/tasks/lease"
    status, body = command_http(args.url, "POST", path, payload, token=args.token)
    if status == 204 or not body:
        print("none")
        return
    print(json.dumps(body if args.json else {"task_id": body["task"]["id"], "attempt_id": body["attempt"]["id"], "state": body["task"]["state"]}, indent=2, ensure_ascii=False))


def task_status_cli(args):
    payload = {
        "state": args.state,
        "worker": args.worker,
        "attempt_id": args.attempt_id,
        "session_id": args.session_id,
    }
    _, body = command_http(args.url, "POST", f"/tasks/{urllib.parse.quote(args.task_id, safe='')}/status", payload, token=args.token)
    print(json.dumps(body["task"], indent=2, ensure_ascii=False))


def task_heartbeat_cli(args):
    payload = {
        "worker": args.worker,
        "attempt_id": args.attempt_id,
        "session_id": args.session_id,
        "lease_seconds": args.lease_seconds,
    }
    _, body = command_http(args.url, "POST", f"/tasks/{urllib.parse.quote(args.task_id, safe='')}/heartbeat", payload, token=args.token)
    print(json.dumps(body["attempt"], indent=2, ensure_ascii=False))


def tasks_cli(args):
    qs = urllib.parse.urlencode({k: v for k, v in {"state": args.state, "harness": args.harness, "target": args.target, "owner": args.owner, "limit": args.limit}.items() if v is not None})
    _, body = command_http(args.url, "GET", "/tasks" + (("?" + qs) if qs else ""), token=args.token)
    print(json.dumps(body["tasks"], indent=2, ensure_ascii=False))


def task_show_cli(args):
    _, body = command_http(args.url, "GET", f"/tasks/{urllib.parse.quote(args.task_id, safe='')}", token=args.token)
    print(json.dumps(body, indent=2, ensure_ascii=False))


def agent_session_cli(args):
    payload = {
        "session_id": args.session_id,
        "worker": args.worker,
        "harness": args.harness,
        "model": args.model,
        "target": args.target,
        "cwd": args.cwd,
        "capabilities": [c.strip() for c in args.capabilities.split(",") if c.strip()],
        "status": args.status,
    }
    _, body = command_http(args.url, "POST", "/agent-sessions", payload, token=args.token)
    print(json.dumps(body["session"], indent=2, ensure_ascii=False))


def status_cli(args):
    _, body = command_http(args.url, "GET", "/status", token=args.token)
    print(json.dumps(body, indent=2, ensure_ascii=False))


def systemd_unit(name: str, description: str, command: str, env_file: str | None = None) -> str:
    env = f"EnvironmentFile=-{env_file}\n" if env_file else ""
    return (
        "[Unit]\n"
        f"Description={description}\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"{env}"
        f"ExecStart={command}\n"
        "Restart=always\n"
        "RestartSec=5\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )


def systemd_generate(args):
    out = Path(args.output).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).resolve()
    python = sys.executable
    units = {
        "switchboard-poller.service": systemd_unit(
            "poller",
            "Switchboard receiver",
            f"{python} {script} --db {args.db} serve --host {args.host} --port {args.port}",
            args.env_file,
        ),
        "switchboard-reconciler.service": systemd_unit(
            "reconciler",
            "Switchboard workspace reconciler",
            f"{python} {script.parent / 'workspace_reconcile.py'}",
            args.env_file,
        ),
    }
    for spec in args.worker or []:
        parts = spec.split(":")
        if len(parts) not in {2, 3}:
            raise SystemExit("--worker must be name:harnesses[:target]")
        name, harnesses = parts[0], parts[1]
        target = parts[2] if len(parts) == 3 else None
        target_arg = f" --target {target}" if target else ""
        units[f"switchboard-worker-{name}.service"] = systemd_unit(
            f"worker {name}",
            f"Switchboard worker {name}",
            f"{python} {script} worker --url {args.url} --harnesses {harnesses} --worker {name}{target_arg}",
            args.env_file,
        )
    for filename, text in units.items():
        (out / filename).write_text(text)
    print(json.dumps({"output": str(out), "units": sorted(units)}, indent=2))


def post_command_status(url: str, command_id: str, status: str, worker: str, token: str | None = None, **fields):
    payload = {"status": status, "worker": worker, **{k: v for k, v in fields.items() if v is not None}}
    return command_http(url, "POST", f"/commands/{urllib.parse.quote(command_id, safe='')}/status", payload, token=token)


def prompt_adapter_argv(command: dict[str, Any], prompt: str) -> list[str]:
    harness = canonical_harness(command.get("harness")) or str(command.get("harness") or "")
    model = command.get("model")
    spec = HARNESS_ROUTES.get(harness)
    binary = os.environ.get(spec["env"]) if spec else None
    binary = binary or (spec["binary"] if spec else harness)
    if harness == "claude-code":
        argv = [binary]
        if model:
            argv.extend(["--model", str(model)])
        argv.extend(["-p", prompt])
        return argv
    if harness == "codex":
        argv = [binary, "exec"]
        if model:
            argv.extend(["--model", str(model)])
        argv.append(prompt)
        return argv
    if harness == "omp":
        argv = [binary, "-p"]
        if model:
            argv.extend(["--model", str(model)])
        argv.append(prompt)
        return argv
    if harness == "hermes":
        argv = [binary]
        if model:
            argv.extend(["-m", str(model)])
        argv.extend(["-z", prompt])
        return argv
    if harness == "orca":
        raise ValueError("Orca is an execution substrate; dispatch it through the handoff/orchestration adapter")
    if harness == "local":
        raise ValueError("direct shell prompts are disabled; route local models through OMP")
    raise ValueError(f"unsupported harness: {command.get('harness')}")


def local_command_cwd(command: dict[str, Any]) -> tuple[str, str]:
    requested = command.get("cwd") or os.getcwd()
    if os.path.isdir(requested):
        return requested, ""
    fallback = os.path.expanduser("~")
    if not os.path.isdir(fallback):
        fallback = tempfile.gettempdir()
    return fallback, f"cwd fallback: requested {requested!r} not found on this machine; using {fallback!r}\n"


def execute_command(command: dict[str, Any], timeout: int) -> tuple[int, str, str | None]:
    verb = command["verb"]
    args = command.get("args") or {}
    cwd, cwd_note = local_command_cwd(command)
    if verb == "prompt":
        prompt = args.get("prompt")
        if not prompt:
            return 2, "prompt arg is required", None
        argv = prompt_adapter_argv(command, str(prompt))
        proc = subprocess.run(
            argv,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, output_tail(cwd_note + output) or "", None
    if verb in VALID_COMMAND_VERBS:
        from stage_runner import execute_stage

        exit_code, output, session_id = execute_stage({**command, "cwd": cwd}, timeout=timeout)
        return exit_code, output_tail(cwd_note + output) or "", session_id
    return 2, f"unknown verb: {verb}", None


def worker(args):
    harnesses = [h.strip() for h in args.harnesses.split(",") if h.strip()]
    if not harnesses:
        raise SystemExit("--harnesses is required")
    install_worker_signal_handlers()
    active_command_id = None
    qs = {
        "harnesses": ",".join(harnesses),
        "worker": args.worker,
        "lease_seconds": str(args.lease_seconds),
    }
    if args.target:
        qs["target"] = args.target
    print(f"worker {args.worker} polling {args.url} harnesses={','.join(harnesses)}", flush=True)
    while True:
        try:
            status, body = command_http(args.url, "GET", "/commands/claim?" + urllib.parse.urlencode(qs), token=args.token, timeout=15)
            if status == 204 or not body:
                if args.once:
                    return
                time.sleep(args.interval)
                continue
            command = body["command"]
            command_id = command["id"]
            print(json.dumps({"claimed": command_id, "verb": command["verb"], "harness": command["harness"]}), flush=True)
            post_command_status(args.url, command_id, "running", args.worker, token=args.token)
            active_command_id = command_id
            try:
                exit_code, output, session_id = execute_command(command, args.timeout)
                final = "done" if exit_code == 0 else "failed"
            except subprocess.TimeoutExpired as e:
                exit_code = 124
                output = output_tail(((e.stdout or "") + (e.stderr or "")) if isinstance(e.stdout, str) else b"") or f"timed out after {args.timeout}s"
                session_id = None
                final = "failed"
            except Exception as e:
                exit_code = 1
                output = f"{type(e).__name__}: {e}"
                session_id = None
                final = "failed"
            post_command_status(args.url, command_id, final, args.worker, token=args.token, exit_code=exit_code, output=output, session_id=session_id)
            active_command_id = None
            print(json.dumps({"finished": command_id, "status": final, "exit_code": exit_code}), flush=True)
            if args.once:
                return
        except WorkerShutdown as shutdown:
            if active_command_id:
                try:
                    post_command_status(
                        args.url,
                        active_command_id,
                        "failed",
                        args.worker,
                        token=args.token,
                        exit_code=128 + shutdown.signum,
                        output=f"worker stopped by signal {shutdown.signum}; retry is safe",
                    )
                except Exception as e:
                    print(f"worker shutdown status error: {e}", file=sys.stderr, flush=True)
            return
        except Exception as e:
            print(f"worker error: {e}", file=sys.stderr, flush=True)
            if args.once:
                raise
            time.sleep(args.interval)


def task_worker(args):
    harnesses = [h.strip() for h in args.harnesses.split(",") if h.strip()]
    if not harnesses:
        raise SystemExit("--harnesses is required")
    install_worker_signal_handlers()
    session_id = f"{args.worker}:{os.getpid()}"
    session_payload = {
        "session_id": session_id,
        "worker": args.worker,
        "harness": canonical_harness(harnesses[0]) or harnesses[0],
        "target": args.target,
        "cwd": os.getcwd(),
        "capabilities": [canonical_harness(h) or h for h in harnesses],
        "status": "online",
    }
    print(f"task-worker {args.worker} polling {args.url} harnesses={','.join(harnesses)}", flush=True)
    active_task_id = None
    active_attempt_id = None
    stop_heartbeat = None
    heartbeat = None
    while True:
        try:
            command_http(args.url, "POST", "/agent-sessions", session_payload, token=args.token)
            lease_payload = {
                "worker": args.worker,
                "harnesses": harnesses,
                "target": args.target,
                "lease_seconds": args.lease_seconds,
            }
            status, body = command_http(
                args.url, "POST", "/tasks/lease", lease_payload, token=args.token, timeout=15
            )
            if status == 204 or not body:
                if args.once:
                    return
                time.sleep(args.interval)
                continue
            task = body["task"]
            attempt = body["attempt"]
            task_id = task["id"]
            attempt_id = attempt["id"]
            running_payload = {
                "state": "running",
                "worker": args.worker,
                "attempt_id": attempt_id,
                "session_id": session_id,
            }
            command_http(
                args.url, "POST", f"/tasks/{urllib.parse.quote(task_id, safe='')}/status",
                running_payload, token=args.token,
            )
            active_task_id = task_id
            active_attempt_id = attempt_id
            stop_heartbeat = threading.Event()

            def heartbeat_loop():
                interval = max(5, min(60, args.lease_seconds // 3))
                while not stop_heartbeat.wait(interval):
                    try:
                        command_http(
                            args.url,
                            "POST",
                            f"/tasks/{urllib.parse.quote(task_id, safe='')}/heartbeat",
                            {
                                "worker": args.worker,
                                "attempt_id": attempt_id,
                                "session_id": session_id,
                                "lease_seconds": args.lease_seconds,
                            },
                            token=args.token,
                        )
                    except Exception as e:
                        print(f"task heartbeat error: {e}", file=sys.stderr, flush=True)

            heartbeat = threading.Thread(target=heartbeat_loop, daemon=True)
            heartbeat.start()
            prompt = "\n\n".join(
                part for part in (task.get("title"), task.get("description")) if part
            )
            executable = {
                "verb": "prompt",
                "args": {"prompt": prompt},
                "harness": task.get("harness") or harnesses[0],
                "model": task.get("model"),
                "target": task.get("target"),
                "cwd": task.get("cwd") or os.getcwd(),
            }
            try:
                exit_code, output, child_session_id = execute_command(executable, args.timeout)
                final = "done" if exit_code == 0 else "failed"
            except subprocess.TimeoutExpired as e:
                exit_code, child_session_id, final = 124, None, "failed"
                output = output_tail(e.stdout or "") or f"timed out after {args.timeout}s"
            except Exception as e:
                exit_code, child_session_id, final = 1, None, "failed"
                output = f"{type(e).__name__}: {e}"
            finally:
                stop_heartbeat.set()
                heartbeat.join(timeout=2)
            command_http(
                args.url,
                "POST",
                f"/tasks/{urllib.parse.quote(task_id, safe='')}/status",
                {
                    "state": final,
                    "worker": args.worker,
                    "attempt_id": attempt_id,
                    "session_id": child_session_id or session_id,
                    "detail": {"exit_code": exit_code, "output": output_tail(output)},
                },
                token=args.token,
            )
            active_task_id = None
            active_attempt_id = None
            stop_heartbeat = None
            heartbeat = None
            print(json.dumps({"finished_task": task_id, "status": final, "exit_code": exit_code}), flush=True)
            if args.once:
                return
        except WorkerShutdown as shutdown:
            if stop_heartbeat:
                stop_heartbeat.set()
            if heartbeat:
                heartbeat.join(timeout=2)
            if active_task_id and active_attempt_id:
                try:
                    command_http(
                        args.url,
                        "POST",
                        f"/tasks/{urllib.parse.quote(active_task_id, safe='')}/status",
                        {
                            "state": "failed",
                            "worker": args.worker,
                            "attempt_id": active_attempt_id,
                            "session_id": session_id,
                            "detail": {
                                "exit_code": 128 + shutdown.signum,
                                "output": f"task worker stopped by signal {shutdown.signum}; retry is safe",
                            },
                        },
                        token=args.token,
                    )
                except Exception as e:
                    print(f"task-worker shutdown status error: {e}", file=sys.stderr, flush=True)
            try:
                command_http(
                    args.url,
                    "POST",
                    "/agent-sessions",
                    {**session_payload, "status": "offline"},
                    token=args.token,
                )
            except Exception:
                pass
            return
        except Exception as e:
            print(f"task-worker error: {e}", file=sys.stderr, flush=True)
            if args.once:
                raise
            time.sleep(args.interval)


def ingest_file(args):
    con=connect(args.db); evs=[]
    for n,line in enumerate(Path(args.path).open(errors='ignore'), start=1):
        line=line.strip()
        if not line: continue
        try: obj=json.loads(line)
        except Exception: continue
        obj["_source_line"] = n
        ev=normalize_claude_line(obj, args.source_machine, args.harness, args.path)
        if ev: evs.append(ev)
    ins,sk=ingest_events(con, evs); con.close(); print(json.dumps({"events":len(evs),"inserted":ins,"skipped":sk}, indent=2))


def relay(args):
    # Polling tailer: portable, no watchdog dependency. Tracks offsets in memory; optional initial EOF.
    roots=[Path(p).expanduser() for p in args.paths]
    exts=tuple(args.ext)
    offsets={}
    if args.from_end:
        for root in roots:
            files=[p for p in root.rglob('*') if p.is_file() and p.suffix in exts] if root.is_dir() else [root]
            for p in files:
                try: offsets[str(p)]=p.stat().st_size
                except OSError: pass
    print(f"relay watching {roots} exts={exts} -> {args.url}", flush=True)
    while True:
        batch=[]
        for root in roots:
            files=[p for p in root.rglob('*') if p.is_file() and p.suffix in exts] if root.is_dir() else [root]
            for p in files:
                if p.name.endswith(('.db', '.sqlite')) or p.name.endswith(('-wal', '-shm')):
                    continue
                try: size=p.stat().st_size
                except OSError: continue
                key=str(p); off=offsets.get(key,0)
                if size < off: off=0
                if size == off: continue
                if p.suffix == '.json':
                    # Whole-file JSON document (e.g. opencode storage); not line-oriented.
                    try:
                        obj=json.loads(p.read_text(errors='ignore'))
                    except Exception:
                        offsets[key]=size; continue
                    if isinstance(obj, dict):
                        obj["_source_offset"]=size
                        ev=normalize_claude_line(obj, args.source_machine, args.harness, str(p))
                        if ev: batch.append(ev)
                    offsets[key]=size
                    continue
                try:
                    with p.open('r', errors='ignore') as f:
                        f.seek(off)
                        line_no = None
                        try:
                            if off:
                                f.seek(0)
                                line_no = sum(1 for _ in f)
                                f.seek(off)
                            else:
                                line_no = 0
                        except Exception:
                            f.seek(off); line_no = None
                        while True:
                            line = f.readline()
                            if not line:
                                break
                            cur_off = f.tell()
                            try: obj=json.loads(line)
                            except Exception: continue
                            if line_no is not None:
                                line_no += 1
                                obj["_source_line"] = line_no
                            obj["_source_offset"] = cur_off
                            ev=normalize_claude_line(obj, args.source_machine, args.harness, str(p))
                            if ev: batch.append(ev)
                        offsets[key]=f.tell()
                except OSError: pass
        if batch:
            try:
                res=post_events(args.url,batch,args.token)
                print(json.dumps({"sent":len(batch), **res}), flush=True)
            except Exception as e:
                print(f"post failed: {e}", file=sys.stderr, flush=True)
        time.sleep(args.interval)


def main(argv=None):
    ap=argparse.ArgumentParser(description='switchboard live handoff bus')
    ap.add_argument('--db', default=DEFAULT_DB)
    sub=ap.add_subparsers(dest='cmd', required=True)
    s=sub.add_parser('serve'); s.add_argument('--host',default=DEFAULT_HOST); s.add_argument('--port',type=int,default=DEFAULT_PORT); s.set_defaults(func=serve)
    s=sub.add_parser('ingest-file'); s.add_argument('path'); s.add_argument('--source-machine',default=os.uname().nodename); s.add_argument('--harness',default='claude-code'); s.set_defaults(func=ingest_file)
    s=sub.add_parser('latest'); add_filter_args(s); s.add_argument('--limit',type=int,default=20); s.set_defaults(func=print_latest)
    s=sub.add_parser('sessions'); add_filter_args(s); s.add_argument('--limit',type=int,default=10); s.set_defaults(func=print_sessions)
    s=sub.add_parser('digest'); add_filter_args(s); s.add_argument('--limit',type=int,default=8); s.set_defaults(func=print_digest)
    s=sub.add_parser('transcript'); s.add_argument('session_id'); s.add_argument('--limit',type=int,default=200); s.add_argument('--url', default=DEFAULT_URL); s.add_argument('--token'); s.set_defaults(func=print_transcript)
    s=sub.add_parser('trigger')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--verb', required=True, choices=sorted(VALID_COMMAND_VERBS))
    s.add_argument('--harness', required=True)
    s.add_argument('--model')
    s.add_argument('--target')
    s.add_argument('--cwd', default=os.getcwd())
    s.add_argument('--created-by', default='cli')
    s.add_argument('--prompt')
    s.add_argument('--arg', action='append', help='verb arg as key=value; JSON values are accepted')
    s.add_argument('--wait', action='store_true')
    s.add_argument('--wait-timeout', type=float, default=3600)
    s.add_argument('--interval', type=float, default=1.0)
    s.add_argument('--json', action='store_true')
    s.set_defaults(func=trigger)
    s=sub.add_parser('task-create')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--title', required=True)
    s.add_argument('--description')
    s.add_argument('--idempotency-key')
    s.add_argument('--harness')
    s.add_argument('--model')
    s.add_argument('--target')
    s.add_argument('--cwd')
    s.add_argument('--priority', type=int, default=0)
    s.add_argument('--created-by', default='cli')
    s.add_argument('--meta', action='append')
    s.add_argument('--json', action='store_true')
    s.set_defaults(func=task_create_cli)
    s=sub.add_parser('task-lease')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--worker', required=True)
    s.add_argument('--task-id', help='Lease this exact task instead of the next matching queued task')
    s.add_argument('--harnesses', required=True)
    s.add_argument('--target')
    s.add_argument('--lease-seconds', type=int, default=DEFAULT_CMD_LEASE_SECONDS)
    s.add_argument('--json', action='store_true')
    s.set_defaults(func=task_lease_cli)
    s=sub.add_parser('task-status')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--task-id', required=True)
    s.add_argument('--attempt-id', required=True)
    s.add_argument('--worker', required=True)
    s.add_argument('--state', required=True, choices=sorted(TASK_STATES))
    s.add_argument('--session-id')
    s.set_defaults(func=task_status_cli)
    s=sub.add_parser('task-heartbeat')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--task-id', required=True)
    s.add_argument('--attempt-id', required=True)
    s.add_argument('--worker', required=True)
    s.add_argument('--session-id')
    s.add_argument('--lease-seconds', type=int, default=DEFAULT_CMD_LEASE_SECONDS)
    s.set_defaults(func=task_heartbeat_cli)
    s=sub.add_parser('tasks')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--state')
    s.add_argument('--harness')
    s.add_argument('--target')
    s.add_argument('--owner')
    s.add_argument('--limit', type=int, default=100)
    s.set_defaults(func=tasks_cli)
    s=sub.add_parser('task-show')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--task-id', required=True)
    s.set_defaults(func=task_show_cli)
    s=sub.add_parser('agent-session')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--session-id', required=True)
    s.add_argument('--worker', required=True)
    s.add_argument('--harness', required=True)
    s.add_argument('--model')
    s.add_argument('--target')
    s.add_argument('--cwd')
    s.add_argument('--capabilities', default='')
    s.add_argument('--status', default='online')
    s.set_defaults(func=agent_session_cli)
    s=sub.add_parser('status')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.set_defaults(func=status_cli)
    s=sub.add_parser('systemd-generate')
    s.add_argument('--output', required=True)
    s.add_argument('--db', default=DEFAULT_DB)
    s.add_argument('--host', default=DEFAULT_HOST)
    s.add_argument('--port', type=int, default=DEFAULT_PORT)
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--env-file')
    s.add_argument('--worker', action='append', help='name:harnesses[:target], repeatable')
    s.set_defaults(func=systemd_generate)
    s=sub.add_parser('worker')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--harnesses', required=True)
    s.add_argument('--worker', required=True)
    s.add_argument('--target')
    s.add_argument('--interval', type=float, default=5.0)
    s.add_argument('--lease-seconds', type=int, default=DEFAULT_CMD_LEASE_SECONDS)
    s.add_argument('--timeout', type=int, default=1800)
    s.add_argument('--once', action='store_true')
    s.set_defaults(func=worker)
    s=sub.add_parser('task-worker')
    s.add_argument('--url', default=DEFAULT_URL or f'http://{DEFAULT_HOST}:{DEFAULT_PORT}')
    s.add_argument('--token')
    s.add_argument('--harnesses', required=True)
    s.add_argument('--worker', required=True)
    s.add_argument('--target')
    s.add_argument('--interval', type=float, default=5.0)
    s.add_argument('--lease-seconds', type=int, default=DEFAULT_CMD_LEASE_SECONDS)
    s.add_argument('--timeout', type=int, default=1800)
    s.add_argument('--once', action='store_true')
    s.set_defaults(func=task_worker)
    s=sub.add_parser('relay'); s.add_argument('--url',default=f'http://{DEFAULT_HOST}:{DEFAULT_PORT}'); s.add_argument('--token'); s.add_argument('--source-machine',default=os.uname().nodename); s.add_argument('--harness',default='claude-code'); s.add_argument('--interval',type=float,default=1.0); s.add_argument('--from-end',action='store_true'); s.add_argument('--ext', action='append', default=['.jsonl'], help='file extension to tail; repeatable, default .jsonl'); s.add_argument('paths',nargs='+'); s.set_defaults(func=relay)
    args=ap.parse_args(argv); args.func(args)
if __name__ == '__main__': main()
