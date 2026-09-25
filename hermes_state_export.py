#!/usr/bin/env python3
"""Export Hermes state.db messages to per-session JSONL files for the switchboard relay.

Hermes stopped writing per-session .jsonl transcripts (~March 2026) and moved all
sessions into ~/.hermes/state.db (sqlite). The switchboard relay only tails .jsonl
files, so this exporter bridges the gap: it polls state.db and appends each new
messages row as an OpenAI-chat-shaped JSON line that normalize_claude_line()
already understands (role/content/tool_calls/session_id/timestamp/cwd).

Watermarks (last exported messages.id per db) are stored in a JSON state file so
restarts never re-export. Run with --once for a single pass (backfill), or as a
long-running poller under systemd.
"""
import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

MAX_CONTENT = 6000  # receiver truncates to 4000; keep export files bounded


def iso(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    except Exception:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(path)


def export_db(db_path: Path, out_dir: Path, state: dict, batch: int, max_per_pass: int = 0) -> int:
    key = str(db_path)
    last_id = int(state.get(key, 0))
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    total = 0
    try:
        while not max_per_pass or total < max_per_pass:
            rows = con.execute(
                """
                SELECT m.id, m.session_id, m.role, m.content, m.tool_calls,
                       m.tool_call_id, m.tool_name, m.timestamp,
                       s.cwd AS cwd, s.source AS session_source, s.title AS session_title
                FROM messages m LEFT JOIN sessions s ON s.id = m.session_id
                WHERE m.id > ? ORDER BY m.id LIMIT ?
                """,
                (last_id, batch),
            ).fetchall()
            if not rows:
                break
            by_session: dict[str, list[str]] = {}
            for r in rows:
                content = r["content"] or ""
                if len(content) > MAX_CONTENT:
                    content = content[:MAX_CONTENT] + f"…[truncated {len(content)} chars]"
                obj = {
                    "session_id": r["session_id"],
                    "timestamp": iso(r["timestamp"]),
                    "role": r["role"],
                    "content": content,
                }
                if r["tool_calls"]:
                    try:
                        obj["tool_calls"] = json.loads(r["tool_calls"])
                    except Exception:
                        pass
                if r["tool_call_id"]:
                    obj["tool_call_id"] = r["tool_call_id"]
                if r["tool_name"]:
                    obj["tool_name"] = r["tool_name"]
                if r["cwd"]:
                    obj["cwd"] = r["cwd"]
                if r["session_source"]:
                    obj["session_source"] = r["session_source"]
                if r["session_title"]:
                    obj["session_title"] = r["session_title"]
                safe_sid = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(r["session_id"]))
                by_session.setdefault(safe_sid, []).append(json.dumps(obj, ensure_ascii=False))
                last_id = r["id"]
            for sid, lines in by_session.items():
                with (out_dir / f"{sid}.jsonl").open("a") as f:
                    f.write("\n".join(lines) + "\n")
            total += len(rows)
            state[key] = last_id
    finally:
        con.close()
    return total


def main():
    ap = argparse.ArgumentParser(description="Export Hermes state.db to relay-tailable JSONL")
    ap.add_argument("--db", action="append", required=True, help="path to a Hermes state.db; repeatable")
    ap.add_argument("--out", required=True, help="output directory for per-session .jsonl files")
    ap.add_argument("--state", help="watermark state file (default: <out>/.export-state.json)")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--max-per-pass", type=int, default=4000,
                    help="cap rows exported per interval so the relay ships bounded batches; 0 = unlimited")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = Path(args.state).expanduser() if args.state else out_dir / ".export-state.json"
    dbs = [Path(d).expanduser() for d in args.db]

    print(f"exporting {[str(d) for d in dbs]} -> {out_dir}", flush=True)
    while True:
        state = load_state(state_path)
        exported = 0
        for db in dbs:
            if not db.exists():
                continue
            try:
                exported += export_db(db, out_dir, state, args.batch, args.max_per_pass)
            except Exception as e:
                print(f"export error {db}: {e}", flush=True)
        if exported:
            save_state(state_path, state)
            print(json.dumps({"exported": exported, "state": state}), flush=True)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
