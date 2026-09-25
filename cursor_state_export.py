#!/usr/bin/env python3.11
"""Export Cursor IDE chats (globalStorage state.vscdb, cursorDiskKV) to append-only
JSONL files that the switchboard relay can tail.

Cursor stores composer chats in sqlite, not files (same failure mode as Hermes
state.db) — this bridge polls the DB and appends only new bubbles per composer.

Run periodically (launchd StartInterval). Safe to re-run: per-composer exported
bubble counts are tracked in .export-state.json, and the receiver dedupes.
"""
import json
import sqlite3
import sys
from pathlib import Path

GDB = Path.home() / "Library/Application Support/Cursor/User/globalStorage/state.vscdb"
EXPORT_DIR = Path.home() / ".cursor/sessions-export"
STATE_FILE = EXPORT_DIR / ".export-state.json"
ROLE = {1: "user", 2: "assistant"}


def bubble_text(obj: dict) -> str:
    for key in ("text", "richText", "codeBlock"):
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def main() -> int:
    if not GDB.exists():
        print(f"no cursor db at {GDB}", file=sys.stderr)
        return 1
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        state = json.loads(STATE_FILE.read_text())
    except Exception:
        state = {}

    con = sqlite3.connect(f"file:{GDB}?mode=ro&immutable=1", uri=True)
    cur = con.cursor()
    exported = 0
    for (key, value) in cur.execute(
        "select key, value from cursorDiskKV where key like 'composerData:%'"
    ).fetchall():
        composer_id = key.split(":", 1)[1]
        try:
            data = json.loads(value)
        except Exception:
            continue
        headers = data.get("fullConversationHeadersOnly")
        rows = []
        if isinstance(headers, list):
            for h in headers:
                rows.append(
                    {
                        "bubbleId": h.get("bubbleId"),
                        "type": h.get("type"),
                        "createdAt": h.get("createdAt"),
                        "inline": None,
                    }
                )
        elif isinstance(data.get("conversation"), list):
            for b in data["conversation"]:
                rows.append(
                    {
                        "bubbleId": b.get("bubbleId"),
                        "type": b.get("type"),
                        "createdAt": b.get("createdAt") or data.get("createdAt"),
                        "inline": b,
                    }
                )
        if not rows:
            continue
        done = int(state.get(composer_id, 0))
        if len(rows) <= done:
            continue
        out = EXPORT_DIR / f"{composer_id}.jsonl"
        with out.open("a") as f:
            for row in rows[done:]:
                bubble = row["inline"]
                if bubble is None and row["bubbleId"]:
                    got = cur.execute(
                        "select value from cursorDiskKV where key = ?",
                        (f"bubbleId:{composer_id}:{row['bubbleId']}",),
                    ).fetchone()
                    if got:
                        try:
                            bubble = json.loads(got[0])
                        except Exception:
                            bubble = None
                text = bubble_text(bubble or {})
                if not text:
                    continue
                f.write(
                    json.dumps(
                        {
                            "sessionId": composer_id,
                            "timestamp": row["createdAt"],
                            "role": ROLE.get(row["type"], "event"),
                            "text": text[:8000],
                            "harness": "cursor",
                            "workspace": "code",
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                exported += 1
        state[composer_id] = len(rows)
    con.close()
    STATE_FILE.write_text(json.dumps(state))
    print(json.dumps({"exported_lines": exported, "composers": len(state)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
