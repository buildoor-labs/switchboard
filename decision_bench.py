#!/usr/bin/env python3
"""decision_bench — bake off decision-model candidates for switchboard's learning-layer gate.

The learning layer promotes a session to a durable learning only if a decision model says
"save this". That model can be Jev (paid, hosted), SemIf, decider, a self-hosted Ollaya/Rev
head, or just a generative model. They all speak the same `/v1/systemone` contract (or, for
the generative baseline, a chat endpoint), so this harness runs each ONE over the SAME labeled
set of sessions and reports how well it agrees with the gold promote/skip labels, plus latency.

The point: pick a decider by how it does on *our* task ("is this coding session worth saving?"),
not on a general leaderboard like JevBench.

Usage:
  # 1. run the bake-off over a labeled set
  python3 decision_bench.py --cases bench/decisions.sample.jsonl --candidates bench/candidates.sample.json

  # 2. build a labeled set from your own sessions, silver-labelled by a strong reference model
  python3 decision_bench.py --make-labels --url http://receiver:17888 --workspace code --limit 40 \
      --base-url http://localhost:8080/v1 --model local --out bench/cases.jsonl

A candidate is one JSON object:
  {"name": "semif",  "kind": "systemone", "url": "https://gateway.smith.langchain.com",
   "model": "semif-qwen3.5-4b", "token_env": "LANGSMITH_API_KEY"}
  {"name": "jev",    "kind": "systemone", "url": "https://gateway.smith.langchain.com",
   "model": "typesafe/jev-1.13.0", "token_env": "LANGSMITH_API_KEY"}
  {"name": "decider-4b", "kind": "systemone", "url": "http://models-vm:8000", "model": "Mapika/decider-4b"}
  {"name": "gen-27b", "kind": "generative", "url": "http://localhost:8080/v1", "model": "local"}
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from pathlib import Path

import switchboard as sb

# labels used by the `classify` kind for the promote/skip decision (index 0 = promote)
CLASSIFY_LABELS = ["worth saving as durable reusable memory", "not worth saving"]


def _rev_prob(url: str, token: str | None, state: str, timeout: int = 30) -> float | None:
    """POST to Rev's native /score endpoint; return p(save). Rev is deterministic, non-generative."""
    body=json.dumps({
        "state": state,
        "questions": [{"id": "promote", "instructions": sb.DECISION_QUESTION,
                       "criteria": {"save": CLASSIFY_LABELS[0], "skip": CLASSIFY_LABELS[1]}}],
    }).encode()
    headers={"content-type": "application/json"}
    if token: headers["authorization"]=f"Bearer {token}"
    req=urllib.request.Request(url.rstrip('/') + '/score', data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data=json.loads(r.read())
    ans=(data.get("answers") or {}).get("promote") or {}
    probs=ans.get("probabilities") or {}
    if "save" in probs:
        return probs["save"]
    return 1.0 if ans.get("choice") == "save" else (0.0 if ans.get("choice") == "skip" else None)


def _classify_prob(url: str, token: str | None, labels: list[str], state: str, timeout: int = 30) -> float | None:
    """POST to a /v1/classify endpoint (classifier.dev-style); return the score for labels[0]."""
    body=json.dumps({"input": state, "labels": labels}).encode()
    headers={"content-type": "application/json"}
    if token: headers["authorization"]=f"Bearer {token}"
    req=urllib.request.Request(url.rstrip('/') + '/v1/classify', data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data=json.loads(r.read())
    res=(data.get("results") or [{}])[0]
    scores=res.get("scores") or {}
    if labels[0] in scores:
        return scores[labels[0]]
    conf=res.get("confidence")
    if conf is None:
        return None
    return conf if res.get("label") == labels[0] else 1.0 - conf


def _candidate_token(c: dict) -> str | None:
    if c.get("token_env"):
        return os.environ.get(c["token_env"])
    return c.get("token")


def decide(c: dict, state: str, threshold: float, max_chars: int) -> tuple[bool, float | None, float]:
    """Return (promote, probability_or_none, latency_ms) for one candidate on one state."""
    token=_candidate_token(c)
    state=state[:max_chars]
    t0=time.perf_counter()
    if c["kind"] == "systemone":
        d=sb.call_decision_endpoint(c["url"], token, c["model"], state)
        prob=d.get("probability")
        promote=(prob is not None) and prob >= threshold
    elif c["kind"] == "classify":
        prob=_classify_prob(c["url"], token, c.get("labels") or CLASSIFY_LABELS, state)
        promote=(prob is not None) and prob >= threshold
    elif c["kind"] == "rev":
        prob=_rev_prob(c["url"], token, state)
        promote=(prob is not None) and prob >= threshold
    elif c["kind"] == "generative":
        v=sb.parse_judge_json(sb.call_judge(c["url"], c["model"], token, sb.DECISION_SYSTEM, state))
        promote=bool(v.get("promote"))
        prob=(int(v.get("score") or 0)) / 10.0
    else:
        raise ValueError(f"unknown candidate kind: {c['kind']}")
    return promote, prob, (time.perf_counter() - t0) * 1000.0


def score_rows(rows: list[dict]) -> dict:
    graded=[r for r in rows if "pred" in r]
    n=len(graded)
    if not n:
        return {"n": 0, "errors": len(rows)}
    acc=sum(1 for r in graded if r["pred"] == r["gold"]) / n
    tp=sum(1 for r in graded if r["pred"] == "promote" and r["gold"] == "promote")
    fp=sum(1 for r in graded if r["pred"] == "promote" and r["gold"] == "skip")
    fn=sum(1 for r in graded if r["pred"] == "skip" and r["gold"] == "promote")
    prec=tp / (tp + fp) if (tp + fp) else 0.0
    rec=tp / (tp + fn) if (tp + fn) else 0.0
    f1=2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    lat=[r["ms"] for r in graded]
    lat_sorted=sorted(lat)
    p95=lat_sorted[min(len(lat_sorted) - 1, int(round(0.95 * (len(lat_sorted) - 1))))] if lat else 0.0
    return {"n": n, "errors": len(rows) - n, "accuracy": round(acc, 3),
            "promote_precision": round(prec, 3), "promote_recall": round(rec, 3), "f1": round(f1, 3),
            "p50_ms": round(statistics.median(lat), 1) if lat else 0.0, "p95_ms": round(p95, 1)}


def run_bench(cases: list[dict], candidates: list[dict], threshold: float, max_chars: int) -> dict:
    report={}
    for c in candidates:
        rows=[]
        for case in cases:
            try:
                promote, prob, ms=decide(c, case["state"], threshold, max_chars)
                rows.append({"id": case.get("id"), "gold": case["gold"],
                             "pred": "promote" if promote else "skip", "prob": prob, "ms": round(ms, 1)})
            except Exception as e:
                rows.append({"id": case.get("id"), "error": f"{type(e).__name__}: {e}"})
        report[c["name"]]={"summary": score_rows(rows), "rows": rows}
    return report


def print_table(report: dict):
    cols=["candidate", "n", "acc", "promote_P", "promote_R", "f1", "p50ms", "p95ms", "err"]
    print("  ".join(f"{c:>10}" if c != "candidate" else f"{c:<18}" for c in cols))
    for name, r in report.items():
        s=r["summary"]
        vals=[f"{name:<18}", f"{s.get('n',0):>10}", f"{s.get('accuracy',0):>10}",
              f"{s.get('promote_precision',0):>10}", f"{s.get('promote_recall',0):>10}",
              f"{s.get('f1',0):>10}", f"{s.get('p50_ms',0):>10}", f"{s.get('p95_ms',0):>10}",
              f"{s.get('errors',0):>10}"]
        print("  ".join(vals))


def make_labels(args):
    """Build a labeled case set from real sessions, silver-labelled by a strong reference model."""
    base_url=args.base_url or os.environ.get("SWITCHBOARD_EVAL_BASE_URL")
    model=args.model or os.environ.get("SWITCHBOARD_EVAL_MODEL", "local")
    api_key=args.api_key or os.environ.get("SWITCHBOARD_EVAL_API_KEY")
    if not base_url:
        raise SystemExit("--make-labels needs --base-url (a strong reference model) to produce silver labels")
    if args.url:
        targets=sb.fetch_remote(args.url, '/sessions', {'workspace': args.workspace, 'limit': args.limit}, args.token).get('sessions', [])
    else:
        targets=sb.sessions(sb.connect(args.db), args.workspace, args.limit)
    out=Path(args.out); n=0
    with out.open("w") as f:
        for s in targets:
            sid=s["session_id"]
            if args.url:
                tx=sb.fetch_remote(args.url, '/transcript/' + urllib.parse.quote(sid, safe=''), {'limit': 400}, args.token).get('transcript', '')
            else:
                tx=sb.transcript(sb.connect(args.db), sid, 400)
            if not tx or tx.startswith("No events"):
                continue
            try:
                v=sb.parse_judge_json(sb.call_judge(base_url, model, api_key, sb.DECISION_SYSTEM, tx[:args.max_chars]))
            except Exception:
                continue
            gold="promote" if v.get("promote") else "skip"
            f.write(json.dumps({"id": sid, "state": tx[:args.max_chars], "gold": gold, "label_source": f"silver:{model}"}) + "\n")
            n+=1
    print(json.dumps({"out": str(out), "labeled": n, "note": "silver labels from a reference model — review before trusting as gold"}, indent=2))


def main(argv=None):
    ap=argparse.ArgumentParser(description="bake off decision models for switchboard's promotion gate")
    ap.add_argument('--db', default=sb.DEFAULT_DB)
    ap.add_argument('--cases', help='JSONL of {"id","state","gold":"promote|skip"}')
    ap.add_argument('--candidates', help='JSON list of candidate configs')
    ap.add_argument('--threshold', type=float, default=0.5, help='promote when p(save) >= this (systemone) ')
    ap.add_argument('--max-chars', type=int, default=24000)
    ap.add_argument('--json', action='store_true', help='emit the full report incl. per-case rows')
    # --make-labels mode
    ap.add_argument('--make-labels', action='store_true')
    ap.add_argument('--out', default='bench/cases.jsonl')
    ap.add_argument('--url'); ap.add_argument('--token'); ap.add_argument('--workspace')
    ap.add_argument('--limit', type=int, default=40)
    ap.add_argument('--base-url'); ap.add_argument('--model'); ap.add_argument('--api-key')
    args=ap.parse_args(argv)

    if args.make_labels:
        make_labels(args); return
    if not args.cases or not args.candidates:
        raise SystemExit("need --cases and --candidates (or --make-labels)")
    cases=[json.loads(l) for l in Path(args.cases).read_text().splitlines() if l.strip()]
    candidates=json.loads(Path(args.candidates).read_text())
    report=run_bench(cases, candidates, args.threshold, args.max_chars)
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_table(report)


if __name__ == '__main__':
    main()
