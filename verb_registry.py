"""Shared command-verb and stage routing registry."""
from __future__ import annotations

import os

# Model the local coding harness (omp) uses for loop stages. Single source of truth —
# override via CE_LOCAL_MODEL. `local` is a PERMANENT model-agnostic alias on the models
# VM, so this survives future model/quant/engine swaps.
#
# 2026-08-23: switched thinking-OFF -> thinking-ON. The old thinking-OFF setting followed
# noname's Tess-on-llama.cpp guidance (thinking hurts tool-calling; streaming tool-calls
# with thinking-ON hit finish=length). Neither reproduces on the current stack
# (Qwen3.8-27B W4A16 + DFlash2 on vLLM 0.27.1): streaming+tools+thinking measured 3/3
# clean tool calls, and tool-eval scored 91 with thinking on vs 86 thinking-off-era.
# superalesha's 4,800-arm study puts thinking-OFF 9-12 points down on agentic tasks.
# TRADE-OFF: this lane was temp 0 for determinism; thinking needs temp 1.0 (thinking +
# greedy is a known-bad combo), so loop stages are no longer reproducible run-to-run.
LOCAL_MODEL = os.environ.get("CE_LOCAL_MODEL", "local")

VERB_REGISTRY = {
    "preflight": {
        "harness": "local",
        "template": "preflight {args}",
    },
    "ce-plan": {
        "harness": "codex",
        "template": "ce-plan {args}",
    },
    "ce-work": {
        "harness": "omp",
        "template": "ce-work {args}",
    },
    "run-evals": {
        "harness": "local",
        "template": "run-evals {args}",
    },
    "ce-code-review": {
        "harness": "codex",
        "template": "ce-code-review {args}",
    },
    "resolve-pr-feedback": {
        "harness": "omp",
        "template": "resolve-pr-feedback {args}",
    },
    "verify": {
        "harness": "local",
        "template": "verify {args}",
    },
    "ce-compound": {
        "harness": "codex",
        "template": "ce-compound {args}",
    },
    "taste-check": {
        "harness": "codex",
        "template": "taste-check {args}",
    },
    "prompt": {
        "harness": "claude-code",
        "template": "claude -p {prompt}",
    },
}

VALID_COMMAND_VERBS = frozenset(VERB_REGISTRY)

STAGE_REGISTRY = {
    "preflight": {
        "verb": "preflight",
        "harness": "local",
        "target": "local",
        "template": "preflight --handoff {handoff}",
    },
    "scoping": {
        "verb": "ce-plan",
        "harness": "codex",
        "template": "ce-plan --handoff {handoff}",
    },
    # Frontier prewalk (routing mode "prewalk"): explore, establish a repro,
    # make the first valid edit, and write the trajectory capsule the cheap
    # executor continues from. Same verb as scoping — the stage arg selects
    # the prewalk prompt in the stage runner.
    "prewalk": {
        "verb": "ce-plan",
        "harness": "codex",
        "template": "ce-plan --mode prewalk --handoff {handoff}",
    },
    "eval-critic": {
        "verb": "ce-code-review",
        "harness": "codex",
        "template": "ce-code-review --mode eval-critic --handoff {handoff}",
    },
    "working": {
        "verb": "ce-work",
        "harness": "omp",
        "model": LOCAL_MODEL,
        "template": "ce-work --handoff {handoff}",
    },
    "verify-local": {
        "verb": "run-evals",
        "harness": "local",
        "target": "local",
        "template": "run-evals --target local --evals {evals}",
    },
    "fixing": {
        "verb": "ce-work",
        "harness": "omp",
        "model": LOCAL_MODEL,
        "template": "ce-work --mode fix --handoff {handoff}",
    },
    "pr-open": {
        "verb": "verify",
        "harness": "local",
        "target": "pr-env",
        "template": "verify --stage pr-open --handoff {handoff}",
    },
    "verify-checkpoint": {
        "verb": "run-evals",
        "harness": "local",
        "target": "pr-env",
        "template": "run-evals --target pr-env --evals {evals}",
    },
    "fixing-checkpoint": {
        "verb": "resolve-pr-feedback",
        "harness": "omp",
        "model": LOCAL_MODEL,
        "target": "pr-env",
        "template": "resolve-pr-feedback --mode checkpoint --handoff {handoff}",
    },
    "cloud-review": {
        "verb": "ce-code-review",
        "harness": "codex",
        "template": "code-review --handoff {handoff}",
    },
    "resolve-feedback": {
        "verb": "resolve-pr-feedback",
        "harness": "omp",
        "model": LOCAL_MODEL,
        "target": "pr-env",
        "template": "resolve-pr-feedback --handoff {handoff}",
    },
    # Visual taste / UI-QA gate. Runs ONLY for UI-touching diffs, after the PR
    # preview is live and cloud-review is green. Boots the app (or targets the PR
    # preview), screenshots each changed route at mobile/tablet/desktop, extracts
    # deterministic layout signals (edge-flush, horizontal overflow, unrendered
    # markdown, theme), and has the reviewer agent emit taste_findings.json.
    # Fail-open by construction: any infra error → empty findings + advisory.
    "taste-check": {
        "verb": "taste-check",
        "harness": "codex",
        "template": "taste-check --handoff {handoff}",
    },
    # Tight UI-fix loop for taste findings: fixes the branch, pushes to the PR,
    # then re-runs taste-check. Mirrors resolve-feedback (post-PR, pushes); the
    # stage arg selects the taste findings file + a visual-focused fix prompt.
    "taste-fixing": {
        "verb": "resolve-pr-feedback",
        "harness": "omp",
        "model": LOCAL_MODEL,
        "target": "pr-env",
        "template": "resolve-pr-feedback --mode taste --handoff {handoff}",
    },
    "ready-for-human": {
        "verb": "verify",
        "harness": "local",
        "target": "pr-env",
        "template": "verify --stage ready-for-human --handoff {handoff}",
    },
    "merged": {
        "verb": "verify",
        "harness": "local",
        "template": "verify --stage merged --handoff {handoff}",
    },
    "compound": {
        "verb": "ce-compound",
        "harness": "codex",
        "template": "ce-compound --handoff {handoff}",
    },
}
