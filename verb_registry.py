"""Command-verb registry for the task/command queue.

A verb names a kind of work a worker can execute. The built-in worker in
``switchboard.py`` executes the ``prompt`` verb directly (it shells out to the
target harness with the given prompt). Add your own verbs here and handle them
in a custom worker if you want richer routing.
"""
from __future__ import annotations

VERB_REGISTRY = {
    "prompt": {
        "harness": "claude-code",
        "template": "{prompt}",
    },
}

VALID_COMMAND_VERBS = frozenset(VERB_REGISTRY)
