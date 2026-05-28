"""Code-review guards for two dangerous Python antipatterns.

1. **Mutable default arguments** (``def f(x=[])`` / ``={}`` / ``=set()``) — the
   default is created once and shared across every call, so any mutation leaks
   between calls (a classic latent-state bug; ruff/flake8-bugbear B006).
2. **``time.sleep()`` inside ``async def``** — blocks the whole event loop,
   stalling every coroutine on that loop. Use ``await asyncio.sleep()``.

Both are scanned across all package source. The blocking-sleep check is clean
today (no allowlist). The mutable-default check grandfathers a small set of
pre-existing, *provably benign* sidecar ``get_ddl`` methods (their
``partition_keys``/``partition_key_types`` defaults are read-only and never
mutated; tracked for cleanup during sidecar consolidation) — any NEW mutable
default fails the build.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# Grandfathered benign sites, keyed ``<filename>:<funcname>`` (line-shift-proof).
# These are sidecar get_ddl methods whose list/dict defaults are read-only.
_MUTABLE_DEFAULT_ALLOWLIST = frozenset(
    {
        "attributes.py:get_ddl",
        "item_metadata.py:get_ddl",
        "access_envelope.py:get_ddl",
        "geometries.py:get_ddl",
        "base.py:get_ddl",
        "stac_items_sidecar.py:get_ddl",
    }
)


def _iter_source_files():
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        for p in root.rglob("*.py"):
            if "__pycache__" not in p.parts:
                yield p


def _has_mutable_default(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    defaults = list(fn.args.defaults) + [d for d in fn.args.kw_defaults if d is not None]
    return any(isinstance(d, (ast.List, ast.Dict, ast.Set)) for d in defaults)


def test_no_new_mutable_default_arguments() -> None:
    offenders: list[str] = []
    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _has_mutable_default(node):
                key = f"{path.name}:{node.name}"
                if key in _MUTABLE_DEFAULT_ALLOWLIST:
                    continue
                offenders.append(f"{path}:{node.lineno} def {node.name}(...=[] / {{}})")
    assert not offenders, (
        "Mutable default argument(s) — the default is shared across calls. Use a "
        "None sentinel and normalise inside the body:\n  " + "\n  ".join(offenders)
    )


def test_no_blocking_time_sleep_in_async() -> None:
    offenders: list[str] = []

    class _V(ast.NodeVisitor):
        def __init__(self, path):
            self.path = path
            self.depth = 0

        def visit_AsyncFunctionDef(self, node):
            self.depth += 1
            self.generic_visit(node)
            self.depth -= 1

        def visit_FunctionDef(self, node):
            # a sync def nested in an async def resets the loop-blocking context
            saved, self.depth = self.depth, 0
            self.generic_visit(node)
            self.depth = saved

        def visit_Call(self, node):
            if self.depth > 0:
                f = node.func
                if (
                    isinstance(f, ast.Attribute)
                    and f.attr == "sleep"
                    and isinstance(f.value, ast.Name)
                    and f.value.id == "time"
                ):
                    offenders.append(f"{self.path}:{node.lineno} time.sleep() in async def")
            self.generic_visit(node)

    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        _V(path).visit(tree)

    assert not offenders, (
        "time.sleep() inside an async function blocks the event loop. "
        "Use `await asyncio.sleep(...)`:\n  " + "\n  ".join(offenders)
    )
