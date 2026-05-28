"""Code-review guard: no blocking network/process calls inside ``async def``.

A synchronous ``requests.*`` HTTP call, ``subprocess.*`` invocation, or
``os.system()`` made directly inside an ``async def`` blocks the entire event
loop until it returns — stalling every other coroutine on that loop (the same
class of hazard as the import-time lock that started this campaign). Offload to
a thread (``await asyncio.to_thread(...)``) or use an async client.

Note: passing the callable to ``asyncio.to_thread`` (e.g.
``await asyncio.to_thread(subprocess.run, ...)``) is NOT a direct call and is
correctly *not* flagged — only a direct ``subprocess.run(...)`` inside async is.

Currently clean — this guard keeps it that way.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# (module-name, set-of-attrs) — a call ``<module>.<attr>(...)`` is blocking.
_BLOCKING = {
    "requests": {"get", "post", "put", "delete", "patch", "head", "options", "request"},
    "subprocess": {"run", "call", "check_call", "check_output", "Popen"},
    "os": {"system"},
}


def _iter_source_files():
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        for p in root.rglob("*.py"):
            if "__pycache__" not in p.parts:
                yield p


class _Visitor(ast.NodeVisitor):
    def __init__(self, path):
        self.path = path
        self.in_async = 0
        self.hits: list[str] = []

    def visit_AsyncFunctionDef(self, node):
        self.in_async += 1
        self.generic_visit(node)
        self.in_async -= 1

    def visit_FunctionDef(self, node):
        # a sync def nested inside an async def runs on its own (it cannot be
        # awaited) — reset the loop-blocking context for its body.
        saved, self.in_async = self.in_async, 0
        self.generic_visit(node)
        self.in_async = saved

    def visit_Call(self, node):
        if self.in_async > 0:
            f = node.func
            if (
                isinstance(f, ast.Attribute)
                and isinstance(f.value, ast.Name)
                and f.value.id in _BLOCKING
                and f.attr in _BLOCKING[f.value.id]
            ):
                self.hits.append(f"{self.path}:{node.lineno} {f.value.id}.{f.attr}() in async def")
        self.generic_visit(node)


def test_no_blocking_calls_in_async() -> None:
    offenders: list[str] = []
    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        v = _Visitor(path)
        v.visit(tree)
        offenders.extend(v.hits)
    assert not offenders, (
        "Blocking network/process call inside an async function stalls the event "
        "loop. Use `await asyncio.to_thread(...)` or an async client:\n  "
        + "\n  ".join(offenders)
    )
