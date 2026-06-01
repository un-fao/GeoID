"""Meta-guard: the architectural-invariant CI fences must not vanish silently.

Several ``test_no_*.py`` files in this tree are not ordinary unit tests — they
are *regression fences* that fail the build when a load-bearing runtime
invariant is violated (no web-framework imports in central control points, no
blocking I/O inside ``async``, ``CancelledError`` must propagate, timezone-aware
UTC only, no event-loop primitives at import time, no mutable default args,
soft-delete reaper coverage).

A large cross-dependency / import-reduction cleanup runs across many branches.
Those sweeps legitimately delete dead code — but a fence file is *not* dead
code, and deleting one as collateral silently removes that invariant's CI
enforcement with no signal in review.

This meta-guard turns "please don't delete these" into a tripwire:

* if a fence file is **deleted**, this test fails;
* if a fence file is **gutted** (its ``def test_…`` bodies stripped so the file
  exists but enforces nothing), this test fails too.

Retiring a fence is allowed — when an invariant is *intentionally* removed, drop
the invariant and its fence in the same change, and remove the entry from
``_INVARIANT_GUARDS`` below in that same change. That edit shows up in review,
so the retirement is a deliberate decision rather than a silent side effect.
"""
from __future__ import annotations

import ast

import pytest

from tests._repo_paths import REPO_ROOT

# Repo-root-relative paths of the architectural-invariant fences, each mapped to
# the one-line invariant it enforces. Keep in sync with a deliberate decision:
# removing an entry must accompany the intentional removal of its invariant.
_INVARIANT_GUARDS: dict[str, str] = {
    "tests/dynastore/tools/unit/test_control_points_no_web_framework.py": (
        "central control points (tools/async_utils, modules/concurrency, cache) "
        "stay runner-agnostic — no web-framework imports"
    ),
    "tests/dynastore/tools/unit/test_no_blocking_calls_in_async.py": (
        "no blocking I/O inside async coroutines (to_thread / run_in_thread "
        "discipline)"
    ),
    "tests/dynastore/tools/unit/test_no_cancel_swallow.py": (
        "asyncio.CancelledError must propagate — load-bearing for the task "
        "SIGTERM / heartbeat reset path"
    ),
    "tests/dynastore/tools/unit/test_no_naive_utc_datetime.py": (
        "timezone-aware UTC only — naive datetimes break ES serialization "
        "round-trips"
    ),
    "tests/dynastore/tools/unit/test_no_import_time_loop_primitives.py": (
        "no event-loop primitives constructed at import time (loop-aware "
        "startup resilience)"
    ),
    "tests/dynastore/tools/unit/test_no_dangerous_python_defaults.py": (
        "no mutable / dangerous default arguments"
    ),
    "tests/unit/modules/catalog/test_soft_delete_reaper.py": (
        "unit coverage for the soft-delete TTL reaper (the component #1530 "
        "wants to soak)"
    ),
}


def _has_live_test(path) -> bool:
    """True iff the module defines at least one ``test_*`` function whose body is
    more than a bare ``pass``/docstring/``...`` — i.e. the fence still asserts
    something rather than having been gutted to an empty shell.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
            node.name.startswith("test_")
        ):
            body = node.body
            # Drop a leading docstring before judging emptiness.
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]
            if not body:
                continue
            if all(
                isinstance(stmt, ast.Pass)
                or (
                    isinstance(stmt, ast.Expr)
                    and isinstance(stmt.value, ast.Constant)
                    and stmt.value.value is Ellipsis
                )
                for stmt in body
            ):
                continue
            return True
    return False


@pytest.mark.parametrize(
    "rel_path", sorted(_INVARIANT_GUARDS), ids=lambda p: p.rsplit("/", 1)[-1]
)
def test_invariant_guard_is_present_and_live(rel_path: str) -> None:
    path = REPO_ROOT / rel_path
    invariant = _INVARIANT_GUARDS[rel_path]
    assert path.is_file(), (
        f"Architectural-invariant fence {rel_path!r} is missing — it enforces: "
        f"{invariant}. If this invariant was intentionally retired, remove its "
        "entry from _INVARIANT_GUARDS in the same change; otherwise restore the "
        "deleted fence (likely dropped as collateral by a cleanup sweep)."
    )
    assert _has_live_test(path), (
        f"Architectural-invariant fence {rel_path!r} exists but no longer "
        f"contains a live test asserting anything — it enforces: {invariant}. "
        "A fence gutted to empty/pass bodies silently stops enforcing its "
        "invariant. Restore the assertions or retire the fence deliberately."
    )
