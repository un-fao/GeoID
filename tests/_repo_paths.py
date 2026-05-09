"""Single source of truth for filesystem paths used by tests.

The Phase 1 packages restructure (PR #397) split sources from
``src/dynastore/`` into ``packages/core/src/dynastore`` plus
``packages/extensions/*/src/dynastore``. Multiple tests carried hard-coded
``src/dynastore`` paths and silently scanned empty trees for weeks (see
#422 / PR #423 for the architectural-invariants instance, #424 for the
sibling sweep).

This module computes the new roots once and **fails loudly** at import
time if any expected directory is missing, so a future restructure
cannot silently re-break the same tests.
"""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_SRC = REPO_ROOT / "packages" / "core" / "src" / "dynastore"
EXTENSIONS_ROOTS = tuple(
    sorted((REPO_ROOT / "packages" / "extensions").glob("*/src/dynastore"))
)

if not CORE_SRC.is_dir():
    raise RuntimeError(
        f"tests/_repo_paths: expected core source tree at {CORE_SRC} — "
        "packages/ layout has changed; update tests/_repo_paths.py."
    )
if not EXTENSIONS_ROOTS:
    raise RuntimeError(
        "tests/_repo_paths: no extension package roots found under "
        f"{REPO_ROOT / 'packages' / 'extensions'} — packages/ layout has "
        "changed; update tests/_repo_paths.py."
    )


def find_in_packages(rel: str) -> Path:
    """Locate ``rel`` (e.g. ``extensions/maps/maps_service.py``) under any
    package root (core or any extension). Raises if not found.
    """
    candidates = [CORE_SRC / rel, *(root / rel for root in EXTENSIONS_ROOTS)]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        f"{rel!r} not found under core or any extension package root"
    )
