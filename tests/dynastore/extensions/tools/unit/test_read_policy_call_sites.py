"""Static guard: every read-path ``map_row_to_feature`` caller threads a
``read_policy`` keyword.

Issue #1076: the read-time wire-shape contract (``feature_type.expose`` merge
and ``external_id_as_feature_id``) only fires when the resolved
``ItemsReadPolicy`` is passed into ``map_row_to_feature``. Several
generator / search call sites historically omitted it, silently dropping the
contract on the STAC / records / features / distributed read paths.

This AST guard fails if any tracked caller of ``map_row_to_feature`` (outside
the sidecar pipeline, which has a different signature, and outside the
abstract protocol/definition stubs) is missing the ``read_policy=`` keyword —
catching a regression directly at the offending line rather than via a live
round-trip.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List, Tuple

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[5]

# Files whose ``.map_row_to_feature(...)`` / ``self.map_row_to_feature(...)``
# calls MUST carry a ``read_policy=`` keyword. Sidecar implementations
# (packages/.../pg_sidecars/*) are intentionally excluded — their
# ``map_row_to_feature(row, feature, context)`` is a different contract that
# reads the policy off ``context['_items_read_policy']``.
_CALLER_FILES: Tuple[str, ...] = (
    "packages/core/src/dynastore/modules/catalog/item_query.py",
    "packages/core/src/dynastore/modules/catalog/item_distributed.py",
    "packages/core/src/dynastore/modules/catalog/catalog_service.py",
    "packages/core/src/dynastore/modules/catalog/query_optimizer.py",
    "packages/extensions/stac/src/dynastore/extensions/stac/stac_virtual.py",
    "packages/extensions/stac/src/dynastore/extensions/stac/search.py",
    "packages/extensions/records/src/dynastore/extensions/records/records_generator.py",
    "packages/extensions/features/src/dynastore/extensions/features/ogc_generator.py",
)


def _iter_map_row_calls(tree: ast.AST) -> Iterator[ast.Call]:
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "map_row_to_feature"
        ):
            yield node


@pytest.mark.parametrize("rel_path", _CALLER_FILES)
def test_map_row_callers_thread_read_policy(rel_path: str) -> None:
    path = _REPO_ROOT / rel_path
    assert path.is_file(), f"caller file not found: {path}"
    tree = ast.parse(path.read_text())

    offenders: List[int] = []
    for call in _iter_map_row_calls(tree):
        kwargs = {kw.arg for kw in call.keywords if kw.arg is not None}
        if "read_policy" not in kwargs:
            offenders.append(call.lineno)

    assert not offenders, (
        f"{rel_path}: map_row_to_feature call(s) missing read_policy= at "
        f"line(s) {offenders}. Thread the resolved ItemsReadPolicy so the "
        f"expose merge / external_id_as_feature_id contract fires (issue #1076)."
    )
