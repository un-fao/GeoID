"""RFC 7396 JSON Merge Patch.

Single-purpose helper: applies a patch document to a target document
following the algorithm in https://datatracker.ietf.org/doc/html/rfc7396.

Semantics:
  * A patch ``dict`` is recursively merged into a target ``dict``.
  * Inside a dict, a value of ``None`` (JSON ``null``) **removes** the key
    from the target. This is how the spec expresses deletion.
  * Any other value type (``list``, scalar, dict-vs-non-dict mismatch)
    **replaces** the value at that key — lists are NOT element-merged.
  * A non-dict patch replaces the target entirely.

This is intentionally small and dependency-free; it lives next to the
asset service that needs it.
"""

from __future__ import annotations

from typing import Any


def merge_patch(target: Any, patch: Any) -> Any:
    """Apply RFC 7396 JSON Merge Patch.

    Returns a new value; does not mutate ``target`` in place.
    """
    if not isinstance(patch, dict):
        return patch

    result: dict = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict):
            result[key] = merge_patch(result.get(key), value)
        else:
            result[key] = value
    return result
