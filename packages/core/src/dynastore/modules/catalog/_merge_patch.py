#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

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
