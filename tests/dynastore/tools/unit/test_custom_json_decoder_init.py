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

"""Regression test for ``CustomJSONDecoder.__init__`` keyword collision.

Background: the previous signature was::

    def __init__(self, *args, **kwargs):
        super().__init__(
            object_hook=CustomJSONDecoder._object_hook, *args, **kwargs
        )

ruff B026 flagged the star-arg unpacking after a keyword argument. The
defect was not just style:

1. ``json.loads(s, cls=CustomJSONDecoder, object_hook=fn)`` raised
   ``TypeError: __init__() got multiple values for keyword argument
   'object_hook'`` because stdlib ``json.loads`` forwards ``object_hook``
   via ``kwargs`` and the explicit kwarg above collided with it.
2. ``CustomJSONDecoder("anything")`` raised
   ``TypeError: __init__() takes 1 positional argument`` because
   ``json.JSONDecoder.__init__`` is keyword-only.

Fix: drop ``*args``; pop any caller-supplied ``object_hook`` from
``kwargs`` before forwarding (the class's whole purpose is to enforce
its own ``_object_hook``).
"""
from __future__ import annotations

import json

import pytest

from dynastore.tools.json import CustomJSONDecoder


def test_basic_decode_uses_custom_object_hook():
    """Pin the happy path so the override isn't accidentally bypassed —
    string values that look like ISO dates should be parsed to datetime."""
    out = json.loads(
        '{"timestamp": "2026-05-17", "scalar": 42}', cls=CustomJSONDecoder
    )
    import datetime

    assert isinstance(out["timestamp"], (datetime.datetime, datetime.date)), (
        f"_object_hook must convert ISO-date strings; got "
        f"{type(out['timestamp']).__name__} {out['timestamp']!r}"
    )
    assert out["scalar"] == 42


def test_json_loads_with_user_supplied_object_hook_no_longer_crashes():
    """Pin the previously-broken call shape. Stdlib ``json.loads``
    forwards ``object_hook`` via kwargs, so this used to collide with
    the explicit ``object_hook=...`` inside ``__init__``."""
    # Before the fix this raised:
    #   TypeError: __init__() got multiple values for keyword argument
    #   'object_hook'
    out = json.loads(
        '{"x": 1}', cls=CustomJSONDecoder, object_hook=lambda d: d
    )
    # The class's own _object_hook wins (we silently drop the caller
    # override since its whole purpose is to enforce its own hook).
    assert out == {"x": 1}


def test_constructor_no_longer_accepts_positional_args():
    """``json.JSONDecoder`` is keyword-only; ``*args`` was dead, and
    accepting positional args silently passed-through used to raise
    deep inside the stdlib instead of failing at our boundary."""
    with pytest.raises(TypeError):
        CustomJSONDecoder("unexpected_positional")  # type: ignore[call-arg]


def test_caller_object_hook_is_dropped_not_collision():
    """Empirical proof that the fix silently drops the caller's
    ``object_hook`` rather than crashing — required for callers that
    legitimately pass ``object_hook=...`` to stdlib ``json.loads``
    without realising they are using ``CustomJSONDecoder`` via ``cls=``."""
    sentinel_calls: list[dict] = []

    def caller_hook(d):
        sentinel_calls.append(d)
        return d

    out = json.loads(
        '{"a": "2026-05-17"}',
        cls=CustomJSONDecoder,
        object_hook=caller_hook,
    )
    assert sentinel_calls == [], (
        "caller-supplied object_hook must be ignored — the class enforces "
        "its own _object_hook, so the caller hook should never run; got "
        f"{sentinel_calls!r}"
    )
    # And _object_hook ran instead (ISO-date → datetime):
    import datetime

    assert isinstance(out["a"], (datetime.datetime, datetime.date))
