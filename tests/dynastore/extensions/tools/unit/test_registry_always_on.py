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

"""Pin the auto-union semantics of ``instantiate_extensions``.

When tests narrow extension instantiation via ``include_only``, the registry
must still load every name flagged as always-on — those classes declare
``always_on = True`` on themselves (#1003) and the runtime exposure-control
matrix enforces them as unconditional in production.

These tests stub the registry with hand-built classes (some declaring
``always_on``, some not) and assert that ``instantiate_extensions`` derives
the always-on set purely from class attributes — no hardcoded extension-name
list anywhere.
"""

from __future__ import annotations

import logging
import re
from types import SimpleNamespace
from unittest.mock import patch

from dynastore.extensions.registry import instantiate_extensions


class _AlwaysOnCls:
    always_on = True


class _TogglableCls:
    pass


# The canonical control-plane set: kept here only as a *test fixture*, not as
# framework state. Production derives this set from class attributes on the
# extension wheels that happen to be installed for the active SCOPE.
_ALWAYS_ON_NAMES = frozenset({
    "iam", "auth", "configs", "web", "admin", "template", "httpx",
})


def _stub_dict(names, always_on_names=_ALWAYS_ON_NAMES):
    return {
        n: SimpleNamespace(
            cls=_AlwaysOnCls if n in always_on_names else _TogglableCls,
            instance=None,
        )
        for n in names
    }


def _captured_load_set(caplog) -> set[str]:
    """Read the registry's "Attempting to instantiate" log line and parse
    the list of extension names it is about to walk."""
    for rec in caplog.records:
        m = re.search(r"Attempting to instantiate enabled extension modules: \[(.*?)\]",
                      rec.getMessage())
        if m:
            inner = m.group(1)
            tokens = re.findall(r"'([^']+)'", inner)
            return {t.lower().replace("_", "-") for t in tokens}
    return set()


def test_include_only_unions_always_on_extensions(caplog):
    """A narrow ``include_only`` from a test must still load class-declared always-on names."""
    discovered = {*_ALWAYS_ON_NAMES, "stac", "features", "wfs"}
    stub = _stub_dict(discovered)

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(app=SimpleNamespace(), include_only=["stac"])

    seen = _captured_load_set(caplog)
    for required in _ALWAYS_ON_NAMES:
        assert required.lower().replace("_", "-") in seen, (
            f"always-on extension {required!r} missing from instantiation "
            f"set when include_only=['stac']; got {sorted(seen)}"
        )
    assert "stac" in seen
    # Negative: non-always-on, non-requested extensions must NOT load
    assert "wfs" not in seen
    assert "features" not in seen


def test_include_only_none_loads_everything_discovered(caplog):
    """Production callers (``include_only=None``) load every discovered ext."""
    discovered = {*_ALWAYS_ON_NAMES, "stac", "features", "wfs"}
    stub = _stub_dict(discovered)

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(app=SimpleNamespace(), include_only=None)

    seen = _captured_load_set(caplog)
    expected = {n.lower().replace("_", "-") for n in discovered}
    assert seen == expected, (
        f"With include_only=None all discovered extensions should be "
        f"loaded; missing={expected - seen}, extra={seen - expected}"
    )


def test_include_only_with_legacy_redundant_always_on_is_idempotent(caplog):
    """Tests that still spell out always-on names continue to work."""
    discovered = {*_ALWAYS_ON_NAMES, "stac", "features"}
    stub = _stub_dict(discovered)

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(
            app=SimpleNamespace(),
            # Old-style marker: explicitly listed always-on names + a real one
            include_only=["iam", "admin", "configs", "stac"],
        )

    seen = _captured_load_set(caplog)
    for required in _ALWAYS_ON_NAMES:
        assert required.lower().replace("_", "-") in seen
    assert "stac" in seen
    assert "features" not in seen


def test_empty_always_on_scope_still_loads_explicit_include_only(caplog):
    """A SCOPE with zero always-on extensions is valid (#1003).

    Pins the framework's pyproject-driven invariant: if no installed wheel
    declares ``always_on = True``, ``instantiate_extensions`` must not invent
    a fallback set — it loads exactly what was asked for via ``include_only``.
    """
    discovered = {"stac", "features"}
    stub = _stub_dict(discovered, always_on_names=frozenset())  # nothing always-on

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(app=SimpleNamespace(), include_only=["stac"])

    seen = _captured_load_set(caplog)
    assert seen == {"stac"}, (
        f"empty always-on scope must not conjure a fallback set; got {sorted(seen)}"
    )
