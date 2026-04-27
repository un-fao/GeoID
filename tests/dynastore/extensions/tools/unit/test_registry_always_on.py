"""Pin the auto-union semantics of ``instantiate_extensions``.

When tests narrow extension instantiation via ``include_only``, the registry
must still load every name in ``ALWAYS_ON_EXTENSIONS`` — those are the core
control-plane components (``iam``, ``auth``, ``configs``, ``web``, ``admin``,
``tools``, ``template``, ``httpx``, ``documentation``) and the runtime
exposure-control matrix already enforces them as unconditional in production.
Asking every test marker to repeat the same nine names is busywork.

These tests assert the union behaviour at the registry layer; the
upstream conftest mechanism for ``@pytest.mark.enable_extensions`` is
unchanged.
"""

from __future__ import annotations

import logging
import re
from types import SimpleNamespace
from unittest.mock import patch

from dynastore.extensions.registry import instantiate_extensions
from dynastore.extensions.tools.exposure_mixin import ALWAYS_ON_EXTENSIONS


def _stub_dict(names):
    return {n: SimpleNamespace(cls=type(f"{n.title()}Cls", (), {}), instance=None)
            for n in names}


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
    """A narrow ``include_only`` from a test must still load ALWAYS_ON names."""
    discovered = {*ALWAYS_ON_EXTENSIONS, "stac", "features", "wfs"}
    stub = _stub_dict(discovered)

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(app=SimpleNamespace(), include_only=["stac"])

    seen = _captured_load_set(caplog)
    for required in ALWAYS_ON_EXTENSIONS:
        assert required.lower().replace("_", "-") in seen, (
            f"ALWAYS_ON extension {required!r} missing from instantiation "
            f"set when include_only=['stac']; got {sorted(seen)}"
        )
    assert "stac" in seen
    # Negative: non-always-on, non-requested extensions must NOT load
    assert "wfs" not in seen
    assert "features" not in seen


def test_include_only_none_loads_everything_discovered(caplog):
    """Production callers (``include_only=None``) load every discovered ext."""
    discovered = {*ALWAYS_ON_EXTENSIONS, "stac", "features", "wfs"}
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
    discovered = {*ALWAYS_ON_EXTENSIONS, "stac", "features"}
    stub = _stub_dict(discovered)

    caplog.set_level(logging.INFO, logger="dynastore.extensions.registry")
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        instantiate_extensions(
            app=SimpleNamespace(),
            # Old-style marker: explicitly listed always-on names + a real one
            include_only=["iam", "admin", "configs", "stac"],
        )

    seen = _captured_load_set(caplog)
    for required in ALWAYS_ON_EXTENSIONS:
        assert required.lower().replace("_", "-") in seen
    assert "stac" in seen
    assert "features" not in seen
