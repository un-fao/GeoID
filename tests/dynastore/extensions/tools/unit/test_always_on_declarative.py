"""Regression tests for the declarative ``always_on`` mechanism (#1003).

Pins:
- Every extension class previously hardcoded in ``ALWAYS_ON_EXTENSIONS`` now
  declares ``always_on = True`` on its class.
- ``_get_dynamic_sets()`` produces the expected union when the registry is
  populated, and falls back to the hardcoded constants otherwise.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.exposure_mixin import (
    ALWAYS_ON_EXTENSIONS,
    KNOWN_EXTENSION_IDS,
    _get_dynamic_sets,
)


def test_extension_protocol_default_always_on_is_false():
    """Base contract: extensions opt-in, they do not default to always-on."""
    assert ExtensionProtocol.always_on is False


@pytest.mark.parametrize(
    "module_path,cls_name",
    [
        ("dynastore.extensions.iam.service", "IamExtension"),
        ("dynastore.extensions.auth.authentication", "Authentication"),
        ("dynastore.extensions.configs.service", "ConfigsService"),
        ("dynastore.extensions.web.web", "Web"),
        ("dynastore.extensions.admin.admin_service", "AdminService"),
        ("dynastore.extensions.template.templating", "TemplatingExtension"),
        ("dynastore.extensions.httpx.httpx_service", "HttpxExtension"),
    ],
)
def test_always_on_extensions_declare_class_attribute(module_path, cls_name):
    """Every previously-hardcoded always-on extension declares it on the class."""
    pytest.importorskip(module_path)
    import importlib

    mod = importlib.import_module(module_path)
    cls = getattr(mod, cls_name)
    assert getattr(cls, "always_on", False) is True, (
        f"{cls_name} must declare ``always_on = True`` so the registry can "
        f"derive its always-on status without a hardcoded list (#1003)."
    )


def test_get_dynamic_sets_returns_legacy_constants_when_registry_empty():
    with patch(
        "dynastore.extensions.registry._DYNASTORE_EXTENSIONS",
        {},
    ):
        always_on, known = _get_dynamic_sets()
    assert always_on == ALWAYS_ON_EXTENSIONS
    assert known == KNOWN_EXTENSION_IDS


def test_get_dynamic_sets_derives_always_on_from_class_attribute():
    """When the registry is populated, always-on is derived from class attrs."""

    class _AlwaysOn:
        always_on = True

    class _Togglable:
        pass

    stub = {
        "iam": SimpleNamespace(cls=_AlwaysOn, instance=None),
        "auth": SimpleNamespace(cls=_AlwaysOn, instance=None),
        "stac": SimpleNamespace(cls=_Togglable, instance=None),
    }
    with patch(
        "dynastore.extensions.registry._DYNASTORE_EXTENSIONS",
        stub,
    ):
        always_on, known = _get_dynamic_sets()

    assert "iam" in always_on
    assert "auth" in always_on
    assert "stac" not in always_on
    assert {"iam", "auth", "stac"}.issubset(known)


def test_get_dynamic_sets_unions_legacy_fallback_for_unregistered_names():
    """Legacy names not represented in the registry stay always-on as a safety net."""

    class _Togglable:
        pass

    stub = {"stac": SimpleNamespace(cls=_Togglable, instance=None)}
    with patch(
        "dynastore.extensions.registry._DYNASTORE_EXTENSIONS",
        stub,
    ):
        always_on, known = _get_dynamic_sets()

    # `iam` isn't in the stub but is in the legacy constant — it must still
    # be treated as always-on so the lifespan logic doesn't accidentally gate
    # an extension that hasn't yet migrated to the class-attribute pattern.
    assert "iam" in always_on
