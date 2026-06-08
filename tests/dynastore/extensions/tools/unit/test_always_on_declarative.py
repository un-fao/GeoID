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

"""Regression tests for the declarative ``always_on`` mechanism (#1003).

Pins:
- ``ExtensionProtocol.always_on`` defaults to ``False`` — extensions opt-in.
- Every core control-plane extension declares ``always_on = True`` on its
  class (so the registry can derive the set without a hardcoded list).
- ``_get_dynamic_sets()`` returns ``(always_on, known)`` derived purely from
  the live registry — no legacy union, no hardcoded fallback. Empty registry
  yields ``(frozenset(), frozenset())``; SCOPE drives everything (#1003).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.exposure_mixin import _get_dynamic_sets


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
    """Every core control-plane extension declares it on the class."""
    pytest.importorskip(module_path)
    import importlib

    mod = importlib.import_module(module_path)
    cls = getattr(mod, cls_name)
    assert getattr(cls, "always_on", False) is True, (
        f"{cls_name} must declare ``always_on = True`` so the registry can "
        f"derive its always-on status without a hardcoded list (#1003)."
    )


def test_get_dynamic_sets_empty_registry_returns_empty_sets():
    """An empty registry is a valid SCOPE — yields empty sets, not legacy fallback."""
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", {}):
        always_on, known = _get_dynamic_sets()
    assert always_on == frozenset()
    assert known == frozenset()


def test_get_dynamic_sets_derives_always_on_from_class_attribute():
    """When the registry is populated, always-on is derived purely from class attrs."""

    class _AlwaysOn:
        always_on = True

    class _Togglable:
        pass

    stub = {
        "iam": SimpleNamespace(cls=_AlwaysOn, instance=None),
        "auth": SimpleNamespace(cls=_AlwaysOn, instance=None),
        "stac": SimpleNamespace(cls=_Togglable, instance=None),
    }
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        always_on, known = _get_dynamic_sets()

    assert always_on == frozenset({"iam", "auth"})
    assert known == frozenset({"iam", "auth", "stac"})


def test_get_dynamic_sets_no_legacy_union():
    """A name not in the registry is NOT silently treated as always-on (#1003).

    The previous behaviour unioned ``ALWAYS_ON_EXTENSIONS`` into the result
    as a safety net; that hid SCOPE bugs (e.g. forgetting to install a wheel)
    and contradicted DynaStore's framework-is-pyproject-driven invariant.
    """

    class _Togglable:
        pass

    # ``iam`` is the canonical always-on name in legacy thinking — pinning
    # that the registry doesn't conjure it out of thin air is the point.
    stub = {"stac": SimpleNamespace(cls=_Togglable, instance=None)}
    with patch("dynastore.extensions.registry._DYNASTORE_EXTENSIONS", stub):
        always_on, known = _get_dynamic_sets()

    assert always_on == frozenset()
    assert known == frozenset({"stac"})
    assert "iam" not in always_on
    assert "iam" not in known
