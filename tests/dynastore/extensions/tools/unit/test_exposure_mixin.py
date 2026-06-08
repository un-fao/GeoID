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

"""Tests for the service-exposure control panel mixin and registry helpers."""

from pydantic import BaseModel
from dynastore.extensions.tools.exposure_mixin import (
    ExposableConfigMixin,
    _get_dynamic_sets,
)


def test_mixin_defaults_enabled_true():
    class C(ExposableConfigMixin, BaseModel):
        pass
    assert C().enabled is True


def test_mixin_respects_explicit_false():
    class C(ExposableConfigMixin, BaseModel):
        pass
    assert C(enabled=False).enabled is False


def test_always_on_is_subset_of_known():
    """Live registry invariant: every always-on extension is also known."""
    always_on, known = _get_dynamic_sets()
    assert always_on.issubset(known)


def test_always_on_includes_core_controlplane_when_installed():
    """The five names below have registered extension entry-points AND declare
    ``always_on = True`` on their class.  "documentation" and "tools" used
    to be in the legacy ``ALWAYS_ON_EXTENSIONS`` set but had no registered
    entry-point — they were dropped in #1003 as part of the
    inverted-dependency cleanup.

    Conditional: only asserts on extensions actually installed in the active
    SCOPE.  Reflects #1003's framework-is-pyproject-driven invariant — a
    minimal SCOPE may legitimately exclude any of these.
    """
    always_on, known = _get_dynamic_sets()
    for e in {"iam", "auth", "configs", "web", "admin"}:
        if e in known:
            assert e in always_on, (
                f"{e!r} is discovered but does not declare ``always_on = True`` "
                "on its class — every core control-plane extension must opt in."
            )
