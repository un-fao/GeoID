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

"""Tests for the neutral PresetProtocol and that existing preset classes satisfy it."""
from __future__ import annotations

from typing import ClassVar
from pydantic import BaseModel

from dynastore.modules.presets.protocol import PresetProtocol
from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams, PresetContext


class _MinimalPreset:
    """Minimal class satisfying PresetProtocol for structural-subtype checks."""

    name: ClassVar[str] = "minimal_test_preset"
    params_model: ClassVar[type[BaseModel]] = NoParams

    async def apply(self, params: BaseModel, scope_key: str, ctx: object) -> AppliedDescriptor:
        return AppliedDescriptor()

    async def revoke(self, applied_descriptor: object, ctx: object) -> None:
        return None


def test_minimal_preset_satisfies_protocol() -> None:
    assert isinstance(_MinimalPreset(), PresetProtocol)


def test_storage_preset_satisfies_protocol() -> None:
    """BundlePreset-based storage presets satisfy the neutral protocol."""
    from dynastore.modules.storage.presets.public_catalog import PublicCatalogPreset
    assert isinstance(PublicCatalogPreset(), PresetProtocol)


def test_iam_preset_satisfies_protocol() -> None:
    """IAM presets satisfy the neutral protocol."""
    from dynastore.modules.iam.presets.public_access_baseline import PublicAccessBaseline
    from dynastore.modules.iam.presets.default_roles_baseline import DefaultRolesBaseline
    assert isinstance(PublicAccessBaseline(), PresetProtocol)
    assert isinstance(DefaultRolesBaseline(), PresetProtocol)


def test_protocol_requires_name_attribute() -> None:
    """A class without ``name`` does not satisfy PresetProtocol."""

    class _NoName:
        params_model: ClassVar[type[BaseModel]] = NoParams

        async def apply(self, params: BaseModel, scope_key: str, ctx: object) -> AppliedDescriptor:
            return AppliedDescriptor()

        async def revoke(self, applied_descriptor: object, ctx: object) -> None:
            return None

    assert not isinstance(_NoName(), PresetProtocol)


def test_protocol_requires_apply_method() -> None:
    """A class without ``apply`` does not satisfy PresetProtocol."""

    class _NoApply:
        name: ClassVar[str] = "no_apply"
        params_model: ClassVar[type[BaseModel]] = NoParams

        async def revoke(self, applied_descriptor: object, ctx: object) -> None:
            return None

    assert not isinstance(_NoApply(), PresetProtocol)
