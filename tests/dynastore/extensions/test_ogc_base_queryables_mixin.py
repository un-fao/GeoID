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

"""Unit tests for ``OGCServiceMixin._collect_queryable_fields`` and the
``register_ogc_preset`` contributor-name fix.

All collaborators are mocked; no database is touched.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.ogc_base import OGCServiceMixin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Svc(OGCServiceMixin):
    """Minimal concrete subclass for exercising the mixin in isolation."""


def _make_schema_entry(name: str):
    """Return an object whose ``.name`` attribute equals *name*."""
    entry = MagicMock()
    entry.name = name
    return entry


# ---------------------------------------------------------------------------
# _collect_queryable_fields — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collect_queryable_fields_returns_columns_and_driver_fields():
    """When driver has INTROSPECTION and get_entity_fields, both are returned."""
    from dynastore.models.protocols.storage_driver import Capability

    driver = AsyncMock()
    driver.capabilities = {Capability.INTROSPECTION}
    driver.introspect_schema = AsyncMock(
        return_value=[_make_schema_entry("col_a"), _make_schema_entry("col_b")]
    )
    sentinel_fields = object()
    driver.get_entity_fields = AsyncMock(return_value=sentinel_fields)

    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=driver),
    ):
        svc = _Svc()
        columns, driver_fields = await svc._collect_queryable_fields(
            "cat1", "col1", conn=object()
        )

    assert columns == ["col_a", "col_b"]
    assert driver_fields is sentinel_fields


@pytest.mark.asyncio
async def test_collect_queryable_fields_no_get_entity_fields():
    """When driver lacks get_entity_fields, driver_fields is None."""
    from dynastore.models.protocols.storage_driver import Capability

    driver = AsyncMock()
    driver.capabilities = {Capability.INTROSPECTION}
    driver.introspect_schema = AsyncMock(
        return_value=[_make_schema_entry("geom"), _make_schema_entry("name")]
    )
    # Ensure driver does NOT have get_entity_fields
    del driver.get_entity_fields

    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=driver),
    ):
        svc = _Svc()
        columns, driver_fields = await svc._collect_queryable_fields(
            "cat1", "col1", conn=object()
        )

    assert columns == ["geom", "name"]
    assert driver_fields is None


# ---------------------------------------------------------------------------
# _collect_queryable_fields — get_entity_fields raises → degrades + logs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collect_queryable_fields_entity_fields_error_logs_debug(caplog):
    """get_entity_fields raising must log at DEBUG and degrade to driver_fields=None."""
    from dynastore.models.protocols.storage_driver import Capability

    driver = AsyncMock()
    driver.capabilities = {Capability.INTROSPECTION}
    driver.introspect_schema = AsyncMock(
        return_value=[_make_schema_entry("x")]
    )
    driver.get_entity_fields = AsyncMock(side_effect=RuntimeError("entity boom"))

    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=driver),
    ):
        svc = _Svc()
        with caplog.at_level(logging.DEBUG, logger="dynastore.extensions.ogc_base"):
            columns, driver_fields = await svc._collect_queryable_fields(
                "cat2", "col2", conn=object()
            )

    assert columns == ["x"]
    assert driver_fields is None
    assert any("queryables field introspection failed" in r.message for r in caplog.records)
    assert any("cat2" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _collect_queryable_fields — no INTROSPECTION capability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collect_queryable_fields_no_introspection_capability():
    """Driver without INTROSPECTION capability yields ([], None)."""
    driver = AsyncMock()
    # Empty capability set — INTROSPECTION is absent
    driver.capabilities = set()

    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=driver),
    ):
        svc = _Svc()
        columns, driver_fields = await svc._collect_queryable_fields(
            "cat3", "col3", conn=object()
        )

    assert columns == []
    assert driver_fields is None
    driver.introspect_schema.assert_not_called()


# ---------------------------------------------------------------------------
# _collect_queryable_fields — outer exception (get_driver raises)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collect_queryable_fields_get_driver_raises_logs_debug(caplog):
    """When get_driver raises, the method logs at DEBUG and returns ([], None)."""
    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(side_effect=RuntimeError("router down")),
    ):
        svc = _Svc()
        with caplog.at_level(logging.DEBUG, logger="dynastore.extensions.ogc_base"):
            columns, driver_fields = await svc._collect_queryable_fields(
                "cat4", "col4", conn=object()
            )

    assert columns == []
    assert driver_fields is None
    assert any("queryables field introspection failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _collect_queryable_fields — None driver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collect_queryable_fields_none_driver_returns_empty():
    """When get_driver returns None, yield ([], None) without error."""
    with patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=None),
    ):
        svc = _Svc()
        columns, driver_fields = await svc._collect_queryable_fields(
            "cat5", "col5", conn=object()
        )

    assert columns == []
    assert driver_fields is None


# ---------------------------------------------------------------------------
# register_ogc_preset — contributor __name__ / __qualname__ per service
# ---------------------------------------------------------------------------


def test_register_ogc_preset_contributor_name_is_per_service(monkeypatch):
    """Each register_ogc_preset call yields a distinct contributor class name."""
    registered: dict = {}

    class _FakePreset:
        def __init__(self, *, name, description, keywords, contributor_factory):
            self.name = name
            self._contributor_factory = contributor_factory

    def _fake_register(preset):
        registered[preset.name] = preset

    monkeypatch.setattr(
        "dynastore.modules.storage.presets.registry.register_preset",
        _fake_register,
    )
    monkeypatch.setattr(
        "dynastore.modules.storage.presets.policy_contributor_adapter.PolicyContributorPreset",
        _FakePreset,
    )

    OGCServiceMixin.register_ogc_preset(
        name="alpha_enable",
        description="Alpha preset",
        keywords=("alpha",),
        policies_factory=lambda: [],
        role_bindings_factory=lambda: [],
    )
    OGCServiceMixin.register_ogc_preset(
        name="beta_enable",
        description="Beta preset",
        keywords=("beta",),
        policies_factory=lambda: [],
        role_bindings_factory=lambda: [],
    )

    alpha_cls = registered["alpha_enable"]._contributor_factory
    beta_cls = registered["beta_enable"]._contributor_factory

    assert alpha_cls.__name__ == "alpha_enablePresetContributor"
    assert alpha_cls.__qualname__ == "alpha_enablePresetContributor"
    assert beta_cls.__name__ == "beta_enablePresetContributor"
    assert beta_cls.__qualname__ == "beta_enablePresetContributor"
    # The two classes must have distinct names.
    assert alpha_cls.__name__ != beta_cls.__name__
