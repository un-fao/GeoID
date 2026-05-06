"""Cycle F.4d.2 — pin the PATCH composer's multi-instance ref support.

When the operator sends a body whose top-level key is NOT a registered
config class_key, the composer must recognise the entry as a multi-
instance ref:

* Set: requires ``class_key`` (or ``driver_class``) discriminator inside
  the body; the composer dispatches to ``set_config_by_ref`` (F.4c.4).
* Delete (``value=null``): dispatches to ``delete_config_by_ref``; an
  unknown ref is a no-op (no error).
* Validation: the discriminator is stripped before model_validate so it
  doesn't pollute the payload; missing/unknown discriminators on a set
  raise a clear ValueError.

Class-keyed entries (existing F.2-era semantics) take the existing
class-keyed path with ``set_config`` / ``delete_config``.

Tests use a mock ConfigsProtocol — no DB hit.  Round-trip against
F.4c.1 storage was pinned in PR #356.
"""

import dynastore.modules.tiles  # noqa: F401  -- registers TilesConfig

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict


def _make_ref_aware_service():
    """Mock ConfigsProtocol implementing F.4c.2 read + F.4c.4 write APIs."""
    svc = MagicMock()
    refs: Dict[str, Any] = {}

    async def _get_config_by_ref(ref_key, catalog_id=None, collection_id=None, **_):
        return refs.get(ref_key)

    async def _set_config_by_ref(ref_key, config, catalog_id=None, collection_id=None, **_):
        refs[ref_key] = config

    async def _delete_config_by_ref(ref_key, catalog_id=None, collection_id=None, **_):
        return refs.pop(ref_key, None) is not None

    # Class-keyed APIs (passthrough — patch_config calls them on the
    # canonical path even though our tests focus on the ref path).
    async def _get_persisted_config(cls, catalog_id=None, collection_id=None, **_):
        return None

    async def _set_config(cls, value, catalog_id=None, collection_id=None, **_):
        pass

    async def _delete_config(cls, catalog_id=None, collection_id=None, **_):
        pass

    svc.get_config_by_ref = AsyncMock(side_effect=_get_config_by_ref)
    svc.set_config_by_ref = AsyncMock(side_effect=_set_config_by_ref)
    svc.delete_config_by_ref = AsyncMock(side_effect=_delete_config_by_ref)
    svc.get_persisted_config = AsyncMock(side_effect=_get_persisted_config)
    svc.set_config = AsyncMock(side_effect=_set_config)
    svc.delete_config = AsyncMock(side_effect=_delete_config)
    return svc, refs


@pytest.mark.asyncio
async def test_patch_creates_new_multi_instance_ref():
    """Body with an unknown key + ``class_key`` discriminator dispatches
    to set_config_by_ref."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, refs = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    result = await api.patch_config(
        catalog_id=None,
        body={
            "tiles_secondary": {
                "class_key": "tiles_config",
                "min_zoom": 5,
                "max_zoom": 15,
            },
        },
    )

    assert "tiles_secondary" in result["updated"]
    svc.set_config_by_ref.assert_awaited_once()
    # Class-keyed setter not called.
    svc.set_config.assert_not_called()
    # Discriminator stripped before validation — payload should NOT carry
    # ``class_key`` after the round-trip.
    written = refs["tiles_secondary"]
    assert getattr(written, "min_zoom", None) == 5
    assert getattr(written, "max_zoom", None) == 15


@pytest.mark.asyncio
async def test_patch_accepts_driver_class_alias_for_discriminator():
    """``driver_class`` is an alias for ``class_key`` (F.2 driver-config
    naming convention)."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, refs = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    await api.patch_config(
        catalog_id=None,
        body={
            "tiles_alias": {
                "driver_class": "tiles_config",
                "min_zoom": 1,
            },
        },
    )

    svc.set_config_by_ref.assert_awaited_once()
    assert "tiles_alias" in refs


@pytest.mark.asyncio
async def test_patch_unknown_ref_without_discriminator_raises():
    """Operators get a clear error when the body is a fresh ref with no
    discriminator — the composer can't guess which class to validate."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, _ = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    with pytest.raises(ValueError, match=r"must include 'class_key'"):
        await api.patch_config(
            catalog_id=None,
            body={"tiles_orphan": {"min_zoom": 7}},
        )


@pytest.mark.asyncio
async def test_patch_unknown_ref_with_unknown_discriminator_raises():
    """A discriminator that doesn't match a registered class is a clear
    422 — preserves the canonical class-key contract."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, _ = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    with pytest.raises(ValueError, match=r"is not a registered config class"):
        await api.patch_config(
            catalog_id=None,
            body={
                "some_ref": {
                    "class_key": "class_that_does_not_exist",
                    "x": 1,
                },
            },
        )


@pytest.mark.asyncio
async def test_patch_delete_unknown_ref_is_noop():
    """``{ref: null}`` for an unknown ref dispatches a delete that the
    service-layer reports as no-op (False) — no error surfaced."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, _ = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    result = await api.patch_config(
        catalog_id=None,
        body={"unknown_ref": None},
    )

    assert "unknown_ref" in result["updated"]
    svc.delete_config_by_ref.assert_awaited_once()


@pytest.mark.asyncio
async def test_patch_class_keyed_path_preserved():
    """Body keys matching a registered class_key keep the existing
    set_config / delete_config dispatch — no regression for single-
    instance writes (the F.2-era semantics)."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    svc, _ = _make_ref_aware_service()
    api = ConfigApiService(config_service=svc)

    await api.patch_config(
        catalog_id=None,
        body={"tiles_config": {"min_zoom": 3}},
    )

    svc.set_config.assert_awaited_once()
    svc.set_config_by_ref.assert_not_called()
