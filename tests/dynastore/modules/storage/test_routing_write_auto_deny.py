"""Issue #480 — auto-fire ``_apply_deny_policy`` / ``_revoke_deny_policy``
when an items routing-config write pins (or removes) the private items
driver. Closes the gap between provisioning (``ensure_storage``) and the
cold-boot scan (``_restore_deny_policies``).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    _sync_deny_policy_for_catalog,
)


def _routing_with_private() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    secondary_index=True,
                ),
            ],
        },
    )


def _routing_without_private() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
        },
    )


@pytest.mark.asyncio
async def test_sync_applies_deny_when_private_driver_pinned():
    apply_mock = AsyncMock()
    revoke_mock = AsyncMock()

    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._apply_deny_policy",
        apply_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._revoke_deny_policy",
        revoke_mock,
    ):
        await _sync_deny_policy_for_catalog(_routing_with_private(), "cat-a")

    apply_mock.assert_awaited_once_with("cat-a")
    revoke_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_revokes_deny_when_no_private_driver_and_no_private_collection():
    apply_mock = AsyncMock()
    revoke_mock = AsyncMock()
    has_private_mock = AsyncMock(return_value=False)

    fake_catalogs = MagicMock()
    fake_configs = MagicMock()

    def _get_protocol(p):
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        if p is CatalogsProtocol:
            return fake_catalogs
        if p is ConfigsProtocol:
            return fake_configs
        return None

    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._apply_deny_policy",
        apply_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._revoke_deny_policy",
        revoke_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._catalog_has_private_collection",
        has_private_mock,
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        await _sync_deny_policy_for_catalog(_routing_without_private(), "cat-b")

    apply_mock.assert_not_awaited()
    revoke_mock.assert_awaited_once_with("cat-b")


@pytest.mark.asyncio
async def test_sync_does_not_revoke_when_other_private_collection_remains():
    """Catalog still has a private sibling collection — DENY must stay."""
    apply_mock = AsyncMock()
    revoke_mock = AsyncMock()
    has_private_mock = AsyncMock(return_value=True)

    fake_catalogs = MagicMock()
    fake_configs = MagicMock()

    def _get_protocol(p):
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        if p is CatalogsProtocol:
            return fake_catalogs
        if p is ConfigsProtocol:
            return fake_configs
        return None

    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._apply_deny_policy",
        apply_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._revoke_deny_policy",
        revoke_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._catalog_has_private_collection",
        has_private_mock,
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        await _sync_deny_policy_for_catalog(_routing_without_private(), "cat-c")

    apply_mock.assert_not_awaited()
    revoke_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_is_no_op_when_protocols_unavailable():
    """Discovery not ready (early lifecycle): bail silently. The cold-boot
    scan + next ensure_storage are the recovery paths."""
    apply_mock = AsyncMock()
    revoke_mock = AsyncMock()

    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._apply_deny_policy",
        apply_mock,
    ), patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._revoke_deny_policy",
        revoke_mock,
    ), patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ):
        await _sync_deny_policy_for_catalog(_routing_without_private(), "cat-d")

    apply_mock.assert_not_awaited()
    revoke_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_swallows_unexpected_failures():
    """A surprise from the inner helpers must not propagate — apply
    handlers run inside config-write transactions."""
    with patch(
        "dynastore.modules.storage.drivers.elasticsearch_private.driver."
        "ItemsElasticsearchPrivateDriver._apply_deny_policy",
        side_effect=RuntimeError("boom"),
    ):
        # No exception should escape.
        await _sync_deny_policy_for_catalog(_routing_with_private(), "cat-e")
