"""Unit tests for GCP provisioning task error-handling contract.

Regression guard for the 2026-04-22 production incident where both
"StorageProtocol not available" (Mode 1) and "Bucket name returned as None"
(Mode 2) were incorrectly classified as PermanentTaskFailure, causing every
catalog after the first to be permanently stuck in 'failed' state with zero
retries consumed.

Expected behaviour after the fix:
- Transient errors (module unavailable, GCS conflicts) → plain RuntimeError
  so the dispatcher increments retry_count and tries again.
- Permanent errors (bad credentials, client init failure) → PermanentTaskFailure
  so the dispatcher dead-letters the task immediately.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.tasks.gcp_provision.task import (
    _get_storage_protocol,
    ProvisioningTask,
)
from dynastore.modules.tasks.models import PermanentTaskFailure


# ---------------------------------------------------------------------------
# _get_storage_protocol — must raise RuntimeError (retryable), never
# PermanentTaskFailure, when no StorageProtocol is registered
# ---------------------------------------------------------------------------


def test_get_storage_protocol_raises_runtime_error_when_unavailable():
    """StorageProtocol missing → RuntimeError (dispatcher retries), not PermanentTaskFailure."""
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=None
    ):
        with pytest.raises(RuntimeError, match="StorageProtocol not available"):
            _get_storage_protocol()


def test_get_storage_protocol_not_permanent_failure():
    """StorageProtocol missing must NOT raise PermanentTaskFailure."""
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=None
    ):
        with pytest.raises(Exception) as exc_info:
            _get_storage_protocol()
        assert not isinstance(exc_info.value, PermanentTaskFailure), (
            "StorageProtocol unavailable should be retryable, not permanent"
        )


def test_get_storage_protocol_returns_instance_when_available():
    mock_storage = MagicMock()
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=mock_storage
    ):
        result = _get_storage_protocol()
    assert result is mock_storage


# ---------------------------------------------------------------------------
# ProvisioningTask.run — permanent vs retryable classification
# ---------------------------------------------------------------------------


def _make_payload(catalog_id: str = "test_cat"):
    payload = MagicMock()
    payload.inputs.catalog_id = catalog_id
    return payload


@pytest.mark.asyncio
async def test_credentials_error_is_permanent():
    """RuntimeError containing 'credentials' → PermanentTaskFailure + _mark_failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError("GCPModule credentials not available"),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(PermanentTaskFailure):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_awaited_once_with("c1", "failed")


@pytest.mark.asyncio
async def test_client_init_error_is_permanent():
    """'failed to create a storage client' → PermanentTaskFailure + _mark_failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError(
                "GCPModule has not been initialized or failed to create a storage client"
            ),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(PermanentTaskFailure):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_awaited_once_with("c1", "failed")


@pytest.mark.asyncio
async def test_storage_protocol_unavailable_is_retryable():
    """'StorageProtocol not available' → plain RuntimeError, catalog NOT marked failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError(
                "StorageProtocol not available - GCP module not loaded"
            ),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="StorageProtocol not available"):
            await task.run(_make_payload("c1"))

    # Catalog must NOT be marked failed — leave in 'provisioning' so retry can succeed
    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_bucket_name_none_is_retryable():
    """'Bucket name returned as None' → retryable RuntimeError, catalog NOT marked failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    setup_gcp = AsyncMock(
        side_effect=RuntimeError(
            "Failed to provision storage for catalog 'c1': Bucket name returned as None."
        )
    )
    mock_storage.setup_catalog_gcp_resources = setup_gcp

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="Bucket name returned as None"):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_lifespan_not_ready_is_retryable():
    """'GCPModule has not been initialized' (no bucket service yet) → retryable."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError("GCPModule has not been initialized."),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="not been initialized"):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_successful_provision_marks_ready():
    """Happy path: bucket provisioned → catalog marked 'ready'."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.setup_catalog_gcp_resources = AsyncMock(
        return_value=("d88971-test-catalog-ok", {})
    )

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        result = await task.run(_make_payload("c1"))

    assert result["status"] == "ready"
    assert result["bucket_name"] == "d88971-test-catalog-ok"
    mock_catalogs.update_provisioning_status.assert_awaited_once_with("c1", "ready")


# ---------------------------------------------------------------------------
# Protocol-gate declaration — regression guard
# ---------------------------------------------------------------------------


def test_provisioning_task_declares_storage_protocol():
    """ProvisioningTask must declare StorageProtocol as required (prevents wrong service claiming it)."""
    from dynastore.models.protocols import StorageProtocol
    assert StorageProtocol in ProvisioningTask.required_protocols


def test_provisioning_task_unsatisfied_without_storage():
    """ProvisioningTask.are_protocols_satisfied() returns False when StorageProtocol unavailable."""
    task = ProvisioningTask()
    with patch("dynastore.tools.discovery.get_all_protocols", return_value=[]):
        assert task.are_protocols_satisfied() is False


# ---------------------------------------------------------------------------
# GcpCatalogProvisioning sub-Protocol — capability gating
# ---------------------------------------------------------------------------


def test_gcp_module_satisfies_gcp_catalog_provisioning():
    """GCPModule must structurally satisfy the GcpCatalogProvisioning sub-Protocol
    so the typed isinstance dispatch in ProvisioningTask.run picks the
    combined bucket+eventing setup path instead of the cross-vendor fallback.
    """
    from dynastore.modules.gcp.gcp_module import GCPModule
    from dynastore.models.protocols import GcpCatalogProvisioning

    assert issubclass(GCPModule, GcpCatalogProvisioning)


def test_storage_without_setup_method_falls_back():
    """A StorageProtocol implementation that does NOT satisfy
    GcpCatalogProvisioning must NOT pass the isinstance check —
    callers fall back to ensure_storage_for_catalog + setup_catalog_eventing.
    """
    from dynastore.models.protocols import GcpCatalogProvisioning

    class MinimalStorage:
        async def ensure_storage_for_catalog(self, catalog_id, conn=None):
            return f"bucket-{catalog_id}"

    assert not isinstance(MinimalStorage(), GcpCatalogProvisioning)


@pytest.mark.asyncio
async def test_destroy_task_invokes_typed_destruction():
    """GcpDestroyCatalogTask must call EventingProtocol.teardown_catalog_eventing
    AND StorageProtocol.delete_storage_for_catalog directly — previously these
    were getattr-dispatched to non-existent methods and silently no-opped.
    Path A bug-fix regression guard.
    """
    from dynastore.tasks.gcp_provision.task import GcpDestroyCatalogTask

    mock_storage = MagicMock()
    mock_storage.delete_storage_for_catalog = AsyncMock(return_value=True)

    mock_eventing = MagicMock()
    mock_eventing.teardown_catalog_eventing = AsyncMock(return_value=None)

    def _get_protocol_dispatch(proto):
        from dynastore.models.protocols import EventingProtocol
        if proto is EventingProtocol:
            return mock_eventing
        return None

    task = GcpDestroyCatalogTask()
    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task.get_protocol",
            side_effect=_get_protocol_dispatch,
        ),
    ):
        result = await task.run(_make_payload("destroy_test_cat"))

    mock_eventing.teardown_catalog_eventing.assert_awaited_once_with("destroy_test_cat")
    mock_storage.delete_storage_for_catalog.assert_awaited_once_with("destroy_test_cat")
    assert result["status"] == "destroyed"
