"""Verify GCPModule async clients are built lazily, bound to the running loop.

Production incident (2026-04-22, startup path):

    RuntimeError: Task <Task ... LifespanOn.main() ...> got Future
    <Task ... InterceptedUnaryUnaryCall._invoke() ...> attached to a
    different loop

Root cause: `run_v2.JobsAsyncClient(...)` was constructed in sync
`__init__` / `reinitialize_clients` — grpc.aio channels capture
`asyncio.get_event_loop()` at construction time, and that loop is a
throwaway (or the wrong one) when module discovery runs before uvicorn
starts.

Fix: async clients are now built lazily in `get_jobs_client` /
`get_run_client`, and rebuilt if the running loop changes.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.gcp.gcp_module import GCPModule


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Override the autouse fixture from gcp/conftest.py which depends on
    ``app_lifespan`` (→ ``wait_for_db``). These tests are pure in-memory and
    have no need for a live DB or full app startup."""
    return None


def _make_module_with_credentials() -> GCPModule:
    """Construct a GCPModule bypassing get_credentials() I/O."""
    fake_creds = MagicMock(name="credentials")
    with patch(
        "dynastore.modules.gcp.gcp_module.get_credentials",
        return_value=(fake_creds, MagicMock(name="identity")),
    ), patch("dynastore.modules.gcp.gcp_module.storage.Client"), \
       patch("dynastore.modules.gcp.gcp_module.pubsub_v1.PublisherClient"), \
       patch("dynastore.modules.gcp.gcp_module.pubsub_v1.SubscriberClient"):
        mod = GCPModule(app_state=MagicMock())
    # Project id / region are only used for logging inside reinitialize_clients.
    return mod


def test_async_clients_not_created_in_init():
    """The bug: sync __init__ used to build async clients → wrong loop.

    Assert they are None until first async access.
    """
    mod = _make_module_with_credentials()
    assert mod._jobs_client is None
    assert mod._run_client is None
    assert mod._async_clients_loop is None


@pytest.mark.asyncio
async def test_get_jobs_client_builds_lazily_on_running_loop():
    mod = _make_module_with_credentials()

    fake_jobs = MagicMock(name="JobsAsyncClient")
    fake_run = MagicMock(name="ServicesAsyncClient")
    with patch(
        "dynastore.modules.gcp.gcp_module.run_v2.JobsAsyncClient",
        return_value=fake_jobs,
    ), patch(
        "dynastore.modules.gcp.gcp_module.run_v2.ServicesAsyncClient",
        return_value=fake_run,
    ):
        got = mod.get_jobs_client()

    assert got is fake_jobs
    assert mod._jobs_client is fake_jobs
    assert mod._run_client is fake_run
    # Tagged with the running loop.
    assert mod._async_clients_loop is asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_second_call_same_loop_reuses_client():
    mod = _make_module_with_credentials()

    with patch(
        "dynastore.modules.gcp.gcp_module.run_v2.JobsAsyncClient",
    ) as jobs_ctor, patch(
        "dynastore.modules.gcp.gcp_module.run_v2.ServicesAsyncClient",
    ) as run_ctor:
        mod.get_jobs_client()
        mod.get_jobs_client()
        mod.get_run_client()

    # Exactly one construction per client type — no re-binding loop-jitter.
    assert jobs_ctor.call_count == 1
    assert run_ctor.call_count == 1


def test_get_jobs_client_without_running_loop_raises():
    """Accessor called from a sync frame with no running loop must not
    silently build against a throwaway loop — that reintroduces the bug.
    """
    mod = _make_module_with_credentials()

    with patch(
        "dynastore.modules.gcp.gcp_module.run_v2.JobsAsyncClient",
    ), patch("dynastore.modules.gcp.gcp_module.run_v2.ServicesAsyncClient"):
        with pytest.raises(RuntimeError, match="no running event loop"):
            mod.get_jobs_client()


@pytest.mark.asyncio
async def test_client_rebuilt_when_running_loop_changes():
    """Simulate a lifespan restart that runs on a different loop.

    Force a loop-id mismatch and confirm the next accessor rebuilds the
    clients instead of returning stale ones bound to the old loop.
    """
    mod = _make_module_with_credentials()

    with patch(
        "dynastore.modules.gcp.gcp_module.run_v2.JobsAsyncClient",
    ) as jobs_ctor, patch(
        "dynastore.modules.gcp.gcp_module.run_v2.ServicesAsyncClient",
    ) as run_ctor:
        mod.get_jobs_client()  # built on loop A
        # Simulate that the loop changed (e.g. new uvicorn lifespan).
        mod._async_clients_loop = object()  # sentinel != get_running_loop()
        mod.get_jobs_client()  # should rebuild
        assert jobs_ctor.call_count == 2
        assert run_ctor.call_count == 2
