import pytest
import pytest_asyncio
import os


@pytest.fixture
def dynastore_modules(dynastore_modules):
    return dynastore_modules + ["gcp"]


@pytest.fixture
def dynastore_extensions(dynastore_extensions):
    return dynastore_extensions + ["gcp_bucket"]


@pytest_asyncio.fixture(autouse=True)
async def disable_managed_eventing(app_lifespan):
    """
    Sets a platform-level GcpEventingConfig with managed eventing DISABLED.

    Locally there is no Cloud Run service (K_SERVICE), so Pub/Sub push
    subscriptions cannot be created.  Tests that specifically need eventing
    must set K_SERVICE/SERVICE_URL and override this config explicitly.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.gcp.gcp_config import (
        GcpEventingConfig,
        ManagedBucketEventing,
    )

    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GcpEventingConfig,
            GcpEventingConfig(
                managed_eventing=ManagedBucketEventing(enabled=False),
            ),
        )
