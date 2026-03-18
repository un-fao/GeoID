import pytest
import pytest_asyncio
import os
from pathlib import Path

_CREDS_FILE = Path(__file__).parents[5] / "src" / "dynastore" / "modules" / "gcp" / "application_default_credentials_geospatial-review.json"
_REAL_PROJECT = "fao-aip-geospatial-review"
_REAL_REGION = "europe-west1"


@pytest.fixture(scope="session", autouse=True)
def gcp_integration_env():
    """Set GCP env vars from local credential file if present.

    In CI, credentials come from env vars or metadata server.
    The ``@pytest.mark.gcp`` marker handles skipping when no creds exist.
    """
    if _CREDS_FILE.exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(_CREDS_FILE))
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", _REAL_PROJECT)
        os.environ.setdefault("REGION", _REAL_REGION)


@pytest_asyncio.fixture
async def catalog_cleaner(app_lifespan):
    """
    Tracks catalog IDs created during a test and force-deletes them on teardown.
    Usage: add ``catalog_cleaner`` to the test signature, then call
    ``catalog_cleaner(catalog_id)`` after each successful catalog creation.
    """
    ids: list = []
    yield ids.append

    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs:
        for cid in ids:
            try:
                await catalogs.delete_catalog(cid, force=True)
            except Exception:
                pass
        await lifecycle_registry.wait_for_all_tasks()
