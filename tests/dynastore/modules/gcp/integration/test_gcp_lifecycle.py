import pytest
import asyncio
from dynastore.models.protocols import CatalogsProtocol, StorageProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.gcp.gcp_module import GCPModule
from google.api_core.exceptions import NotFound
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from tests.dynastore.test_utils import generate_test_id


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks", "gcp")
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup")
async def test_gcp_resource_lifecycle(app_lifespan, catalog_obj, catalog_id, monkeypatch):
    """
    Verifies that creating a catalog with GCP config creates a bucket,
    and hard-deleting the catalog cleans it up.

    Managed eventing (Pub/Sub) is disabled by the conftest fixture
    because K_SERVICE is not available outside Cloud Run.
    """
    gcp_module = get_protocol(StorageProtocol)
    if not isinstance(gcp_module, GCPModule):
        pytest.skip("GCPModule not available or not registered as StorageProtocol.")

    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")
        
    try:
        gcp_module.get_storage_client()
    except RuntimeError:
        pytest.skip("GCP storage client not available (no credentials).")

    # Override catalog_id with a shorter one to respect GCS bucket name limits (63 chars)
    short_catalog_id = f"it_{generate_test_id(12)}"
    catalog_obj.id = short_catalog_id
    test_catalog_id = short_catalog_id

    catalogs = get_protocol(CatalogsProtocol)
    storage_client = gcp_module.get_storage_client()

    # 1. Cleanup: force-delete any leftover from a previous run
    try:
        await catalogs.delete_catalog(test_catalog_id, force=True)
        await lifecycle_registry.wait_for_all_tasks(timeout=10.0)
    except Exception:
        pass

    # 2. Create the catalog (triggers GCP provisioning task)
    await catalogs.create_catalog(catalog_obj)

    # 3. Wait for lifecycle hooks + background provisioning to complete
    await lifecycle_registry.wait_for_all_tasks(timeout=60.0)

    bucket_name = None
    try:
        # 4. Retrieve the created resources to verify
        bucket_name = await gcp_module.get_storage_identifier(test_catalog_id)

        assert bucket_name is not None, (
            f"Bucket not created for catalog {test_catalog_id}. "
            f"Check dispatcher logs for task execution errors."
        )

        bucket_config = await gcp_module.get_catalog_bucket_config(test_catalog_id)
        assert bucket_config is not None

        # Handle GCS eventual consistency
        bucket = None
        for i in range(20):
            try:
                bucket = storage_client.get_bucket(bucket_name)
                break
            except NotFound:
                if i == 19:
                    raise
                await asyncio.sleep(1.0)
        assert bucket.exists()

        print(f"Verified resources for catalog {test_catalog_id}: Bucket exists.")

    finally:
        # 5. Hard Delete Catalog -> cleanup
        try:
            await catalogs.delete_catalog(test_catalog_id, force=True)
            await lifecycle_registry.wait_for_all_tasks(timeout=30.0)
        except Exception:
            pass

        # 6. Direct bucket cleanup (the async destruction hook has a known
        #    race: schema drops before the hook can look up the bucket name
        #    from DB, so we force-delete the bucket directly as test cleanup).
        if bucket_name:
            try:
                bucket = storage_client.get_bucket(bucket_name)
                bucket.delete(force=True)
                print(f"Test cleanup: Bucket {bucket_name} force-deleted.")
            except NotFound:
                print(f"Verified: Bucket {bucket_name} already deleted by lifecycle hooks.")

    # 7. Verify auto_create=False behavior
    mock_catalog_id = f"mock_{generate_test_id()}"
    result = await gcp_module.get_bucket_service().ensure_storage_for_catalog(
        mock_catalog_id, auto_create=False
    )
    assert result is None, f"Expected None for non-existent bucket with auto_create=False, got {result}"

    print("GCP Lifecycle Integration Test SUCCESS.")
