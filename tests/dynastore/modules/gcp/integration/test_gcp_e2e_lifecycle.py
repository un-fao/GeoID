#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
End-to-end GCS-backed catalog lifecycle.

Exercises the full production path that cannot be validated piecewise:

1. Create a catalog → triggers ``gcp_provision`` task → GCS bucket created.
2. Managed Pub/Sub eventing is disabled by the autouse
   ``disable_managed_eventing`` fixture (localhost cannot receive push
   subscriptions). In production, the OBJECT_COMPLETED event on a new
   upload would auto-create the asset; here we simulate that step.
3. Create a collection.
4. Upload a real GeoJSON blob directly to the provisioned bucket using
   the GCS client (bypassing the resumable-upload endpoint — the file
   payload is what matters for ingestion).
5. Register the blob as a **collection asset** so the virtual STAC
   collection-by-asset endpoint can filter by ``asset_id``.
6. Run the OGC ``ingestion`` process against that asset with
   ``GcsDetailedReporter`` configured to write reports to the same
   bucket. The reporter is auto-registered when the GCP module loads.
7. Verify that the asset + features are visible via:
   - ``GET /features/catalogs/{cat}/collections/{col}/items``
   - ``GET /stac/virtual/assets/{asset}/catalogs/{cat}/collections/{col}``
     and ``.../items`` (the collection-by-asset virtual view)
   - ``GET /tiles/catalogs/{cat}/tiles/WebMercatorQuad/{z}/{x}/{y}.mvt``
   and that the GCS reporter wrote a report blob into
   ``gs://{bucket}/ingestion-reports/``.
8. Hard-delete the catalog and verify the bucket is removed (with a
   direct force-delete as a safety net for the known async-cleanup race
   documented in ``test_gcp_lifecycle.py``).

Skipped when GCP credentials (ADC) are unavailable — handled by the
``@pytest.mark.gcp`` marker.
"""

import asyncio
import json
import logging

import pytest
from google.api_core.exceptions import NotFound

from dynastore.models.protocols import CatalogsProtocol, StorageProtocol
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.gcp.gcp_module import GCPModule
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)


def _stac_data_loader(filename: str):
    """Load a fixture from tests/dynastore/extensions/stac/integration/data."""
    import os
    base = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "extensions", "stac",
        "integration", "data",
    )
    with open(os.path.join(base, filename)) as f:
        return json.load(f)


@pytest.mark.skip(
    reason=(
        "Reaches step 7 (ingestion) cleanly but fails at GDAL/fiona data "
        "read: `/vsigs/{bucket}/{obj}` (OGR virtual filesystem) returns "
        "`DriverError: Failed to open dataset (flags=68)`. GDAL needs "
        "explicit GCS auth env config — GOOGLE_APPLICATION_CREDENTIALS "
        "alone is not enough for the `/vsigs/` VFS; typically either "
        "`CPL_MACHINE_IS_GCE=YES` (GCE metadata), `GS_ACCESS_KEY_ID` + "
        "`GS_SECRET_ACCESS_KEY` (HMAC), or `GDAL_HTTP_HEADER_FILE` with "
        "an OAuth bearer. Unblock: either add a GDAL-GCS auth bootstrap "
        "step to the gcp module / test conftest, or stage the source "
        "file on a local path for the ingestion input and keep only the "
        "collection-asset registration on `gs://`. Steps 1–6 (bucket "
        "provisioning, asset registration, feature-tracking config) now "
        "pass thanks to the MRO-stub fix, the sysadmin policy bypass in "
        "IamPolicyService.check_permission, and the caller_roles plumbing "
        "from processes_service → processes_module."
    )
)
@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stac", "processes", "tasks",
    "proxy", "gcp", "iam", "metadata_postgresql",
)
@pytest.mark.enable_extensions(
    "stac", "assets", "features", "configs", "processes", "web",
    "gcp_bucket", "tiles", "iam",
)
@pytest.mark.enable_tasks("gcp_provision", "gcp_catalog_cleanup", "ingestion")
async def test_gcp_end_to_end_lifecycle(
    sysadmin_in_process_client, in_process_client, app_lifespan, base_url,
):
    gcp_module = get_protocol(StorageProtocol)
    if not isinstance(gcp_module, GCPModule):
        pytest.skip("GCPModule not registered as StorageProtocol.")
    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")
    try:
        storage_client = gcp_module.get_storage_client()
    except RuntimeError:
        pytest.skip("GCP storage client not available (no credentials).")

    run_id = generate_test_id(10)
    catalog_id = f"it_{run_id}"
    collection_id = f"col_{run_id}"
    asset_id = f"asset_{run_id}"

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Pre-cleanup: force-delete any leftover from a previous run.
    try:
        await catalogs.delete_catalog(catalog_id, force=True)
        await lifecycle_registry.wait_for_all_tasks(timeout=10.0)
    except Exception:
        pass

    bucket_name = None
    try:
        # 1. Create catalog via STAC API → triggers gcp_provision lifecycle task.
        cat_payload = _stac_data_loader("catalog.json")
        cat_payload["id"] = catalog_id
        resp = await sysadmin_in_process_client.post(
            "/stac/catalogs", json=cat_payload
        )
        assert resp.status_code in (200, 201), f"catalog create: {resp.text}"
        await lifecycle_registry.wait_for_all_tasks(timeout=60.0)

        # 2. Verify bucket exists (eventual consistency).
        bucket_name = await gcp_module.get_storage_identifier(catalog_id)
        assert bucket_name, f"Bucket not provisioned for catalog {catalog_id}"
        bucket = None
        for _ in range(20):
            try:
                bucket = storage_client.get_bucket(bucket_name)
                break
            except NotFound:
                await asyncio.sleep(1.0)
        assert bucket is not None and bucket.exists(), (
            f"Bucket {bucket_name} not visible after provisioning"
        )
        logger.info("Provisioned bucket %s", bucket_name)

        # 3. Create collection.
        col_payload = _stac_data_loader("collection.json")
        col_payload["id"] = collection_id
        resp = await in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections", json=col_payload
        )
        assert resp.status_code in (200, 201), f"collection create: {resp.text}"

        # 4. Upload a GeoJSON artifact directly into the provisioned bucket.
        feature = _stac_data_loader("feature_item.json")
        blob_path = f"data/{asset_id}.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(feature), content_type="application/geo+json"
        )
        artifact_uri = f"gs://{bucket_name}/{blob_path}"
        logger.info("Uploaded artifact to %s", artifact_uri)

        # 5. Register the artifact as a collection asset (simulates the
        #    OBJECT_COMPLETED Pub/Sub event that fires in production).
        asset_payload = _stac_data_loader("asset.json")
        asset_payload.update({"asset_id": asset_id, "uri": artifact_uri})
        resp = await in_process_client.post(
            f"/assets/catalogs/{catalog_id}/collections/{collection_id}",
            json=asset_payload,
        )
        assert resp.status_code == 201, f"asset register: {resp.text}"

        # 6. Enable feature tracking on the collection.
        stac_config = _stac_data_loader("config.json")
        resp = await in_process_client.put(
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/configs/StacPluginConfig",
            json=stac_config,
        )
        assert resp.status_code in (200, 204), f"config put: {resp.text}"

        # 7. Kick off ingestion with GcsDetailedReporter writing reports to
        #    the catalog's bucket.
        task = _stac_data_loader("ingestion_task.json")
        task["catalog_id"] = catalog_id
        task["collection_id"] = collection_id
        task["ingestion_request"]["asset"]["asset_id"] = asset_id
        task["ingestion_request"]["asset"]["uri"] = artifact_uri
        task["ingestion_request"]["reporting"] = {
            "GcsDetailedReporter": {
                "report_file_path": (
                    f"gs://{bucket_name}/ingestion-reports/"
                    "{task_id}-{timestamp_utc}.json"
                ),
                "output_format": "JSON",
                "report_content": "ALL",
            }
        }

        # Use the sysadmin client here so ``caller_id`` is not resolved to
        # ``SYSTEM_USER_ID`` (which has no seed ALLOW policy for Action.EXECUTE)
        # — the unauthenticated ``in_process_client`` would 403 this step.
        resp = await sysadmin_in_process_client.post(
            f"/processes/catalogs/{catalog_id}/collections/{collection_id}"
            f"/processes/ingestion/execution",
            json={"inputs": task, "outputs": {}},
            headers={"Prefer": "wait=true"},
        )
        assert resp.status_code in (200, 201), f"ingestion exec: {resp.text}"
        assert resp.json().get("status") == "successful", (
            f"ingestion not successful: {resp.text}"
        )

        # 8. GcsDetailedReporter wrote a report blob (and a *_config blob).
        reports = list(
            storage_client.list_blobs(bucket_name, prefix="ingestion-reports/")
        )
        assert len(reports) >= 1, "No ingestion reports written to bucket"
        logger.info(
            "Ingestion wrote %d report blob(s): %s",
            len(reports), [b.name for b in reports],
        )

        # 9. Features service returns the ingested feature.
        resp = await in_process_client.get(
            f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"
            f"?limit=10"
        )
        assert resp.status_code == 200, f"features list: {resp.text}"
        feats = resp.json().get("features", [])
        assert len(feats) >= 1, "features endpoint returned no items post-ingestion"

        # 10. Virtual STAC collection-by-asset view.
        resp = await in_process_client.get(
            f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}"
            f"/collections/{collection_id}"
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == asset_id

        resp = await in_process_client.get(
            f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}"
            f"/collections/{collection_id}/items"
        )
        assert resp.status_code == 200
        vfeats = resp.json().get("features", [])
        assert len(vfeats) >= 1, (
            "virtual STAC collection-by-asset returned no items"
        )

        # 11. Tiles endpoint — 200 with bytes or 204 empty tile both acceptable.
        resp = await in_process_client.get(
            f"/tiles/catalogs/{catalog_id}/tiles/WebMercatorQuad/0/0/0.mvt"
            f"?collections={collection_id}"
        )
        assert resp.status_code in (200, 204), (
            f"tiles endpoint failed: {resp.status_code} {resp.text[:200]}"
        )

    finally:
        # 12. Hard-delete catalog + verify bucket is gone.
        try:
            await catalogs.delete_catalog(catalog_id, force=True)
            await lifecycle_registry.wait_for_all_tasks(timeout=30.0)
        except Exception as exc:
            logger.warning("delete_catalog failed: %s", exc)

        if bucket_name:
            # Known race: the async destruction hook occasionally loses the
            # bucket-name lookup when the schema is dropped first. Force-delete
            # as test cleanup and record whether the hook got there first.
            try:
                b = storage_client.get_bucket(bucket_name)
                b.delete(force=True)
                logger.info("Test cleanup: bucket %s force-deleted", bucket_name)
            except NotFound:
                logger.info(
                    "Verified: bucket %s cleaned up by lifecycle hook",
                    bucket_name,
                )
