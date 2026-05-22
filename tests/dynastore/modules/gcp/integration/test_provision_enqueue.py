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

"""Catalog creation must enqueue ``gcp_provision_catalog`` when provisioning is
enabled — without needing live GCP credentials (#1174).

The enqueue happens in ``GCPModule._on_post_create_catalog``, inside the catalog
creation transaction, *before* any GCS API call. So the decision is observable
against a real database with the GCP module loaded but no credentials present —
distinct from the credential-gated ``@pytest.mark.gcp`` end-to-end suite, which
also drives the actual bucket creation.

This pins the bug from #1174: with ``provision_enabled=True`` (the code default),
catalog creation produced no provisioning task, so the dispatcher had nothing to
claim and no bucket was ever created.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.tasks.tasks_module import list_tasks_for_catalog
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id


async def _provision_tasks(engine, catalog_id: str):
    async with managed_transaction(engine) as conn:
        tasks = await list_tasks_for_catalog(conn, catalog_id)
    return [t for t in tasks if t.task_type == "gcp_provision_catalog"]


@pytest.mark.asyncio
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "catalog_postgresql", "tasks", "gcp",
)
async def test_create_catalog_enqueues_gcp_provision_task(app_lifespan):
    """provision_enabled defaults to True → exactly one gcp_provision_catalog
    task is enqueued inside catalog creation."""
    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")

    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = f"it_pq_{generate_test_id(8)}"
    await catalogs.delete_catalog(catalog_id, force=True)

    try:
        await catalogs.create_catalog({"id": catalog_id, "title": {"en": "p"}}, lang="*")

        prov = await _provision_tasks(app_lifespan.engine, catalog_id)
        assert prov, (
            "create_catalog enqueued no gcp_provision_catalog task while "
            "provision_enabled=True — the catalog can never be provisioned (#1174)."
        )
        assert len(prov) == 1, f"expected one provision task, got {len(prov)}"
        assert prov[0].inputs == {"catalog_id": catalog_id}
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
        await lifecycle_registry.wait_for_all_tasks()
