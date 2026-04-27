"""Shared fixtures for admin extension integration tests.

Mirrors the principal/catalog fixtures defined locally in
`tests/dynastore/extensions/iam/integration/test_authorization_api.py`.
The duplication is intentional for now — both could be promoted to
`tests/dynastore/extensions/conftest.py` later if a third caller
appears.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest_asyncio
from httpx import AsyncClient

from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_uuidv7
from dynastore.modules.iam.iam_service import IamService
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CreatedPrincipal:
    """Convenience handle for tests that need to address a fixture
    principal by its UUID (admin endpoints) and by its identity link
    (provider/subject_id).
    """

    principal_id: str
    email: str
    provider: str
    subject_id: str


@pytest_asyncio.fixture
async def created_principal(sysadmin_in_process_client: AsyncClient) -> CreatedPrincipal:
    """Create a principal with a `local` identity link and return all
    handles a test might need.
    """
    iam_service = get_protocol(IamService)
    assert iam_service is not None
    storage = iam_service.storage
    assert storage is not None

    email = f"test_{generate_test_id(12)}@example.com"
    subject_id = f"sub_{generate_test_id()}"
    principal_id = generate_uuidv7()

    from dynastore.modules.iam.models import Principal
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.models.protocols import DatabaseProtocol

    principal = Principal(
        id=principal_id,
        identifier=email,
        display_name=email,
        roles=[],
        is_active=True,
    )
    db = get_protocol(DatabaseProtocol)
    async with managed_transaction(db.engine) as conn:
        await storage.create_principal(principal, conn=conn)
        await storage.create_identity_link(
            provider="local",
            subject_id=subject_id,
            principal_id=principal_id,
            conn=conn,
        )

    logger.info("Created fixture principal %s (%s)", principal_id, email)
    return CreatedPrincipal(
        principal_id=str(principal_id),
        email=email,
        provider="local",
        subject_id=subject_id,
    )


@pytest_asyncio.fixture
async def setup_catalogs(sysadmin_in_process_client: AsyncClient):
    """Create two catalogs for the test, delete them after."""
    catalog_ids = [f"cat_{generate_test_id()}", f"cat_{generate_test_id()}"]
    for catalog_id in catalog_ids:
        response = await sysadmin_in_process_client.post(
            "/features/catalogs",
            json={
                "id": catalog_id,
                "title": catalog_id,
                "description": "Test catalog for admin API",
            },
        )
        assert response.status_code in (201, 409), (
            f"Failed to create catalog {catalog_id}: {response.status_code} {response.text}"
        )

    yield catalog_ids

    for catalog_id in catalog_ids:
        try:
            await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}")
        except Exception as e:  # pragma: no cover — best-effort cleanup
            logger.warning("Cleanup of catalog %s failed: %s", catalog_id, e)
