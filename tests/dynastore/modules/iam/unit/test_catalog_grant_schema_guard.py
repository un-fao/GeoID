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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Storage-layer guard: catalog-scoped grants must never land in ``iam`` (#1698).

Defense-in-depth for the #1698 silent-misroute class: regardless of how a
caller resolved the schema, ``grant_catalog_role`` / ``revoke_catalog_role``
are *inherently* catalog-scoped. Writing them into the platform ``iam``
schema is always wrong, so the storage layer refuses it loudly rather than
persisting the grant somewhere the catalog never reads it back.

The guard fires before any DB access, so these run as pure unit tests with
no engine.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage


def _storage() -> PostgresIamStorage:
    # No live engine in unit context; the guard short-circuits before the
    # method ever reaches for ``self.engine``.
    return PostgresIamStorage()


async def test_grant_catalog_role_refuses_platform_iam_schema():
    storage = _storage()
    with pytest.raises(ValueError, match="iam"):
        await storage.grant_catalog_role(
            principal_id=uuid4(),
            role_name="admin",
            catalog_schema="iam",
        )


async def test_revoke_catalog_role_refuses_platform_iam_schema():
    storage = _storage()
    with pytest.raises(ValueError, match="iam"):
        await storage.revoke_catalog_role(
            principal_id=uuid4(),
            role_name="admin",
            catalog_schema="iam",
        )
