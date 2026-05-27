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

"""Task: promote an ``_attrs.<key>`` JSONB path to a first-class PG column.

Triggered by::

    POST /admin/catalogs/{cat}/collections/{coll}/attrs/promote
    {"column": "dept", "pg_type": "TEXT"}

The task runs :func:`~dynastore.modules.iam.migrations.promote_envelope_attr_column.promote_envelope_attr_column`
which:

1. Checks idempotency via ``iam.applied_presets``.
2. Adds ``_attr_{column_name} {pg_type}`` to ``{schema}.items``.
3. Creates a btree (TEXT/NUMERIC/TIMESTAMPTZ) or GIN (TEXT[]) index.
4. Backfills existing rows in batches.
5. Records the completion sentinel.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)


class PromoteEnvelopeAttrInputs(BaseModel):
    """Inputs for the ``promote_envelope_attr_column`` task."""

    catalog_id: str
    collection_id: str
    column_name: str
    pg_type: str = "TEXT"


class PromoteEnvelopeAttrColumnTask(TaskProtocol):
    """Migrate a single ABAC attribute key to a first-class PG column.

    Inputs (via ``TaskCreate.inputs``):
        ``catalog_id``, ``collection_id``, ``column_name``, ``pg_type``

    This task is idempotent: the underlying migration checks the
    ``iam.applied_presets`` sentinel and is a no-op on re-run.
    """

    task_type = "promote_envelope_attr_column"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.driver_context import DriverContext
        from dynastore.modules.iam.migrations.promote_envelope_attr_column import (
            promote_envelope_attr_column,
        )
        from dynastore.tools.discovery import get_protocol

        inputs = PromoteEnvelopeAttrInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id
        column_name = inputs.column_name
        pg_type = inputs.pg_type

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")
        engine = db.engine  # type: ignore[union-attr]

        # Resolve physical schema for this catalog.
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not available.")
        schema = await catalogs.resolve_physical_schema(
            catalog_id,
            ctx=DriverContext(db_resource=engine),
        )
        if not schema:
            raise RuntimeError(
                f"Cannot resolve physical schema for catalog '{catalog_id}'."
            )

        await promote_envelope_attr_column(
            engine=engine,
            catalog_id=catalog_id,
            collection_id=collection_id,
            column_name=column_name,
            pg_type=pg_type,
            schema=schema,
        )

        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "column_name": column_name,
            "pg_type": pg_type,
            "schema": schema,
            "status": "done",
        }
