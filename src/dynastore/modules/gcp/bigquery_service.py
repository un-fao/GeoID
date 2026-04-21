#    Copyright 2025 FAO
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

"""
BigQueryService — BigQueryProtocol implementation using google-cloud-bigquery.

Registered via ``register_plugin(BigQueryService())`` during GCPModule lifespan.
Credentials are resolved through ``CloudIdentityProtocol``.
"""

import logging
from typing import Any, Dict, List, Optional

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class BigQueryService:
    """BigQueryProtocol implementation backed by google-cloud-bigquery.

    Each ``execute_query()`` call creates a short-lived BQ client using
    credentials from ``CloudIdentityProtocol``.  Results are returned as
    a list of row dicts suitable for dict-lookup joins or DataFrame
    construction.
    """

    async def execute_query(
        self, query: str, project_id: str,
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query against BigQuery and return rows as dicts.

        Args:
            query:      The BigQuery SQL to execute.
            project_id: The GCP project ID for billing/quota.

        Returns:
            List of row dicts (one per result row).

        Raises:
            RuntimeError: If credentials are unavailable.
        """
        from dynastore.models.protocols import CloudIdentityProtocol

        identity = get_protocol(CloudIdentityProtocol)
        if not identity:
            raise RuntimeError("CloudIdentityProtocol not available — cannot execute BigQuery query")

        if not project_id:
            raise ValueError("BigQuery project_id is required")

        credentials = identity.get_credentials_object()

        from google.cloud import bigquery

        client = bigquery.Client(project=project_id, credentials=credentials)
        try:
            logger.info("Executing BQ query (via BigQueryService): %s...", query[:200])
            df = client.query(query).to_dataframe()
            return df.to_dict("records")
        finally:
            client.close()

    async def insert_rows_json(
        self,
        table_fqn: str,
        rows: List[Dict[str, Any]],
        *,
        project_id: str,
        row_ids: Optional[List[Optional[str]]] = None,
    ) -> List[Dict[str, Any]]:
        """Stream ``rows`` into ``table_fqn`` via BQ's ``insertAll`` API.

        Empty input is a fast no-op (no client created).  Partial failures
        are returned rather than raised — callers under ``on_failure=warn``
        log them and continue.
        """
        if not rows:
            return []
        if row_ids is not None and len(row_ids) != len(rows):
            raise ValueError(
                f"insert_rows_json: row_ids length ({len(row_ids)}) "
                f"must match rows length ({len(rows)})."
            )
        if not project_id:
            raise ValueError("BigQuery project_id is required for insert_rows_json")

        from dynastore.models.protocols import CloudIdentityProtocol

        identity = get_protocol(CloudIdentityProtocol)
        if not identity:
            raise RuntimeError(
                "CloudIdentityProtocol not available — cannot stream BigQuery rows"
            )

        credentials = identity.get_credentials_object()

        from google.cloud import bigquery

        client = bigquery.Client(project=project_id, credentials=credentials)
        try:
            table_ref = bigquery.TableReference.from_string(
                table_fqn, default_project=project_id
            )
            errors = client.insert_rows_json(
                table_ref,
                rows,
                row_ids=row_ids,
            )
            # google-cloud-bigquery returns a Sequence[dict] (no specific
            # dict[str, Any] parameterisation in its stubs).  Materialise
            # as a typed list so the protocol contract holds.
            result: List[Dict[str, Any]] = list(errors)
            if result:
                logger.warning(
                    "BQ streaming insert partial failure: %d/%d rows failed to "
                    "insert into %s",
                    len(result), len(rows), table_fqn,
                )
            else:
                logger.debug(
                    "BQ streaming insert OK: %d rows into %s", len(rows), table_fqn
                )
            return result
        finally:
            client.close()
