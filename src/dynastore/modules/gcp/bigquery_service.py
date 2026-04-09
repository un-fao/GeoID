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
from typing import Any, Dict, List

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
