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
BigQueryProtocol — abstraction for executing queries against Google BigQuery.

Implementations are registered via ``register_plugin(service)`` and discovered
with ``get_protocol(BigQueryProtocol)``.  The DWH extension uses this protocol
to decouple BigQuery access from the ``google.cloud.bigquery`` library.

Typical implementation: ``BigQueryService`` in ``modules/gcp/bigquery_service.py``.
"""

from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class BigQueryProtocol(Protocol):
    """Execute SQL queries against Google BigQuery.

    Implementations handle credential resolution, client lifecycle, and
    query execution.  Results are returned as a list of row dicts.
    """

    async def execute_query(
        self, query: str, project_id: str,
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as row dicts.

        Args:
            query:      The BigQuery SQL query to execute.
            project_id: The GCP project ID to run the query against.

        Returns:
            List of dicts, one per row, with column names as keys.

        Raises:
            RuntimeError: If credentials or client are unavailable.
        """
        ...
