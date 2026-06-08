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

"""Platform notebook registrations for the storage module.

Registers multi-driver routing pattern notebooks:
  - storage_combo_duckdb_plus_es    — DuckDB write + ES search
  - storage_combo_iceberg_plus_es   — Iceberg lakehouse + ES search
  - storage_combo_iceberg_plus_pg   — Iceberg write + PG read/search

Import this module during StorageModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.storage"

register_platform_notebook(
    notebook_id="storage_combo_duckdb_plus_es",
    registered_by=_REG,
    notebook_path=_HERE / "duckdb_plus_es.ipynb",
    title={"en": "DuckDB Analytics + ES Search"},
    description={"en": "Route WRITE to DuckDB for analytical queries; SEARCH to Elasticsearch for full-text."},
    tags=["storage", "duckdb", "elasticsearch", "routing", "analytics"],
)

register_platform_notebook(
    notebook_id="storage_combo_iceberg_plus_es",
    registered_by=_REG,
    notebook_path=_HERE / "iceberg_plus_es.ipynb",
    title={"en": "Iceberg Lakehouse + ES Search"},
    description={"en": "Route WRITE to Apache Iceberg for lakehouse storage; SEARCH to Elasticsearch."},
    tags=["storage", "iceberg", "elasticsearch", "routing", "lakehouse"],
)

register_platform_notebook(
    notebook_id="storage_combo_iceberg_plus_pg",
    registered_by=_REG,
    notebook_path=_HERE / "iceberg_plus_pg.ipynb",
    title={"en": "Iceberg Write + PG Read/Search"},
    description={"en": "Route WRITE to Apache Iceberg; READ and SEARCH to PostgreSQL."},
    tags=["storage", "iceberg", "postgresql", "routing"],
)

# Storage driver showcases — migrated from notebook_showcase/storage_drivers/
register_platform_notebook(
    notebook_id="storage_postgresql_driver_configure_and_verify",
    registered_by=_REG,
    notebook_path=_HERE / "sd01_postgresql_driver_configure_and_verify.ipynb",
    title={"en": "PostgreSQL Driver — Configure and Verify"},
    tags=["storage", "postgresql", "driver", "configuration"],
)
register_platform_notebook(
    notebook_id="storage_duckdb_analytical_otf_queries",
    registered_by=_REG,
    notebook_path=_HERE / "sd02_duckdb_analytical_otf_queries.ipynb",
    title={"en": "DuckDB — Analytical OTF Queries"},
    tags=["storage", "duckdb", "analytics", "otf"],
)
register_platform_notebook(
    notebook_id="storage_iceberg_driver_with_pg_sql_catalog",
    registered_by=_REG,
    notebook_path=_HERE / "sd03_iceberg_driver_with_pg_sql_catalog.ipynb",
    title={"en": "Iceberg Driver with PG SQL Catalog"},
    tags=["storage", "iceberg", "postgresql", "catalog"],
)
register_platform_notebook(
    notebook_id="storage_engines_and_multi_instance",
    registered_by=_REG,
    notebook_path=_HERE / "sd04_engines_and_multi_instance.ipynb",
    title={"en": "Storage Engines and Multi-Instance"},
    tags=["storage", "engines", "multi-instance", "configuration"],
)

# Routing showcases — migrated from notebook_showcase/routing/
register_platform_notebook(
    notebook_id="storage_routing_corner_cases_failure_policies_and_hints",
    registered_by=_REG,
    notebook_path=_HERE / "rt02_corner_cases_failure_policies_and_hints.ipynb",
    title={"en": "Routing — Corner Cases, Failure Policies, Hints"},
    tags=["storage", "routing", "failure-policy", "hints"],
)
