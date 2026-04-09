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

"""
Registration of storage-combination demo notebooks.

Each notebook demonstrates a distinct multi-driver routing pattern.
Import this module to register all 5 notebooks into the platform registry.

Patterns:

| Notebook ID                | WRITE              | READ   | SEARCH              |
|----------------------------|--------------------|--------|---------------------|
| obfuscated_es_plus_pg      | [pg, es_obfuscated]| [pg]   | [es_obfuscated]     |
| es_obfuscated_only         | [es_obfuscated]    | [es_o] | [es_obfuscated]     |
| duckdb_plus_es             | [duckdb]           | [duck] | [elasticsearch]     |
| iceberg_plus_es            | [iceberg, es]      | [ice]  | [elasticsearch]     |
| iceberg_plus_pg            | [iceberg, pg]      | [pg]   | [pg]                |
"""

from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent

_NOTEBOOKS = [
    "obfuscated_es_plus_pg",
    "es_obfuscated_only",
    "duckdb_plus_es",
    "iceberg_plus_es",
    "iceberg_plus_pg",
]

for _nb_id in _NOTEBOOKS:
    register_platform_notebook(
        notebook_id=f"storage_combo_{_nb_id}",
        registered_by="dynastore.modules.notebooks.storage_combo_notebooks",
        notebook_path=_HERE / f"{_nb_id}.ipynb",
    )
