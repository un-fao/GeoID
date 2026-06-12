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

"""Platform notebook registrations for the ingestion task.

Registers ingestion workflow notebooks using presets and OGC protocols.

Import this module during NotebooksModule lifespan (before seeding).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.tasks.ingestion"

register_platform_notebook(
    notebook_id="ingestion_virtual_asset",
    registered_by=_REG,
    notebook_path=_HERE / "nb06_ingestion_virtual_asset.ipynb",
    title={"en": "Ingestion via Virtual Asset"},
    description={
        "en": (
            "Register a virtual (external href) asset and trigger the ingestion task. "
            "Verifies items appear via STAC /items after the task completes."
        )
    },
    tags=["ingestion", "virtual-asset", "preset", "stac", "processes"],
)
