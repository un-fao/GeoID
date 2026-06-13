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

"""Platform notebook registrations for the dimensions_materialize task.

Picked up by NotebooksModule.lifespan via the hardcoded module-path list
so the showcase notebook lands in the platform notebook table.
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.tasks.dimensions_materialize"


register_platform_notebook(
    notebook_id="dimensions_materialize_trigger_and_check",
    registered_by=_REG,
    notebook_path=_HERE / "trigger_and_check.ipynb",
    title={"en": "Dimensions Materialize — Trigger & Verify"},
    description={
        "en": (
            "Triggers the dimensions_materialize OGC Process and verifies "
            "the resulting _dimensions_ catalog over HTTP only. Idempotent "
            "via a cube:dimensions equality check — safe to run on each "
            "deploy from a notebook or post-deploy job."
        )
    },
    tags=["dimensions", "ogc", "process", "task", "demo"],
)
