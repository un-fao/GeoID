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

"""NotebookContributorProtocol contributions for the Web extension.

General-purpose UI-builder walkthroughs and cross-cutting demos that
don't belong to a single domain extension live here. Picked up at
runtime via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. ``Web.get_notebooks`` calls
:func:`build_contributions` here.

No import-time registration and no hard dependency on the notebooks
module — ``NotebookContribution`` and the folder-discovery helper are
imported lazily so the extension stays loadable in SCOPEs that don't
include the notebooks module.

Auto-discovers every ``*.ipynb`` in the colocated ``notebooks/`` dir.
Filename prefixes group the showcases:
  ``01_ui_…``   UI walkthrough (4)
  ``cfg…``       Config API patterns (4)
  ``proc…``      OGC Processes execution (3)
  ``qry…``       Queryables / collection search (1)
  ``uc…``        Cycle-F use cases — config API end-to-end (4)
"""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"


def build_contributions():
    try:
        from dynastore.modules.notebooks.folder_discovery import discover_notebooks
    except Exception:
        return []
    return discover_notebooks(_HERE, prefix="web")
