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

"""Platform notebook registrations for the JupyterLite-management showcase.

These demonstrate the notebooks module's own surface (copy from platform,
sysadmin management). Loaded by ``NotebooksModule.lifespan`` via the
hardcoded module-path list — keeps the legacy import-time pattern.
"""
from pathlib import Path

from .example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.notebooks"


register_platform_notebook(
    notebook_id="jupyterlite_copy_platform_notebook_and_edit",
    registered_by=_REG,
    notebook_path=_HERE / "jl01_copy_platform_notebook_and_edit.ipynb",
    title={"en": "JupyterLite — Copy a Platform Notebook and Edit"},
    tags=["jupyterlite", "notebooks", "copy", "edit"],
)
register_platform_notebook(
    notebook_id="jupyterlite_manage_platform_notebooks_sysadmin",
    registered_by=_REG,
    notebook_path=_HERE / "jl02_manage_platform_notebooks_sysadmin.ipynb",
    title={"en": "JupyterLite — Manage Platform Notebooks (sysadmin)"},
    tags=["jupyterlite", "notebooks", "sysadmin"],
)
