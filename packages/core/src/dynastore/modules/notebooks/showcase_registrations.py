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
