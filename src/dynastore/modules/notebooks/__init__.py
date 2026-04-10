from .notebooks_module import NotebooksModule
from .models import NotebookCreate, Notebook, NotebookBase, PlatformNotebookCreate, PlatformNotebook, OwnerType
from .notebooks_db import init_notebooks_storage
from .example_registry import register_platform_notebook, get_registered_notebooks
# Module-owned notebook registrations — each import triggers register_platform_notebook()
from dynastore.modules.catalog import notebooks as _catalog_notebooks  # noqa: F401
from dynastore.modules.storage import notebooks as _storage_notebooks  # noqa: F401
from dynastore.modules.elasticsearch import notebooks as _es_notebooks  # noqa: F401

__all__ = [
    "NotebooksModule",
    "init_notebooks_storage",
    "NotebookCreate",
    "Notebook",
    "NotebookBase",
    "PlatformNotebookCreate",
    "PlatformNotebook",
    "OwnerType",
    "register_platform_notebook",
    "get_registered_notebooks",
]
