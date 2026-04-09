"""In-memory registry for platform-level notebook examples.

Modules call register_platform_notebook() at import time.
NotebooksModule.lifespan() reads the registry and seeds into
notebooks.platform_notebooks with ON CONFLICT DO NOTHING.
"""
import json
import logging
from pathlib import Path
from typing import Optional

from .models import PlatformNotebookCreate, OwnerType

logger = logging.getLogger(__name__)

_platform_registry: list[PlatformNotebookCreate] = []
_registered_ids: set[str] = set()


def register_platform_notebook(
    notebook_id: str,
    registered_by: str,
    notebook_path: Optional[Path] = None,
    notebook_content: Optional[dict] = None,
) -> None:
    """Register an example notebook for platform-level seeding.

    Call at module import time. Seeding happens during NotebooksModule.lifespan().
    Provide either notebook_path (to a .ipynb file) or notebook_content (raw dict).
    """
    if notebook_path is None and notebook_content is None:
        raise ValueError("Provide either notebook_path or notebook_content")

    if notebook_id in _registered_ids:
        logger.debug("Platform notebook '%s' already registered, skipping.", notebook_id)
        return

    if notebook_path is not None:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook_content = json.load(f)

    title = notebook_content.get("metadata", {}).get("title", notebook_id)

    entry = PlatformNotebookCreate(
        notebook_id=notebook_id,
        title=title,
        description=notebook_content.get("metadata", {}).get("description"),
        content=notebook_content,
        metadata=notebook_content.get("metadata", {}),
        registered_by=registered_by,
        owner_type=OwnerType.MODULE,
    )
    _platform_registry.append(entry)
    _registered_ids.add(notebook_id)
    logger.info("Registered platform notebook '%s' from '%s'.", notebook_id, registered_by)


def get_registered_notebooks() -> list[PlatformNotebookCreate]:
    """Return all registered platform notebook entries."""
    return list(_platform_registry)
