import json
import tempfile
from pathlib import Path

from dynastore.modules.notebooks.example_registry import (
    register_platform_notebook,
    get_registered_notebooks,
    _platform_registry,
    _registered_ids,
)
from dynastore.modules.notebooks.models import OwnerType


def setup_function():
    """Clear registry before each test."""
    _platform_registry.clear()
    _registered_ids.clear()


def test_register_with_content():
    register_platform_notebook(
        notebook_id="test-nb",
        registered_by="test_module",
        notebook_content={"cells": [], "metadata": {"title": "Test"}},
    )
    entries = get_registered_notebooks()
    assert len(entries) == 1
    assert entries[0].notebook_id == "test-nb"
    assert entries[0].registered_by == "test_module"
    assert entries[0].owner_type == OwnerType.MODULE
    assert entries[0].content == {"cells": [], "metadata": {"title": "Test"}}
    assert entries[0].title.en == "Test"


def test_register_with_path():
    nb_content = {"cells": [{"cell_type": "code", "source": "print(1)"}], "metadata": {"title": "From File"}}
    with tempfile.NamedTemporaryFile(suffix=".ipynb", mode="w", delete=False) as f:
        json.dump(nb_content, f)
        tmp_path = Path(f.name)

    register_platform_notebook(
        notebook_id="file-nb",
        registered_by="file_module",
        notebook_path=tmp_path,
    )
    entries = get_registered_notebooks()
    assert len(entries) == 1
    assert entries[0].notebook_id == "file-nb"
    assert entries[0].content == nb_content
    assert entries[0].title == "From File"

    tmp_path.unlink()


def test_register_requires_content_or_path():
    try:
        register_platform_notebook(
            notebook_id="bad",
            registered_by="mod",
        )
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_duplicate_id_skipped():
    register_platform_notebook(
        notebook_id="dup",
        registered_by="mod1",
        notebook_content={"cells": [], "metadata": {"title": "First"}},
    )
    register_platform_notebook(
        notebook_id="dup",
        registered_by="mod2",
        notebook_content={"cells": [], "metadata": {"title": "Second"}},
    )
    entries = get_registered_notebooks()
    assert len(entries) == 1
    assert entries[0].registered_by == "mod1"


def test_title_fallback_to_notebook_id():
    register_platform_notebook(
        notebook_id="no-title",
        registered_by="mod",
        notebook_content={"cells": [], "metadata": {}},
    )
    entries = get_registered_notebooks()
    assert entries[0].title == "no-title"
