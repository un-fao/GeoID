"""
Unit tests for the Web extension's documentation scanner.

These are pure unit tests — no database, no lifespan, no HTTP client.
The Web instance is constructed with a MagicMock app and its internal
state is injected directly so _scan_for_documentation() can be exercised
against a controlled temporary filesystem.
"""

import pytest
from unittest.mock import MagicMock
from dynastore.extensions.web.web import Web


@pytest.mark.asyncio
async def test_docs_manifest_excludes_node_modules(tmp_path):
    """
    Verifies that the documentation scanner excludes node_modules directories.
    """
    src = tmp_path / "src"
    modules = src / "dynastore" / "modules"
    modules.mkdir(parents=True)

    # Valid module documentation
    (modules / "valid_module").mkdir()
    (modules / "valid_module" / "readme.md").write_text("# Valid Module")

    # node_modules clutter — should be excluded
    (modules / "valid_module" / "node_modules" / "pkg").mkdir(parents=True)
    (modules / "valid_module" / "node_modules" / "pkg" / "readme.md").write_text("# Clutter")

    # Deeply nested node_modules — should also be excluded
    (modules / "valid_module" / "static" / "node_modules" / "deep").mkdir(parents=True)
    (modules / "valid_module" / "static" / "node_modules" / "deep" / "readme.md").write_text("# Deep Clutter")

    app = MagicMock()
    web_ext = Web(app)
    web_ext.project_root = str(tmp_path)
    web_ext.app_dirs = [str(src / "dynastore")]

    registry = web_ext._scan_for_documentation()

    found_readme = False
    for doc in registry.values():
        if "Valid Module" in doc["title"]:
            found_readme = True
        if "Clutter" in doc["title"]:
            pytest.fail(f"Found node_modules documentation: {doc['path']}")

    assert found_readme, "Did not find valid module documentation"
