"""NotebookContributorProtocol contributions for the Coverages extension."""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"


def build_contributions():
    try:
        from dynastore.modules.notebooks.folder_discovery import discover_notebooks
    except Exception:
        return []
    return discover_notebooks(_HERE, prefix="coverages")
