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

"""Helper for ``NotebookContributorProtocol`` implementations that ship a
folder of ``.ipynb`` files alongside their code.

Typical use, in an extension's ``notebooks.py``::

    from pathlib import Path
    from dynastore.modules.notebooks.folder_discovery import discover_notebooks

    _HERE = Path(__file__).parent / "notebooks"

    def build_contributions():
        return discover_notebooks(_HERE, prefix="myext")

The extension class then exposes the protocol method::

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

``NotebookContribution`` is imported lazily so the helper itself stays
loadable in SCOPEs that omit the notebooks module — callers get an
empty list.
"""
from pathlib import Path
from typing import List

from dynastore.models.localization import LocalizedText


def _humanize(stem: str) -> str:
    """Turn ``01_foo_bar`` into ``Foo Bar``; numeric prefix dropped."""
    parts = stem.split("_")
    if parts and parts[0].isdigit():
        parts = parts[1:]
    return " ".join(p.capitalize() for p in parts) or stem


def _extract_title_from_notebook(path: Path) -> str | None:
    """Pull the first ``# heading`` line from the notebook's first markdown
    cell. Returns ``None`` if the notebook can't be read or has no heading.
    """
    import json
    try:
        with open(path, "r", encoding="utf-8") as f:
            nb = json.load(f)
    except Exception:
        return None
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "markdown":
            continue
        src = cell.get("source")
        text = src if isinstance(src, str) else "".join(src or [])
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("#"):
                return line.lstrip("#").strip() or None
        return None
    return None


def discover_notebooks(folder: Path, *, prefix: str = "", registered_by: str | None = None) -> List:
    """Return a list of :class:`NotebookContribution` for every ``*.ipynb``
    in ``folder`` (sorted, non-recursive).

    Title resolution: prefer the first ``# heading`` found in the
    notebook's first markdown cell; fall back to a humanized form of the
    filename stem.

    Parameters
    ----------
    folder
        Directory containing ``.ipynb`` files.
    prefix
        String prepended to each notebook stem to form a unique
        ``notebook_id`` across contributors. Use a short slug like
        ``"web"`` or ``"assets"``.

    Returns ``[]`` if the folder doesn't exist or the notebooks module
    isn't importable in this SCOPE.
    """
    try:
        from dynastore.modules.notebooks.contribution import NotebookContribution
    except Exception:
        return []

    if not folder.is_dir():
        return []

    out = []
    for ipynb in sorted(folder.glob("*.ipynb")):
        stem = ipynb.stem
        nb_id = f"{prefix}_{stem}" if prefix else stem
        title = _extract_title_from_notebook(ipynb) or _humanize(stem)
        out.append(
            NotebookContribution(
                notebook_id=nb_id,
                title=LocalizedText(en=title),
                notebook_path=ipynb,
                registered_by=registered_by or prefix or "unknown",
            )
        )
    return out
