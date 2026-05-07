"""Import-time platform-notebook registration for the geoid extension.

The catalog service does not load the geoid extension's service class
(geoid runs as its own Cloud Run service), so the
``NotebookContributorProtocol`` aggregator in ``NotebooksModule.lifespan``
never sees ``Geoid.get_notebooks()``.  This module is added to that
module's legacy import-time list so the same notebooks land in the
catalog's ``platform_notebooks`` registry — and therefore in JupyterLite
— regardless of which service the lifespan runs in.

Mirrors the pattern used by ``dynastore.extensions.dimensions``.
``register_platform_notebook`` upserts with ``ON CONFLICT DO NOTHING``
so importing this from a SCOPE that already loaded the geoid extension
(e.g. the geoid service itself) is a no-op.
"""
from dynastore.modules.notebooks.example_registry import register_platform_notebook

from .notebooks import build_contributions

for _nb in build_contributions():
    register_platform_notebook(
        notebook_id=_nb.notebook_id,
        registered_by="geoid_extension",
        notebook_path=_nb.notebook_path,
        notebook_content=_nb.notebook_content,
        title=_nb.title,
        description=_nb.description,
        tags=list(_nb.tags or []),
    )
