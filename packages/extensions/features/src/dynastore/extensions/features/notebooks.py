"""NotebookContributorProtocol contributions for the OGC Features extension.

Picked up at runtime via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. The extension class's ``get_notebooks``
calls :func:`build_contributions` here.

This module deliberately does NOT register at import time and does NOT
hard-depend on ``register_platform_notebook``. ``NotebookContribution``
is imported lazily so the extension stays loadable in SCOPEs that don't
include the notebooks module.
"""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.features"


def build_contributions():
    from dynastore.modules.notebooks.contribution import NotebookContribution

    return [
        NotebookContribution(
            notebook_id="features_ingestion_and_diagnostics",
            title={"en": "Features — Ingestion & Diagnostics"},
            description={
                "en": (
                    "Walks the waterfall-driven ingestion path: zero-config "
                    "collection accepts items via code defaults, policy-driven "
                    "rejections return an IngestionReport (HTTP 207 partial / "
                    "200 fully accepted) with diagnostic links."
                )
            },
            tags=["features", "ingestion", "ogc", "demo"],
            notebook_path=_HERE / "ingestion_and_diagnostics.ipynb",
            registered_by=_REG,
        ),
    ]
