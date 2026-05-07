"""NotebookContributorProtocol contributions for the geoid extension.

Picked up at runtime via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. ``Geoid.get_notebooks`` calls
:func:`build_contributions` here.

No import-time registration and no hard dependency on the notebooks
module — ``NotebookContribution`` is imported lazily so the extension
stays loadable in SCOPEs that don't include the notebooks module.
"""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"


def build_contributions():
    from dynastore.modules.notebooks.contribution import NotebookContribution

    return [
        NotebookContribution(
            notebook_id="lookup_only_demo",
            title={"en": "Lookup-only public catalog — anonymous demo"},
            description={
                "en": (
                    "Anonymous demo of the lookup-only profile: geoid lookup + "
                    "exact-item GET on STAC/Features both succeed without a token, "
                    "while collection list / items list / STAC search return 401/403. "
                    "Drives /search/catalogs/{cat}/geoid (PG-backed exact geometry) "
                    "and contrasts the open catalog surface against the auth-gated "
                    "surface to show DENY policies firing as expected."
                )
            },
            tags=["geoid", "lookup", "anonymous", "iam", "demo"],
            notebook_path=_HERE / "lookup_only_demo.ipynb",
        ),
    ]
