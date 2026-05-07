"""Platform notebook registrations for the geoid extension.

Imported during GeoidExtension lifespan so the showcase notebooks land in
the platform notebook table before NotebooksModule seeds the JupyterLite
content tree.
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.geoid"


register_platform_notebook(
    notebook_id="lookup_only_demo",
    registered_by=_REG,
    notebook_path=_HERE / "lookup_only_demo.ipynb",
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
)
