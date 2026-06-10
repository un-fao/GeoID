"""Collections emit the STAC language extension via the StacContributor path.

`create_collection` routes its language enrichment through
`apply_stac_contributions` + `LanguageStacContributor` instead of hardcoding
the URI and reading `meta_dict`. This locks the building block: the real
contributor, applied to a bare Collection, declares the URI and the
`language`/`languages` fields.
"""
import pystac
from dynastore.tools.discovery import register_plugin, unregister_plugin, get_protocols
from dynastore.extensions.stac.asset_factory import apply_stac_contributions
from dynastore.extensions.stac.stac_contributor import LanguageStacContributor
from dynastore.models.localization import STAC_LANGUAGE_EXTENSION_URI
from dynastore.models.protocols.asset_contrib import ResourceRef


def test_collection_gets_language_fields_from_contributor():
    c = LanguageStacContributor()
    register_plugin(c)
    get_protocols.cache_clear()
    try:
        col = pystac.Collection(
            id="col", description="d",
            extent=pystac.Extent(
                pystac.SpatialExtent([[0, 0, 0, 0]]),
                pystac.TemporalExtent([[None, None]]),
            ),
        )
        ref = ResourceRef(
            catalog_id="cat", collection_id="col", lang="en",
            extras={"available_languages": {"en", "fr"}},
        )
        apply_stac_contributions(col, ref)
        assert STAC_LANGUAGE_EXTENSION_URI in col.stac_extensions
        assert "language" in col.extra_fields
        assert "languages" in col.extra_fields
    finally:
        unregister_plugin(c)
        get_protocols.cache_clear()


def test_collection_no_languages_gets_no_uri():
    """Conditional standardization: no available languages → no URI, no fields."""
    c = LanguageStacContributor()
    register_plugin(c)
    get_protocols.cache_clear()
    try:
        col = pystac.Collection(
            id="col", description="d",
            extent=pystac.Extent(
                pystac.SpatialExtent([[0, 0, 0, 0]]),
                pystac.TemporalExtent([[None, None]]),
            ),
        )
        ref = ResourceRef(
            catalog_id="cat", collection_id="col", lang="en",
            extras={"available_languages": set()},
        )
        apply_stac_contributions(col, ref)
        assert STAC_LANGUAGE_EXTENSION_URI not in col.stac_extensions
        assert "language" not in col.extra_fields
    finally:
        unregister_plugin(c)
        get_protocols.cache_clear()
