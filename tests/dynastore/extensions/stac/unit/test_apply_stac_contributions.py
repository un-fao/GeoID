import pystac
from dynastore.tools.discovery import register_plugin, unregister_plugin, get_protocols
from dynastore.extensions.stac.stac_contributor import StacContribution
from dynastore.extensions.stac.asset_factory import apply_stac_contributions
from dynastore.models.protocols.asset_contrib import ResourceRef


class _FakeContributor:
    priority = 50

    def contribute_stac(self, ref):
        yield StacContribution(
            stac_extensions=("https://example.com/ext.json",),
            extra_fields={"x:foo": "bar"},
        )


def test_apply_merges_uris_and_fields_idempotently():
    c = _FakeContributor()
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
        ref = ResourceRef(catalog_id="c", collection_id="col")
        apply_stac_contributions(col, ref)
        apply_stac_contributions(col, ref)  # idempotent
        assert col.stac_extensions.count("https://example.com/ext.json") == 1
        assert col.extra_fields["x:foo"] == "bar"
    finally:
        unregister_plugin(c)
        get_protocols.cache_clear()
