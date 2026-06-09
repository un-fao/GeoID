from dynastore.extensions.stac.stac_contributor import (
    StacContribution,
    StacContributor,
)
from dynastore.models.protocols.asset_contrib import ResourceRef


def test_stac_contribution_defaults_empty():
    c = StacContribution()
    assert c.stac_extensions == ()
    assert dict(c.extra_fields) == {}


def test_stac_contributor_is_runtime_checkable():
    class _Dummy:
        priority = 100

        def contribute_stac(self, ref):
            yield StacContribution(stac_extensions=("uri",), extra_fields={"k": 1})

    d = _Dummy()
    assert isinstance(d, StacContributor)
    out = list(d.contribute_stac(ResourceRef(catalog_id="c", collection_id="col")))
    assert out[0].stac_extensions == ("uri",)
