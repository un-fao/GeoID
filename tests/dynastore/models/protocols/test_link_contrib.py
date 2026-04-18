from dynastore.models.protocols.link_contrib import (
    AnchoredLink,
    LinkContributor,
)
from dynastore.models.protocols.asset_contrib import ResourceRef


def test_anchored_link_is_frozen_dataclass():
    link = AnchoredLink(
        anchor="resource_root",
        rel="styles",
        href="http://example/styles",
        title="Styles list",
        media_type="application/json",
    )
    # Frozen — assignment should raise.
    import dataclasses
    try:
        link.rel = "other"  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("AnchoredLink should be frozen")


def test_link_contributor_structural_protocol():
    # A plain class with the right shape satisfies the protocol.
    class Fake:
        priority = 100

        def contribute_links(self, ref: ResourceRef):
            yield AnchoredLink(
                anchor="data_asset",
                rel="style",
                href="http://example/s",
                title="s",
                media_type="application/json",
            )

    assert isinstance(Fake(), LinkContributor)


def test_anchor_accepts_three_values():
    for anchor in ("resource_root", "data_asset", "collection_root"):
        AnchoredLink(
            anchor=anchor,
            rel="x",
            href="h",
            title="t",
            media_type="application/json",
        )
