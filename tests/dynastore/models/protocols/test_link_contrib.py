import dataclasses

import pytest

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import (
    AnchoredLink,
    LinkContributor,
)


def test_anchored_link_is_frozen_dataclass():
    link = AnchoredLink(
        anchor="resource_root",
        rel="styles",
        href="http://example/styles",
        title="Styles list",
        media_type="application/json",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        link.rel = "other"  # type: ignore[misc]


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


def test_anchor_documents_supported_values():
    # Literal is a typing construct, not a runtime guard — this test pins the
    # intended value set as documentation. Runtime acceptance of other strings
    # is caught by type-checkers (pyright/mypy), not by dataclass validation.
    for anchor in ("resource_root", "data_asset", "collection_root"):
        AnchoredLink(
            anchor=anchor,
            rel="x",
            href="h",
            title="t",
            media_type="application/json",
        )
