"""Unit tests for the STAC catalog-creation capability precheck.

``_assert_stac_capable_metadata_stack`` refuses a STAC catalog create
when neither ``CatalogMetadataStore`` nor ``CollectionMetadataStore``
has a registered driver declaring ``domain == MetadataDomain.STAC``.
Default PG deployment satisfies the check (PG Stac drivers register at
both tiers); custom configs pointing at STAC-blind backends must fail
loudly so the STAC envelope isn't silently dropped on write.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.stac.stac_service import (
    _assert_stac_capable_metadata_stack,
)
from dynastore.models.protocols.driver_roles import MetadataDomain


def _fake_driver(domain):
    d = MagicMock()
    d.domain = domain
    return d


def test_raises_when_no_catalog_stac_driver_registered():
    """No CatalogMetadataStore with STAC domain → reject the create."""

    def _get_protocols(proto_cls):
        # Collection-tier has a STAC driver; catalog-tier does not.
        if proto_cls.__name__ == "CatalogMetadataStore":
            return [_fake_driver(MetadataDomain.CORE)]
        return [_fake_driver(MetadataDomain.STAC)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        with pytest.raises(HTTPException) as exc:
            _assert_stac_capable_metadata_stack()

    assert exc.value.status_code == 422
    assert "CatalogMetadataStore" in exc.value.detail


def test_warns_but_proceeds_when_no_collection_stac_driver_registered(caplog):
    """Missing collection-tier STAC driver is a WARNING, not a reject.

    A STAC catalog can be created before any STAC collection exists;
    operators may register the collection-tier STAC driver later.
    """

    def _get_protocols(proto_cls):
        if proto_cls.__name__ == "CollectionMetadataStore":
            return [_fake_driver(MetadataDomain.CORE)]
        return [_fake_driver(MetadataDomain.STAC)]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        with caplog.at_level("WARNING"):
            _assert_stac_capable_metadata_stack()  # should not raise

    assert any(
        "CollectionMetadataStore STAC driver" in r.message
        for r in caplog.records
    )


def test_passes_when_both_tiers_have_stac_driver():
    """Default PG config: STAC drivers at both tiers → no raise."""

    def _get_protocols(_proto_cls):
        # Mix of CORE + STAC at both tiers — STAC is present.
        return [
            _fake_driver(MetadataDomain.CORE),
            _fake_driver(MetadataDomain.STAC),
        ]

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        side_effect=_get_protocols,
    ):
        _assert_stac_capable_metadata_stack()  # should not raise


def test_raises_when_registry_empty_on_catalog_tier():
    """Empty catalog-tier registry → hard reject (catalog can't be STAC).

    A deployment with no registered ``CatalogMetadataStore`` at all
    fails the catalog-tier hard requirement even if the collection
    tier might come up later.
    """

    with patch(
        "dynastore.extensions.stac.stac_service.get_protocols",
        return_value=[],
    ):
        with pytest.raises(HTTPException) as exc:
            _assert_stac_capable_metadata_stack()

    assert exc.value.status_code == 422
    assert "CatalogMetadataStore" in exc.value.detail
