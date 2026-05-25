"""Unit tests for process scope→URL alignment, including asset-targeting work.

Asset-targeting processes (e.g. ``gdal``) declare CATALOG and/or COLLECTION
scope and take ``asset_id`` as a regular ``inputs`` value. They execute at the
catalog mount for a catalog-level asset, or the collection mount for a
collection-level asset — there is no dedicated ``/assets/{asset_id}`` URL
surface. These tests cover the scope-allow rules and the path→inputs injection
(which copies ``catalog_id``/``collection_id`` but never ``asset_id``).
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from dynastore.extensions.processes.processes_service import (
    _allowed_scopes_for,
    _inject_path_into_inputs,
    _validate_process_scope_or_raise,
)
from dynastore.modules.processes import models

PLATFORM = models.ProcessScope.PLATFORM
CATALOG = models.ProcessScope.CATALOG
COLLECTION = models.ProcessScope.COLLECTION


def _process(scopes):
    return models.Process(
        id="gdal",
        title="GDAL Info",
        version="1.0.0",
        scopes=scopes,
        jobControlOptions=[models.JobControlOptions.ASYNC_EXECUTE],
        inputs={},
        outputs={},
    )


def test_allowed_scopes_follow_url_mount():
    assert _allowed_scopes_for(None, None) == frozenset({PLATFORM})
    assert _allowed_scopes_for("c1", None) == frozenset({CATALOG})
    assert _allowed_scopes_for("c1", "col1") == frozenset({COLLECTION})


def test_asset_process_accepted_at_catalog_mount():
    # gdal declares [CATALOG, COLLECTION]; valid at the catalog mount.
    _validate_process_scope_or_raise(
        _process([CATALOG, COLLECTION]), catalog_id="c1", collection_id=None
    )


def test_asset_process_accepted_at_collection_mount():
    _validate_process_scope_or_raise(
        _process([CATALOG, COLLECTION]), catalog_id="c1", collection_id="col1"
    )


def test_catalog_only_process_rejected_at_collection_mount():
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([CATALOG]), catalog_id="c1", collection_id="col1"
        )
    assert exc.value.status_code == 400


def test_collection_only_process_rejected_at_catalog_mount():
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([COLLECTION]), catalog_id="c1", collection_id=None
        )
    assert exc.value.status_code == 400


def test_inject_path_adds_catalog_and_collection_but_not_asset():
    req = models.ExecuteRequest(inputs={"asset_id": "a1"})
    out = _inject_path_into_inputs(req, catalog_id="c1", collection_id="col1")
    # asset_id is a body input, preserved untouched; path ids injected.
    assert out.inputs == {
        "asset_id": "a1",
        "catalog_id": "c1",
        "collection_id": "col1",
    }


def test_inject_path_rejects_conflicting_catalog_id():
    req = models.ExecuteRequest(inputs={"catalog_id": "other"})
    with pytest.raises(HTTPException) as exc:
        _inject_path_into_inputs(req, catalog_id="c1", collection_id=None)
    assert exc.value.status_code == 400
