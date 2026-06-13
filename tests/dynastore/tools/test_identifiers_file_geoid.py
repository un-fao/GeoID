#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Deterministic geoid for file-sourced features (file-backed collections, #373).

A file row carries a native feature id (fid) but no geoid. We derive a stable geoid
scoped by (catalog_id, collection_id, fid) so that:
  - GET /items/{id}, self/next links, _id=geoid in ES and STAC conformance keep
    working without a PostgreSQL row;
  - a reindex maps the same source row to the same geoid every time;
  - the SAME file published into two different collections (even within the same
    catalog) never clashes, because the catalog+collection scope differs.
"""
from __future__ import annotations

from uuid import UUID

import pytest


def test_derive_file_geoid_is_stable_across_calls():
    from dynastore.tools.identifiers import derive_file_geoid
    a = derive_file_geoid("cat1", "col1", "src-1")
    b = derive_file_geoid("cat1", "col1", "src-1")
    assert a == b


def test_derive_file_geoid_disambiguates_per_catalog():
    """Same collection_id string + same fid in a different catalog must differ."""
    from dynastore.tools.identifiers import derive_file_geoid
    a = derive_file_geoid("cat1", "col1", "src-1")
    b = derive_file_geoid("cat2", "col1", "src-1")
    assert a != b


def test_derive_file_geoid_disambiguates_per_collection():
    """Same file published twice into two collections of one catalog must not clash."""
    from dynastore.tools.identifiers import derive_file_geoid
    a = derive_file_geoid("cat1", "col1", "src-1")
    b = derive_file_geoid("cat1", "col2", "src-1")
    assert a != b


def test_derive_file_geoid_disambiguates_per_fid():
    from dynastore.tools.identifiers import derive_file_geoid
    a = derive_file_geoid("cat1", "col1", "src-1")
    b = derive_file_geoid("cat1", "col1", "src-2")
    assert a != b


def test_derive_file_geoid_format_is_uuidv5():
    from dynastore.tools.identifiers import derive_file_geoid
    parsed = UUID(derive_file_geoid("c", "cc", "s"))
    assert parsed.version == 5


def test_derive_file_geoid_namespace_is_pinned():
    """Snapshot for the file-geoid namespace + key shape. If this changes, every
    file-backed collection's ids shift and ES docs orphan — so it must stay constant."""
    from dynastore.tools.identifiers import derive_file_geoid
    assert derive_file_geoid("cat-x", "col-x", "src-1") == "3a2c7025-0356-5b35-bf27-e68d49cfe82f"


def test_derive_file_geoid_coerces_non_string_fid():
    """Integer/float fids (common in CSV/Parquet/Shapefile FID) must be handled
    without the caller pre-stringifying; 1 (int) must match "1" (str) for stability."""
    from dynastore.tools.identifiers import derive_file_geoid
    assert derive_file_geoid("cat1", "col1", 1) == derive_file_geoid("cat1", "col1", "1")


def test_derive_file_geoid_rejects_empty_fid():
    """An empty/None fid cannot produce a stable identity — callers must supply a
    content-hash fallback instead of silently collapsing every row to one geoid."""
    from dynastore.tools.identifiers import derive_file_geoid
    with pytest.raises(ValueError):
        derive_file_geoid("cat1", "col1", "")
    with pytest.raises(ValueError):
        derive_file_geoid("cat1", "col1", None)  # type: ignore[arg-type]
