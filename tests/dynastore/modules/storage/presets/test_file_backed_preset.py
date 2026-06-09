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

"""The file_backed preset (#378): driver + routing + discoverability composition."""
from __future__ import annotations


def _bundle(**params):
    from dynastore.modules.storage.presets.file_backed import (
        FileBackedPreset, FileBackedPresetParams,
    )
    p = FileBackedPresetParams(**params)
    return FileBackedPreset()._build_bundle(p, {"catalog_id": "cat1", "collection_id": "col1"})


def _by_slot(bundle):
    return {e.slot: e for e in bundle.entries}


def test_preset_is_registered():
    from dynastore.modules.storage.presets import find_preset
    preset = find_preset("file_backed")
    assert preset is not None
    assert preset.name == "file_backed"


def test_bundle_writes_duckdb_config_with_asset_binding():
    slots = _by_slot(_bundle(asset_id="asset-7", format="gpkg", id_column="fid"))
    cfg = slots["duckdb_driver_config"].instance
    assert cfg.asset_id == "asset-7"
    assert cfg.format == "gpkg"
    assert cfg.id_column == "fid"


def test_discoverable_routes_file_read_and_es_secondary():
    slots = _by_slot(_bundle(asset_id="a", discoverable=True))
    routing = slots["items_routing"].instance
    ops = routing.operations

    # READ resolves the file driver via GEOMETRY_EXACT (file = exact source).
    read_refs = [e.driver_ref for e in ops["READ"]]
    assert read_refs == ["items_duckdb_driver"]
    from dynastore.modules.storage.hints import Hint
    assert Hint.GEOMETRY_EXACT in ops["READ"][0].hints

    # WRITE has the ES secondary indexer so the file->ES reindex can run.
    write = ops["WRITE"]
    assert any(e.driver_ref == "items_elasticsearch_driver" and e.secondary_index for e in write)

    # SEARCH prefers ES, falls back to the file driver.
    search_refs = [e.driver_ref for e in ops["SEARCH"]]
    assert search_refs[0] == "items_elasticsearch_driver"
    assert "items_duckdb_driver" in search_refs

    # Discoverable bundle requests geometry simplification for the global index.
    es_cfg = _by_slot(_bundle(asset_id="a", discoverable=True))["es_driver_config"].instance
    assert es_cfg.simplify_geometry is True


def test_non_discoverable_has_no_es_and_no_write():
    bundle = _bundle(asset_id="a", discoverable=False)
    slots = _by_slot(bundle)
    assert "es_driver_config" not in slots
    ops = slots["items_routing"].instance.operations
    assert "WRITE" not in ops or not ops["WRITE"]
    # SEARCH falls back to the file driver only.
    assert [e.driver_ref for e in ops["SEARCH"]] == ["items_duckdb_driver"]


def test_simplify_geometry_param_is_respected():
    slots = _by_slot(_bundle(asset_id="a", discoverable=True, simplify_geometry=False))
    assert slots["es_driver_config"].instance.simplify_geometry is False
