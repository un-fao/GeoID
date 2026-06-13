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

"""Guards on ItemMetadataSidecar.prepare_upsert_payload when the STAC
extension is absent from the running service's SCOPE.

Repro for the cross-service write-path crash: a non-STAC service (e.g.
maps, SCOPE ``api_maps_open``) writing to a collection whose
``col_config.sidecars`` was persisted by the STAC-aware catalog service
used to fail with a raw ``ModuleNotFoundError: No module named
'dynastore.extensions.stac'``. After the guard, it raises
``ConfigResolutionError`` with the catalog/collection in the message
and an actionable install hint.
"""

import sys
import importlib
import pytest

from dynastore.modules.db_config.exceptions import ConfigResolutionError
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata import (
    ItemMetadataSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata_config import (
    ItemMetadataSidecarConfig,
)


def _hide_stac_extension(monkeypatch):
    """Make every ``dynastore.extensions.stac.*`` import raise ImportError."""
    for mod in list(sys.modules):
        if mod == "dynastore.extensions.stac" or mod.startswith(
            "dynastore.extensions.stac."
        ):
            monkeypatch.delitem(sys.modules, mod, raising=False)

    real_import = importlib.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "dynastore.extensions.stac" or name.startswith(
            "dynastore.extensions.stac."
        ):
            raise ImportError(f"No module named '{name}'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", blocked_import)


class TestItemMetadataSidecarStacGuard:
    def test_raises_config_resolution_error_when_stac_unavailable(
        self, monkeypatch
    ):
        _hide_stac_extension(monkeypatch)

        sidecar = ItemMetadataSidecar(ItemMetadataSidecarConfig())
        feature = {"id": "item-1", "properties": {"title": "t"}}
        context = {
            "geoid": "g-1",
            "catalog_id": "cat-A",
            "collection_id": "col-X",
        }

        with pytest.raises(ConfigResolutionError) as excinfo:
            sidecar.prepare_upsert_payload(feature, context)

        msg = str(excinfo.value)
        assert "cat-A" in msg
        assert "col-X" in msg
        assert "extension_stac" in msg
        assert excinfo.value.missing_key == "extension:stac"

    def test_error_chains_original_import_error(self, monkeypatch):
        _hide_stac_extension(monkeypatch)

        sidecar = ItemMetadataSidecar(ItemMetadataSidecarConfig())
        with pytest.raises(ConfigResolutionError) as excinfo:
            sidecar.prepare_upsert_payload(
                {"id": "x", "properties": {}},
                {"geoid": "g", "catalog_id": "c", "collection_id": "k"},
            )

        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_unknown_collection_falls_back_to_placeholder(self, monkeypatch):
        _hide_stac_extension(monkeypatch)

        sidecar = ItemMetadataSidecar(ItemMetadataSidecarConfig())
        with pytest.raises(ConfigResolutionError) as excinfo:
            sidecar.prepare_upsert_payload(
                {"id": "x", "properties": {}},
                {"geoid": "g"},
            )

        msg = str(excinfo.value)
        assert "<unknown>" in msg
