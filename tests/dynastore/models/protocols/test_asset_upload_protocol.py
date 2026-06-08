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

"""Contract tests: AssetUploadProtocol surface and shipped implementors."""
from __future__ import annotations

import pytest

from dynastore.models.protocols.asset_upload import AssetUploadProtocol


def test_protocol_declares_driver_id_and_supports_versioning():
    # `runtime_checkable` Protocols don't enforce attributes via isinstance,
    # but the attribute names must exist on the class for type checkers
    # and for downstream users.
    assert "driver_id" in AssetUploadProtocol.__annotations__
    assert "supports_versioning" in AssetUploadProtocol.__annotations__


def test_protocol_has_initiate_and_status_methods():
    assert hasattr(AssetUploadProtocol, "initiate_upload")
    assert hasattr(AssetUploadProtocol, "get_upload_status")


def test_gcp_driver_exposes_modern_protocol_attrs():
    from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin

    assert getattr(GcpStorageOpsMixin, "driver_id", None) == "gcs"
    assert getattr(GcpStorageOpsMixin, "supports_versioning", None) is False


def test_local_driver_exposes_modern_protocol_attrs():
    from dynastore.modules.local.local_upload import LocalUploadModule

    assert getattr(LocalUploadModule, "driver_id", None) == "local"
    assert getattr(LocalUploadModule, "supports_versioning", None) is False


def test_driver_ids_are_unique_across_shipped_impls():
    from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin
    from dynastore.modules.local.local_upload import LocalUploadModule

    seen = {GcpStorageOpsMixin.driver_id, LocalUploadModule.driver_id}
    assert len(seen) == 2, f"driver_id collision: {seen}"
