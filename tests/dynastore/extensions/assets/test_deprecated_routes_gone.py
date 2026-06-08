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

"""Regression: deprecated assets routes and shims must not be re-introduced."""
from __future__ import annotations

import pytest


def test_upload_provider_property_is_removed():
    from dynastore.extensions.assets.assets_service import AssetService

    assert not hasattr(AssetService, "upload_provider"), (
        "AssetService.upload_provider was removed; use resolve_upload_driver()"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/assets/search",
        "/assets/catalogs/some-cat/search",
    ],
)
def test_deprecated_search_aliases_are_unmounted(path):
    """The /search aliases were removed; only /assets-search remains."""
    from dynastore.extensions.assets.assets_service import AssetService

    service = AssetService.__new__(AssetService)
    routes = getattr(service, "router", None)
    if routes is None:
        # Service not yet bootstrapped in this lightweight test; assert by source-grep instead.
        import inspect
        src = inspect.getsource(AssetService)
        assert '"/search"' not in src, "deprecated /search alias re-introduced"
        assert '"/catalogs/{catalog_id}/search"' not in src, (
            "deprecated /catalogs/{catalog_id}/search alias re-introduced"
        )
