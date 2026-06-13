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

"""Regression guard: ``TilePreseedRequest`` no longer accepts operation='invalidate'.

The invalidate path has been moved to the dedicated ``tiles_invalidate`` task
type (``TileInvalidateTask``). This module verifies that the preseed model's
operation field is restricted to seed/renew only.
"""
from __future__ import annotations

import pytest

from dynastore.tasks.tiles_preseed.models import TilePreseedRequest
from pydantic import ValidationError


def test_preseed_operation_does_not_accept_invalidate():
    """TilePreseedRequest must reject operation='invalidate' (moved to tiles_invalidate)."""
    with pytest.raises(ValidationError, match="operation"):
        TilePreseedRequest(
            catalog_id="cat",
            collection_id="col",
            operation="invalidate",
        )


def test_preseed_operation_accepts_seed():
    """TilePreseedRequest accepts operation='seed'."""
    req = TilePreseedRequest(catalog_id="cat", collection_id="col", operation="seed")
    assert req.operation == "seed"


def test_preseed_operation_accepts_renew():
    """TilePreseedRequest accepts operation='renew'."""
    req = TilePreseedRequest(catalog_id="cat", collection_id="col", operation="renew")
    assert req.operation == "renew"


def test_preseed_operation_default_is_seed():
    """TilePreseedRequest default operation is 'seed'."""
    req = TilePreseedRequest(catalog_id="cat", collection_id="col")
    assert req.operation == "seed"
