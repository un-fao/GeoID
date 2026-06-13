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

"""``_create_bucket_sync`` must report whether it actually created the bucket.

It returns ``(bucket, created)``:
- ``created is True``  — ``bucket.create()`` succeeded; this call owns the bucket.
- ``created is False`` — a ``Conflict`` was resolved by fetching a pre-existing
  bucket; callers MUST NOT orphan-delete it.
- ``BucketConflictError`` — the name belongs to a bucket in another GCP project
  (``Conflict`` then ``Forbidden`` on ``get_bucket``); not ours to claim or delete.

This is the leaf guard behind the catalog-provisioning data-loss fix: without the
flag, a link failure on a pre-existing bucket would force-delete another owner's
bucket and all its objects.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from google.api_core.exceptions import Conflict, Forbidden

from dynastore.modules.gcp.tools.bucket import BucketConflictError, _create_bucket_sync
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig, GcpLocation


def _config() -> GcpCatalogBucketConfig:
    return GcpCatalogBucketConfig(location=GcpLocation("europe-west1"))


def test_returns_created_true_when_bucket_is_created():
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    bucket.create.return_value = None  # success

    result_bucket, created = _create_bucket_sync(
        "proj-cat-1", _config(), "proj", client=client
    )

    assert result_bucket is bucket
    assert created is True
    bucket.create.assert_called_once()


def test_returns_created_false_when_bucket_already_exists_in_our_project():
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    bucket.create.side_effect = Conflict("bucket already exists")
    existing = MagicMock()
    client.get_bucket.return_value = existing

    result_bucket, created = _create_bucket_sync(
        "proj-cat-1", _config(), "proj", client=client
    )

    assert result_bucket is existing
    assert created is False


def test_raises_bucket_conflict_when_name_owned_by_another_project():
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    bucket.create.side_effect = Conflict("bucket already exists")
    client.get_bucket.side_effect = Forbidden("no storage.buckets.get")

    with pytest.raises(BucketConflictError, match="another GCP project"):
        _create_bucket_sync("proj-cat-1", _config(), "proj", client=client)
