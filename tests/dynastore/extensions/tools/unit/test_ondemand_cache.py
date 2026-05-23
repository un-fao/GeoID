#    Copyright 2025 FAO
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

"""Unit tests for the shared GCS on-demand cache lookup helper."""

import hashlib

import pytest

from dynastore.extensions.tools.ondemand_cache import (
    ondemand_cache_key,
    ondemand_cache_lookup,
)


class _FakeStorage:
    """Minimal StorageProtocol stand-in for the on-demand cache read path."""

    def __init__(self, *, bucket, exists, blob=b""):
        self._bucket = bucket
        self._exists = exists
        self._blob = blob
        self.file_exists_calls: list[str] = []
        self.download_calls: list[str] = []

    async def get_storage_identifier(self, catalog_id):
        return self._bucket

    async def file_exists(self, path):
        self.file_exists_calls.append(path)
        return self._exists

    async def download_file(self, source_path, target_path):
        self.download_calls.append(source_path)
        with open(target_path, "wb") as fh:
            fh.write(self._blob)


# ---------------------------------------------------------------------------
# ondemand_cache_key — pure key derivation
# ---------------------------------------------------------------------------


def test_cache_key_excludes_volatile_params():
    """``_``, ``access_token`` and ``token`` must not influence the key."""
    base = {"bbox": "0,0,1,1", "limit": "10"}
    volatile = {**base, "_": "12345", "access_token": "abc", "token": "def"}
    assert ondemand_cache_key("bkt", "features_cache", base) == ondemand_cache_key(
        "bkt", "features_cache", volatile
    )


def test_cache_key_is_order_independent():
    """Param insertion order must not change the derived key."""
    a = ondemand_cache_key("bkt", "wfs_cache", {"a": "1", "b": "2"})
    b = ondemand_cache_key("bkt", "wfs_cache", {"b": "2", "a": "1"})
    assert a == b


def test_cache_key_matches_legacy_format():
    """Key must be byte-identical to the inlined blocks it replaces."""
    params = {"bbox": "0,0,1,1", "limit": "10"}
    param_str = "bbox=0,0,1,1&limit=10"
    digest = hashlib.md5(param_str.encode()).hexdigest()
    assert (
        ondemand_cache_key("my-bucket", "features_cache", params)
        == f"gs://my-bucket/features_cache/{digest}"
    )


def test_cache_key_honours_prefix():
    params = {"x": "1"}
    assert "/features_cache/" in ondemand_cache_key("b", "features_cache", params)
    assert "/wfs_cache/" in ondemand_cache_key("b", "wfs_cache", params)


# ---------------------------------------------------------------------------
# ondemand_cache_lookup — async read path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lookup_returns_none_when_storage_missing():
    assert (
        await ondemand_cache_lookup(
            None,
            cache_prefix="features_cache",
            catalog_id="cat",
            params={"a": "1"},
            media_type="application/geo+json",
        )
        is None
    )


@pytest.mark.asyncio
async def test_lookup_returns_none_when_no_bucket():
    storage = _FakeStorage(bucket=None, exists=True, blob=b"x")
    assert (
        await ondemand_cache_lookup(
            storage,
            cache_prefix="features_cache",
            catalog_id="cat",
            params={"a": "1"},
            media_type="application/geo+json",
        )
        is None
    )


@pytest.mark.asyncio
async def test_lookup_returns_none_on_miss():
    storage = _FakeStorage(bucket="bkt", exists=False)
    result = await ondemand_cache_lookup(
        storage,
        cache_prefix="wfs_cache",
        catalog_id="cat",
        params={"a": "1"},
        media_type="application/xml",
    )
    assert result is None
    # the MISS must have probed the expected key
    assert storage.file_exists_calls == [
        ondemand_cache_key("bkt", "wfs_cache", {"a": "1"})
    ]
    assert storage.download_calls == []


@pytest.mark.asyncio
async def test_lookup_returns_response_on_hit():
    storage = _FakeStorage(bucket="bkt", exists=True, blob=b"cached-bytes")
    result = await ondemand_cache_lookup(
        storage,
        cache_prefix="features_cache",
        catalog_id="cat",
        params={"a": "1"},
        media_type="application/geo+json",
    )
    assert result is not None
    assert result.body == b"cached-bytes"
    assert result.media_type == "application/geo+json"
    expected_key = ondemand_cache_key("bkt", "features_cache", {"a": "1"})
    assert storage.download_calls == [expected_key]
