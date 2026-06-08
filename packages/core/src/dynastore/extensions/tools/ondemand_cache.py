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

"""Shared GCS on-demand cache lookup for OGC feature responses.

The OGC Features and WFS services can optionally serve a pre-rendered response
from object storage (``cache_on_demand``). Both derive a stable cache key from
the request's query parameters and, on a HIT, stream the cached blob straight
back to the client. This module is the single source of that read path.
"""

import hashlib
import logging
import tempfile
from typing import Any, Mapping, Optional

from fastapi import Response

from dynastore.models.protocols.storage import StorageProtocol

logger = logging.getLogger(__name__)

# Volatile request params that must not influence the cache key (cache busters
# and credentials carried in the query string).
_EXCLUDED_PARAMS = ("_", "access_token", "token")


def ondemand_cache_key(
    bucket_name: str, cache_prefix: str, params: Mapping[str, Any]
) -> str:
    """Derive the ``gs://`` cache object path for a parameterised request.

    The key is stable across param ordering and ignores volatile params
    (cache busters, tokens), so the same logical request always maps to the
    same blob.
    """
    cache_params = {k: v for k, v in params.items() if k not in _EXCLUDED_PARAMS}
    param_str = "&".join(f"{k}={v}" for k, v in sorted(cache_params.items()))
    digest = hashlib.md5(param_str.encode(), usedforsecurity=False).hexdigest()
    return f"gs://{bucket_name}/{cache_prefix}/{digest}"


async def ondemand_cache_lookup(
    storage_svc: Optional[StorageProtocol],
    *,
    cache_prefix: str,
    catalog_id: str,
    params: Mapping[str, Any],
    media_type: str,
) -> Optional[Response]:
    """Return a cached ``Response`` for ``params`` on HIT, else ``None``.

    Resolves the catalog's storage bucket, probes the derived cache key and,
    when present, downloads the blob and wraps it in a ``Response`` with the
    given ``media_type``. Any of: no storage service, no bucket, or a cache
    MISS yields ``None`` so the caller falls through to live rendering.
    """
    if storage_svc is None:
        return None

    bucket_name = await storage_svc.get_storage_identifier(catalog_id)
    if not bucket_name:
        return None

    cache_key = ondemand_cache_key(bucket_name, cache_prefix, params)
    if not await storage_svc.file_exists(cache_key):
        return None

    logger.info("On-demand cache HIT (%s): %s", cache_prefix, cache_key)
    with tempfile.NamedTemporaryFile() as tmp:
        await storage_svc.download_file(cache_key, tmp.name)
        tmp.seek(0)
        cached_data = tmp.read()

    return Response(content=cached_data, media_type=media_type)
