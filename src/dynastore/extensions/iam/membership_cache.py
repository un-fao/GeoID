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

"""Per-pod cached lookup of a principal's catalog memberships.

Used by `CatalogMembershipHandler` (in `modules/iam/conditions.py`) and by
any future consumer that needs to know which catalogs a principal can see.
Cache key is `(provider, subject_id)` only — the `IamQueryProtocol` singleton
is excluded so cache hits survive test fixture teardown / setup boundaries.

Per-pod L1 only (`distributed=False`). 60 s TTL is the safe upper bound on
grant-change propagation; a Valkey RTT per request would cost more than the
saved DB query for short dashboard sessions. Upgrade to `distributed=True`
when Valkey is provisioned in dev + prod.
"""

from __future__ import annotations

from typing import Any, Dict

from dynastore.models.protocols.iam_query import IamQueryProtocol
from dynastore.tools.cache import cached


@cached(
    maxsize=512,
    ttl=60,
    jitter=10,
    namespace="dashboard_authz_membership",
    distributed=False,
    ignore=["iam_query"],
)
async def get_membership_cached(
    iam_query: IamQueryProtocol, provider: str, subject_id: str,
) -> Dict[str, Any]:
    return await iam_query.list_catalog_memberships(
        provider=provider, subject_id=subject_id,
    )
