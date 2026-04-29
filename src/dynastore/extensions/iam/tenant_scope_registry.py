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

"""Declarative tenant-scope routing rules consumed by ``TenantScopeMiddleware``.

The middleware walks this registry on every request to decide whether a URL
needs per-catalog authorization. Routes themselves carry no authz wiring;
operators (or extension authors) add patterns here.

Each ``TenantScopeRule`` declares:
  - ``pattern``         — compiled regex matched against ``request.url.path``;
  - ``catalog_source``  — where to extract the catalog id from
                          (``"query:NAME"``, ``"path:NAME"``, ``"header:NAME"``);
  - ``default``         — value used when the source key is absent
                          (``"_system_"`` keeps platform-scope read sysadmin-only);
  - ``allow_anonymous`` — when False, anonymous callers are 401'd before
                          dispatch.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class TenantScopeRule:
    id: str
    pattern: re.Pattern
    catalog_source: str
    default: str
    allow_anonymous: bool


# Dashboard endpoints exposing per-catalog operational data: stats, logs,
# events, tasks, processes, ogc-compliance. All scope by ``?catalog_id=``;
# absence resolves to the platform-scope synthetic catalog (sysadmin-only).
_DASHBOARD_PATTERN = re.compile(
    r"^/web/dashboard/(stats|logs|events|tasks|processes|ogc-compliance)(/|\?|$|$)"
)


TENANT_SCOPED_ROUTES: List[TenantScopeRule] = [
    TenantScopeRule(
        id="dashboard_per_catalog",
        pattern=_DASHBOARD_PATTERN,
        catalog_source="query:catalog_id",
        default="_system_",
        allow_anonymous=False,
    ),
]


def match_tenant_scope_rule(path: str) -> Optional[TenantScopeRule]:
    """Return the first rule whose pattern matches ``path``, or ``None``."""
    for rule in TENANT_SCOPED_ROUTES:
        if rule.pattern.match(path):
            return rule
    return None
