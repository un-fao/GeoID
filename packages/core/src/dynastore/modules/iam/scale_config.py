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

"""Runtime accessor for :class:`IamScaleConfig` + per-binding quota wiring.

``get_iam_scale_config`` mirrors ``IamService._get_roles_config``: it reads
the persisted config through :class:`PlatformConfigsProtocol` so a runtime
PATCH applies on the next request, and falls back to the schema defaults
whenever the protocol or row is unavailable (config must never block the
authz hot path). It is a free function rather than a service method because
both the module wiring (``valkey_required``) and the policy resolver
(per-binding quota defaults) consume it.

``quota_to_conditions`` turns a grant's ``quota`` JSONB into the
``rate_limit`` / ``max_count`` conditions the existing
:mod:`dynastore.modules.iam.conditions` handlers already enforce. The
counter *namespace* is the grant id, so two grants that differ only by
``resource_ref`` (e.g. ``100/min on collection A`` vs ``collection B``)
land on distinct counter rows — per-scope counting falls out for free.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from dynastore.models.auth import Condition
from dynastore.models.protocols.authorization import IamScaleConfig
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.modules import get_protocol
from dynastore.modules.iam.iam_queries import build_usage_counters_steps

logger = logging.getLogger(__name__)

__all__ = [
    "IamScaleConfig",
    "build_usage_counters_steps",
    "get_iam_scale_config",
    "quota_namespace",
    "quota_to_conditions",
    "usage_counter_hash_partitions",
]


async def get_iam_scale_config() -> IamScaleConfig:
    """Return the active :class:`IamScaleConfig`, or defaults on any failure."""
    configs = get_protocol(PlatformConfigsProtocol)
    if configs is None:
        return IamScaleConfig()
    try:
        cfg = await configs.get_config(IamScaleConfig)
        if isinstance(cfg, IamScaleConfig):
            return cfg
        return IamScaleConfig.model_validate(cfg)
    except Exception:
        logger.debug(
            "IamScaleConfig unavailable; using defaults", exc_info=True
        )
        return IamScaleConfig()


def usage_counter_hash_partitions() -> int:
    """Structural hash-partition count for the ``usage_counters`` sink.

    Read from the ``IAM_USAGE_COUNTER_HASH_PARTITIONS`` env var rather than
    the persisted config, because schema-init runs before the platform
    config row is reliably readable. Defaults to 1 (flat table). Mirrors
    :attr:`IamScaleConfig.usage_counter_hash_partitions`, which carries the
    same value for Configuration-Hub visibility.
    """
    try:
        n = int(os.environ.get("IAM_USAGE_COUNTER_HASH_PARTITIONS", "1"))
    except (TypeError, ValueError):
        return 1
    return n if n >= 1 else 1


def quota_namespace(grant_id: Any) -> str:
    """Counter namespace for a grant binding.

    Distinct per grant row (and therefore per ``resource_ref`` scope, since
    the resource scope is part of the grant's identity), so collection-A and
    collection-B quotas on the same principal never share a bucket.
    """
    return f"grant:{grant_id}"


def quota_to_conditions(
    quota: Optional[Dict[str, Any]],
    namespace: str,
    *,
    default_rate_limit: Optional[Dict[str, Any]] = None,
    default_quota: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Condition], Dict[int, str]]:
    """Build the rate-limit / lifetime-quota conditions for one grant.

    ``quota`` is the grant's ``quota`` JSONB. Recognised keys:

    * ``rate_limit`` — dict mirroring the ``rate_limit`` condition config
      (``limit`` + ``window_seconds`` required; optional ``scope`` /
      ``path_pattern`` / ``methods`` / ``mode``).
    * ``max_count`` — dict mirroring the ``max_count`` condition config
      (``limit`` required; same optional gates).

    When the grant omits a key, the matching ``IamScaleConfig`` default
    (``default_rate_limit`` / ``default_quota``) is used as a fallback —
    so a per-binding spec always wins over the platform default. Returns
    the conditions and a ``{id(config): namespace}`` mapping the caller
    merges into ``ctx.extras['_policy_id_by_config_id']`` so the handlers
    can namespace their counter rows without mutating the condition config
    in place.
    """
    quota = quota or {}
    conditions: List[Condition] = []
    mapping: Dict[int, str] = {}

    rate_spec = quota.get("rate_limit")
    if not isinstance(rate_spec, dict):
        rate_spec = default_rate_limit
    if isinstance(rate_spec, dict) and int(rate_spec.get("limit", 0) or 0) > 0:
        cfg = dict(rate_spec)
        cfg.setdefault("scope", "principal")
        cond = Condition(type="rate_limit", config=cfg)
        conditions.append(cond)
        mapping[id(cond.config)] = namespace

    count_spec = quota.get("max_count")
    if not isinstance(count_spec, dict):
        count_spec = default_quota
    if isinstance(count_spec, dict) and int(count_spec.get("limit", 0) or 0) > 0:
        cfg = dict(count_spec)
        cfg.setdefault("scope", "principal")
        cond = Condition(type="max_count", config=cfg)
        conditions.append(cond)
        mapping[id(cond.config)] = namespace

    return conditions, mapping
