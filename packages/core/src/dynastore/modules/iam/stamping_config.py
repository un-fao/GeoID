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

"""Per-collection attribute-stamping policy config.

:class:`AttributeStampingPolicy` is a :class:`~dynastore.modules.db_config.plugin_config.PluginConfig`
scoped to ``(catalog_id, collection_id)``.  When present it declares which
Feature properties to lift onto the document's ``_attrs`` envelope at write
time.  The ``_attrs`` envelope is then queried by the read-filter compiler to
enforce per-document ABAC restrictions from :data:`iam.grants.attribute_predicates`.

Missing config (collection not enrolled in ABAC stamping) ⟹ no ``_attrs`` key
is written — byte-for-byte today's behaviour except that ``_grant_subjects``
is also no longer written (retired by #1441).

Example config:

    PUT /configs/catalogs/{cat}/collections/{col}/plugins/attribute_stamping_policy
    {
        "attribute_paths": {
            "dept":        "$.properties.department",
            "sensitivity": "$.properties.classification"
        }
    }

Each key in ``attribute_paths`` becomes ``_attrs.<key>`` on the stored
document; the value is a simple property path of the form
``$.properties.<field_name>``.  Only direct ``properties`` sub-keys are
supported in this first slice; nested paths and array selectors are deferred.

Promoted columns (F5): keys in ``promoted_columns`` have a corresponding
``_attr_{key}`` first-class PG column that mirrors the JSONB envelope value.
The admin endpoint ``POST /admin/catalogs/{cat}/collections/{coll}/attrs/promote``
enqueues the DDL migration and appends to this list.  Only keys that also
appear in ``attribute_paths`` are eligible for promotion — validated at config
load time.
"""
from __future__ import annotations

import re
from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import Field, field_validator, model_validator

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

__all__ = ["AttributeStampingPolicy", "_KEY_PATTERN", "_ALLOWED_PG_TYPES"]

#: Regex applied to promoted-column keys (must match AttributePredicate.key).
_KEY_PATTERN = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")

#: PG types that the promotion migration accepts.
_ALLOWED_PG_TYPES = frozenset({"TEXT", "NUMERIC", "TIMESTAMPTZ", "TEXT[]"})


class AttributeStampingPolicy(PluginConfig):
    """Per-collection policy for write-time attribute stamping.

    When ``attribute_paths`` is non-empty, :meth:`ItemService._resolve_access_envelope`
    extracts each declared path from the Feature and populates ``_attrs`` on
    the index payload.  Drivers that do not understand ``_attrs`` ignore it.

    Activate via::

        PUT /configs/catalogs/{cat}/collections/{col}/plugins/attribute_stamping_policy
        {"attribute_paths": {"dept": "$.properties.department"}}

    Promoted columns: when ``promoted_columns`` is non-empty, the write path
    also updates ``_attr_{key}`` first-class columns alongside the JSONB
    envelope.  The PG access-filter translator uses direct-column predicates
    for promoted keys (btree/GIN index scan) and falls back to the JSONB path
    for non-promoted ones.
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "attribute_stamping_policy",
    )
    _freeze_at: ClassVar[Optional[str]] = "collection"

    attribute_paths: Mutable[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Map of envelope attribute key → JSONPath to the Feature property. "
            "Only ``$.properties.<field>`` paths are supported in this slice. "
            "Example: {\"dept\": \"$.properties.department\"}. "
            "Missing config or empty dict = no attribute stamping."
        ),
    )

    promoted_columns: Mutable[List[str]] = Field(
        default_factory=list,
        description=(
            "Keys from ``attribute_paths`` that have been promoted to first-class "
            "PG columns (``_attr_{key}``).  Only keys also present in "
            "``attribute_paths`` are eligible.  Populated by the admin promotion "
            "endpoint; do not edit manually."
        ),
    )

    promoted_column_types: Mutable[Dict[str, str]] = Field(
        default_factory=dict,
        description=(
            "Map of promoted key → PG type string.  Allowed values: "
            "``TEXT``, ``NUMERIC``, ``TIMESTAMPTZ``, ``TEXT[]``.  "
            "Defaults to ``TEXT`` when a key is not listed."
        ),
    )

    @field_validator("promoted_columns", mode="before")
    @classmethod
    def _validate_promoted_column_keys(cls, v: object) -> object:
        """Each promoted key must match the attribute-key regex."""
        if not isinstance(v, list):
            return v
        for key in v:
            if not isinstance(key, str) or not _KEY_PATTERN.fullmatch(key):
                raise ValueError(
                    f"promoted_columns entry {key!r} must match "
                    "[A-Za-z_][A-Za-z0-9_]* (no dots, quotes, or whitespace)."
                )
        return v

    @field_validator("promoted_column_types", mode="before")
    @classmethod
    def _validate_promoted_column_types(cls, v: object) -> object:
        """Each type value must be in the allowlist."""
        if not isinstance(v, dict):
            return v
        for key, pg_type in v.items():
            if not isinstance(key, str) or not _KEY_PATTERN.fullmatch(key):
                raise ValueError(
                    f"promoted_column_types key {key!r} must match "
                    "[A-Za-z_][A-Za-z0-9_]*."
                )
            if pg_type not in _ALLOWED_PG_TYPES:
                raise ValueError(
                    f"promoted_column_types[{key!r}] = {pg_type!r} is not "
                    f"allowed; must be one of {sorted(_ALLOWED_PG_TYPES)}."
                )
        return v

    @model_validator(mode="after")
    def _promoted_must_be_subset_of_paths(self) -> "AttributeStampingPolicy":
        """Every promoted key must also exist in ``attribute_paths``."""
        paths = set(self.attribute_paths or {})
        promoted = list(self.promoted_columns or [])
        orphans = [k for k in promoted if k not in paths]
        if orphans:
            raise ValueError(
                f"promoted_columns keys {orphans} are not declared in "
                "attribute_paths; remove them from promoted_columns or add "
                "the corresponding attribute_paths entries first."
            )
        return self
