#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Shared FastAPI query-parameter definitions for the 3 composed-config GETs.

`get_platform_config_composed` / `get_catalog_config_composed` /
`get_collection_config_composed` all expose the same five query knobs
(`resolved`, `meta`, `include`, `strict`, `links`).  Lifting them into
`Annotated` aliases here keeps the pattern + description in ONE place —
handlers just spell ``links: LinksQuery = "none"`` and FastAPI builds
the same OpenAPI surface at every tier.

Defaults live on the parameter (``= "none"``) rather than inside
``Query(...)`` because FastAPI rejects setting both when ``Annotated``
is used.
"""

from typing import Annotated

from fastapi import Query


ResolvedQuery = Annotated[
    bool,
    Query(
        description=(
            "When true (default): all registered configs with waterfall-resolved values. "
            "When false: only configs explicitly stored at the requested scope."
        ),
    ),
]


MetaQuery = Annotated[
    str,
    Query(
        pattern="^(none|field|schema)$",
        description=(
            "Per-class documentation mode injected INLINE on each "
            "in-scope plugin leaf as a ``_meta`` sibling.  "
            "``field`` (default): leaf carries ``_meta = {field_docs: "
            "{field_name: description}}``.  ``schema``: leaf carries "
            "``_meta = {json_schema: <full Pydantic schema>}`` "
            "(title/description/type/default/examples/constraints — "
            "everything a form-builder needs; heavier).  ``none``: "
            "no ``_meta`` key on any leaf."
        ),
    ),
]


IncludeQuery = Annotated[
    str,
    Query(
        pattern="^(scope|upstream)$",
        description=(
            "Body-rendering mode. ``scope`` (default): body shows configs "
            "owned by this scope (``_visibility`` matches OR an explicit "
            "row exists here). Upstream-tier configs are summarised in "
            "the hierarchical ``inherited`` tree — leaves carry "
            "``{source: <tier>}`` at the same address the resolved value "
            "would land at if inlined. ``upstream``: every visible "
            "class is rendered with its waterfall-resolved value (today's "
            "verbose mode). At platform scope this flag is a no-op since "
            "platform IS the top tier."
        ),
    ),
]


StrictQuery = Annotated[
    bool,
    Query(
        description=(
            "Cycle F.7d.2 — narrow the response body to configs strictly "
            "related to the requested scope.  At platform scope, ``true`` "
            "(default) drops catalog-/collection-tier templates from the "
            "body and routes them to ``inherited`` instead, so platform "
            "view shows only platform-intrinsic configs (``modules``, "
            "``extensions``, ``tasks``, ``engines``).  ``false`` restores "
            "the previous always-true platform-scope inclusion.  No effect "
            "at catalog or collection scope (per-tier ``_visibility`` "
            "filter already runs there); accepted for API symmetry so the "
            "same query-string template works at every tier."
        ),
    ),
]


LinksQuery = Annotated[
    str,
    Query(
        pattern="^(none|minimal|full)$",
        description=(
            "Per-plugin HATEOAS edit affordances injected INLINE on "
            "each in-scope leaf as a ``_links`` sibling — ``self`` "
            "(GET), ``edit`` (PUT — replace), ``edit`` (DELETE — clear "
            "override), ``describedby`` (GET registry/{class_key}).  "
            "``none`` (default): no ``_links`` on any leaf, "
            "wire-compatible with pre-#517 clients.  ``minimal``: "
            "rel/href/method only.  ``full``: adds a contextual "
            "``title`` per link naming the class key and tier."
        ),
    ),
]
