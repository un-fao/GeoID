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

"""Response-level i18n helpers for OGC API endpoints.

These utilities operate on *serialized dicts* (output of model_dump) rather
than on live Pydantic model instances.  That distinction is intentional:
``Link.title`` carries a ``@field_validator("title", mode="before")`` that
re-wraps any plain ``str`` value back into ``{"en": str}`` on every
validation pass.  Resolving on the dumped dict avoids triggering that
validator and keeps plain strings on the wire for the chosen language.

Typical use::

    from dynastore.extensions.tools.response_i18n import localize_model

    data = localize_model(landing_page, language)
    return JSONResponse(content=data)
"""

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from dynastore.models.localization import (
    LocalizedDTO,
    is_multilanguage_input,
)


def resolve_localized(value: Any, lang: str) -> Any:
    """Collapse a localised value to a single language string.

    Collapse rules (applied in order):
    * ``lang == '*'`` — return *value* unchanged (full round-trip passthrough).
    * ``value is None`` — return ``None``.
    * Plain ``str`` — return unchanged.
    * :class:`~dynastore.models.localization.LocalizedDTO` (including
      ``LocalizedText``) — delegate to ``.resolve(lang, default="en")``.
    * Language-keyed ``dict`` (detected via
      :func:`~dynastore.models.localization.is_multilanguage_input`) —
      collapse with the same fallback order as ``LocalizedDTO.resolve``:
      exact → base-language → ``"en"`` → first key.
    * Anything else — return unchanged.
    """
    if lang == "*":
        return value
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, LocalizedDTO):
        return value.resolve(lang, default="en")
    if is_multilanguage_input(value):
        # Replicate LocalizedDTO.resolve fallback order on a plain dict.
        data: Dict[str, Any] = {k: v for k, v in value.items() if v is not None}
        if not data:
            return None
        # 1. Exact match
        if lang in data:
            return data[lang]
        # 2. Base language (e.g. "en" from "en-US")
        base = lang.split("-")[0]
        if base in data:
            return data[base]
        # 3. Default "en"
        if "en" in data:
            return data["en"]
        # 4. First available
        return next(iter(data.values()))
    return value


def resolve_links(links: Optional[List[Any]], lang: str) -> Optional[List[Dict[str, Any]]]:
    """Dump each Link and resolve its ``title`` field to *lang*.

    Each element is serialized via ``model_dump(by_alias=True,
    exclude_none=True)`` when it is a Pydantic model, otherwise passed
    through as-is.  The ``title`` key in the resulting dict is then
    collapsed with :func:`resolve_localized`.

    ``lang == '*'`` is a passthrough: links are still dumped to dicts but
    their ``title`` values (which are stored as ``{"en": "..."}`` after the
    Link validator runs) are returned as-is (full multi-language dicts).
    """
    if links is None:
        return None
    result: List[Dict[str, Any]] = []
    for link in links:
        if isinstance(link, BaseModel):
            d: Dict[str, Any] = link.model_dump(by_alias=True, exclude_none=True)
        elif isinstance(link, dict):
            d = dict(link)
        else:
            continue
        if "title" in d:
            d["title"] = resolve_localized(d["title"], lang)
            if d["title"] is None:
                del d["title"]
        result.append(d)
    return result


def localize_response_dict(
    data: Dict[str, Any],
    lang: str,
    *,
    text_fields: Tuple[str, ...] = ("title", "description"),
    link_keys: Tuple[str, ...] = ("links",),
) -> Dict[str, Any]:
    """Resolve localised fields in a serialized response dict, in-place.

    * Each key in *text_fields* at the top level is passed through
      :func:`resolve_localized`.
    * Each key in *link_keys* is treated as a list of link objects and
      passed through :func:`resolve_links`.

    The dict is mutated **and** returned so callers can chain the call::

        data = localize_response_dict(landing_page.model_dump(...), lang)

    ``lang == '*'`` is a no-op for plain-string fields (they are left
    unchanged) but the full multi-language dict is preserved on localised
    fields.
    """
    for field in text_fields:
        if field in data:
            resolved = resolve_localized(data[field], lang)
            if resolved is None:
                del data[field]
            else:
                data[field] = resolved

    for lk in link_keys:
        if lk in data:
            data[lk] = resolve_links(data[lk], lang)

    return data


def localize_model(
    model: BaseModel,
    lang: str,
    *,
    text_fields: Tuple[str, ...] = ("title", "description"),
    link_keys: Tuple[str, ...] = ("links",),
) -> Dict[str, Any]:
    """Serialize *model* and resolve all localised fields for *lang*.

    Equivalent to::

        localize_response_dict(
            model.model_dump(by_alias=True, exclude_none=True),
            lang,
            text_fields=text_fields,
            link_keys=link_keys,
        )

    Returns a plain ``dict`` ready for ``JSONResponse(content=...)``.
    """
    data = model.model_dump(by_alias=True, exclude_none=True)
    return localize_response_dict(data, lang, text_fields=text_fields, link_keys=link_keys)
