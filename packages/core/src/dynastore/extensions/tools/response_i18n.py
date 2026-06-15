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

"""Helpers for resolving LocalizedText fields in OGC response objects.

These functions operate on already-serialized dicts (``model_dump`` output) or
on Pydantic model instances via ``model_construct``, never through a normal
constructor.  The ``Link.title`` before-validator re-wraps any plain ``str``
to ``{"en": "..."}``; bypassing that validator is required to put a plain
string on the wire when ``lang != '*'``.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def resolve_localized(
    value: Any,
    language: str,
) -> Any:
    """Resolve a localized value (``LocalizedText`` instance or ``dict``) to a
    single-language scalar.

    - ``language == '*'``: returns the full ``{"lang": "value", ...}`` dict.
    - Otherwise: returns the best-match string, falling back to ``"en"``, then
      the first available language.
    - If ``value`` is already a plain ``str`` / non-dict / ``None``, it is
      returned unchanged.
    """
    if value is None:
        return None

    # Already a plain string (field was never multi-language).
    if isinstance(value, str):
        return value

    # Support both LocalizedText pydantic instances and raw dicts.
    from pydantic import BaseModel

    if isinstance(value, BaseModel):
        data: Dict[str, Any] = {k: v for k, v in value.model_dump().items() if v is not None}
    elif isinstance(value, dict):
        data = {k: v for k, v in value.items() if v is not None}
    else:
        return value

    if not data:
        return None

    if language == "*":
        return data

    return (
        data.get(language)
        or data.get(language.split("-")[0])
        or data.get("en")
        or next(iter(data.values()), None)
    )


def resolve_links(
    links: Optional[List[Any]],
    language: str,
) -> Optional[List[Any]]:
    """Resolve ``title`` on each ``Link`` (model or dict) in *links*.

    Returns a new list where every element whose ``title`` is a
    ``LocalizedText`` (or a ``{"lang": "..."}`` dict) has been replaced with a
    ``Link`` instance built via ``model_construct`` — bypassing the
    ``before`` validator that would otherwise re-wrap a resolved plain string
    back into ``{"en": "..."}``.

    Elements that are already plain dicts (no ``model_construct``) are
    processed in-place as shallow copies.
    """
    if not links:
        return links

    from dynastore.models.shared_models import Link

    resolved: List[Any] = []
    for link in links:
        if link is None:
            resolved.append(link)
            continue

        # --- dict branch (pre-dumped links) ---
        if isinstance(link, dict):
            title = link.get("title")
            if title is None:
                resolved.append(link)
                continue
            new_title = resolve_localized(title, language)
            if new_title is title:
                resolved.append(link)
            else:
                copy = dict(link)
                if new_title is None:
                    copy.pop("title", None)
                else:
                    copy["title"] = new_title
                resolved.append(copy)
            continue

        # --- Pydantic Link branch ---
        title = getattr(link, "title", None)
        if title is None:
            resolved.append(link)
            continue

        new_title = resolve_localized(title, language)
        # Dump all fields, replace title with the resolved value, then
        # rebuild via model_construct to bypass the before-validator.
        d = link.model_dump(exclude_none=True)
        if new_title is None:
            d.pop("title", None)
        else:
            d["title"] = new_title
        resolved.append(Link.model_construct(**d))

    return resolved


def localize_model(
    model_dict: Dict[str, Any],
    language: str,
) -> Dict[str, Any]:
    """Resolve all ``links[].title`` entries in a serialized model dict.

    Operates on the dict returned by ``model.model_dump(...)``.  Only the
    ``links`` key is processed; all other members are left untouched so no
    foreign members are added or dropped.
    """
    raw_links = model_dict.get("links")
    if not raw_links:
        return model_dict

    resolved = resolve_links(raw_links, language)
    if resolved is raw_links:
        return model_dict

    # Return a shallow copy with the resolved links so the original dict is
    # not mutated (callers may hold references to it).
    result = dict(model_dict)
    result["links"] = resolved
    return result
