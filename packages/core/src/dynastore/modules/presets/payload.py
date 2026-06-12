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

"""Preset payload DTOs — multi-purpose envelope for single presets and ordered chains.

This module defines the wire shape used by three distinct callers:

* File seeds — ``${DYNASTORE_CONFIG_ROOT}/presets/<name>.json`` may contain a
  single payload, a chain, or a bare list.
* Admin API request bodies — the REST handler accepts the same shapes.
* Bootstrap chains — ``bootstrap_presets`` coerces any accepted shape to an
  ordered list and applies each entry sequentially.

The outer envelope (``PresetPayload`` / ``PresetChainPayload``) enforces
``extra="forbid"`` so typo'd keys fail loudly.  The ``params`` field itself is
open (``Dict[str, Any]``) because each preset validates its own params via
``validate_preset_params``; centralising that validation here would force a
dependency on the storage / IAM layer, which this module must avoid.

Import rule: stdlib + pydantic only — NO imports from modules/iam,
modules/storage, or modules/db_config.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field, ValidationError


# ---------------------------------------------------------------------------
# Envelope models
# ---------------------------------------------------------------------------

class PresetPayload(BaseModel):
    """Single-preset invocation envelope.

    ``params`` is free-form JSON; it is validated against the target preset's
    own ``params_model`` by ``validate_preset_params``, never here.
    The outer envelope uses ``extra="forbid"`` so typo'd top-level keys
    raise ``ValidationError`` immediately.
    """

    model_config = {"extra": "forbid"}  # type: ignore[misc]

    preset_name: str
    scope_key: str = "platform"
    force: bool = False
    params: Dict[str, Any] = Field(default_factory=dict)


class PresetChainPayload(BaseModel):
    """Ordered sequence of preset invocations.

    ``presets`` is applied sequentially; ordering encodes intent.
    Minimum length is 1 so an empty chain is rejected eagerly.
    """

    model_config = {"extra": "forbid"}  # type: ignore[misc]

    presets: List[PresetPayload] = Field(min_length=1)


# ---------------------------------------------------------------------------
# Coercion helper
# ---------------------------------------------------------------------------

def coerce_payloads(obj: Any) -> List[PresetPayload]:
    """Normalise any accepted input shape to an ordered list of ``PresetPayload``.

    Accepted shapes:

    * A ``PresetPayload`` instance — returned as a 1-element list.
    * A ``PresetChainPayload`` instance — returns ``presets`` list.
    * A ``dict`` that contains a ``"presets"`` key — parsed as
      ``PresetChainPayload``; returns the inner list.
    * A ``dict`` without ``"presets"`` — parsed as a single
      ``PresetPayload``; returned as a 1-element list.
    * A ``list`` of ``dict`` / ``PresetPayload`` items — each element is
      coerced to ``PresetPayload`` and the resulting list is returned.

    Raises ``ValueError`` with a descriptive message for anything else,
    including dicts that fail Pydantic validation (the ``ValidationError``
    message is embedded in the ``ValueError`` so callers receive one
    exception type).
    """
    if isinstance(obj, PresetPayload):
        return [obj]

    if isinstance(obj, PresetChainPayload):
        return list(obj.presets)

    if isinstance(obj, dict):
        if "presets" in obj:
            try:
                chain = PresetChainPayload.model_validate(obj)
            except ValidationError as exc:
                raise ValueError(f"Invalid preset chain payload: {exc}") from exc
            return list(chain.presets)
        try:
            single = PresetPayload.model_validate(obj)
        except ValidationError as exc:
            raise ValueError(f"Invalid preset payload: {exc}") from exc
        return [single]

    if isinstance(obj, list):
        result: List[PresetPayload] = []
        for idx, item in enumerate(obj):
            if isinstance(item, PresetPayload):
                result.append(item)
            elif isinstance(item, dict):
                try:
                    result.append(PresetPayload.model_validate(item))
                except ValidationError as exc:
                    raise ValueError(
                        f"Invalid preset payload at index {idx}: {exc}"
                    ) from exc
            else:
                raise ValueError(
                    f"Expected dict or PresetPayload at index {idx}, "
                    f"got {type(item).__name__!r}"
                )
        return result

    raise ValueError(
        f"Cannot coerce {type(obj).__name__!r} to a list of PresetPayload. "
        "Expected a PresetPayload, PresetChainPayload, dict, or list."
    )


# ---------------------------------------------------------------------------
# Per-preset param validation
# ---------------------------------------------------------------------------

def validate_preset_params(preset: Any, payload: PresetPayload) -> BaseModel:
    """Validate ``payload.params`` against the preset's own ``params_model``.

    Delegates entirely to ``preset.params_model.model_validate(payload.params)``
    so Pydantic performs the authoritative type-check.  ``ValidationError``
    propagates to the caller — the caller decides whether to abort boot or log
    and continue.

    ``preset`` must satisfy ``PresetProtocol`` (i.e. expose a ``params_model``
    class variable that is a Pydantic ``BaseModel`` subclass).
    """
    return preset.params_model.model_validate(payload.params)
