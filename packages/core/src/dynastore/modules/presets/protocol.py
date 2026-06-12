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

"""Neutral PresetProtocol — the minimal structural contract for any preset.

This protocol is intentionally lean: it captures only the four attributes
that the platform bootstrap machinery needs to interact with a preset. The
full ``Preset`` protocol (with ``dry_run``, ``keywords``, ``tier``, etc.)
lives in ``modules/storage/presets/preset.py`` and satisfies this protocol
structurally — no changes needed there.

IAM and other domain modules may declare presets that only satisfy
``PresetProtocol`` (simpler) or the full ``Preset`` (richer); the registry
and bootstrap layer require only ``PresetProtocol``.

Design note: ``scope_key`` is not a class variable on individual preset
classes — it is the runtime scope string passed as an argument to
``apply`` and ``revoke``. It appears here as a parameter name in the
method signatures, not as a class attribute.
"""
from __future__ import annotations

from typing import Any, ClassVar, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class PresetProtocol(Protocol):
    """Minimal protocol every platform preset must satisfy.

    Class variables carry static metadata used by the registry and admin
    surfaces. The three instance methods carry the actual preset logic.

    Import rule: this module MUST NOT import from ``modules/iam`` or from
    ``modules/storage``. It may import from ``models/`` or stdlib only.
    """

    name: ClassVar[str]
    """Stable, unique registry key. Snake-case; matches the admin URL slug."""

    params_model: ClassVar[type[BaseModel]]
    """Pydantic model for this preset's parameters. Use ``NoParams`` when the
    preset accepts no structured input."""

    async def apply(
        self,
        params: BaseModel,
        scope_key: str,
        ctx: Any,
    ) -> Any:
        """Apply this preset at ``scope_key`` using ``ctx``.

        Returns an ``AppliedDescriptor``-compatible object whose ``.payload``
        dict is stored in the audit row and later handed to ``revoke``.
        """
        ...

    async def revoke(
        self,
        applied_descriptor: Any,
        ctx: Any,
    ) -> None:
        """Undo exactly what ``apply`` introduced.

        ``applied_descriptor`` is the ``AppliedDescriptor`` produced by
        ``apply`` and retrieved from the audit row.
        """
        ...
