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

"""Core protocol + value objects for hierarchy providers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, Protocol, runtime_checkable


@dataclass
class HierarchyNode:
    """A single node in a hierarchy — uniform shape across all provider kinds.

    Mirrors ogc-dimensions' `GeneratedMember` so dimension-backed is a
    pass-through, while also carrying enough for the STAC virtual-Collection
    renderer (labels, level, has_children).
    """

    code: str
    label: str
    parent_code: Optional[str] = None
    level: Optional[int] = None
    has_children: bool = False
    labels: Dict[str, str] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChildrenPage:
    members: List[HierarchyNode]
    number_matched: int
    number_returned: int
    limit: int
    offset: int


@dataclass
class HierarchyExtent:
    """Spatial/temporal extent aggregated over a node's descendants."""

    spatial_bbox: Optional[List[float]] = None
    temporal_interval: Optional[List[Optional[str]]] = None


@runtime_checkable
class HierarchyProvider(Protocol):
    """Uniform interface over any source of hierarchy."""

    kind: ClassVar[str]

    async def roots(
        self,
        ctx: Any,
        *,
        limit: int = 100,
        offset: int = 0,
        language: Optional[str] = None,
    ) -> ChildrenPage: ...

    async def children(
        self,
        ctx: Any,
        parent_code: str,
        *,
        limit: int = 100,
        offset: int = 0,
        language: Optional[str] = None,
    ) -> ChildrenPage: ...

    async def ancestors(
        self,
        ctx: Any,
        member_code: str,
        *,
        language: Optional[str] = None,
    ) -> List[HierarchyNode]: ...

    async def extent(
        self,
        ctx: Any,
        parent_code: Optional[str],
    ) -> HierarchyExtent: ...

    async def has_children(self, ctx: Any, member_code: str) -> bool: ...
