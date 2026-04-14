#    Copyright 2025 FAO
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

"""Convenience decorators for web-page / static-asset contribution.

The decorators attach pure metadata to the function.  A contributing
class uses `collect_web_pages(self)` / `collect_static_assets(self)` from
`dynastore.extensions.tools.web_collect` to materialise bound-method
`WebPageSpec` / `StaticAsset` instances in its own
`get_web_pages()` / `get_static_assets()` implementation.

No reflective class-walking happens outside the contributing class.
"""

from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


def expose_static(virtual_path: str):
    """Mark a method as a static-files provider.

    The decorated method must return a ``List[str]`` of absolute file
    paths that are allowed to be served under the virtual prefix.
    """

    def decorator(func: Callable[..., List[str]]) -> Callable[..., List[str]]:
        setattr(func, "_web_static_prefix", virtual_path)
        return func

    return decorator


def expose_web_page(
    page_id: str,
    title: Union[str, Dict[str, str]],
    icon: str = "fa-circle",
    description: Union[str, Dict[str, str]] = "",
    required_roles: Optional[List[str]] = None,
    priority: int = 0,
    section: Optional[Union[str, Dict[str, str]]] = None,
    is_embed: bool = False,
    enabled: bool = True,
):
    """Mark a method as a web-page provider (metadata only)."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            func,
            "_web_page_spec",
            {
                "page_id": page_id,
                "title": title,
                "icon": icon,
                "description": description,
                "required_roles": (
                    tuple(required_roles) if required_roles else None
                ),
                "priority": priority,
                "section": section,
                "is_embed": is_embed,
                "enabled": enabled,
            },
        )
        return func

    return decorator
