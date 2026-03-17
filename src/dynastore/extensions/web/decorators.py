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

from typing import List, Callable, Any, Optional
import logging

logger = logging.getLogger(__name__)


def expose_static(virtual_path: str):
    """
    Decorator to mark a method as a provider of static files.

    The decorated method must return a ``List[str]`` of **absolute** file paths
    that are allowed to be served under the given virtual_path.
    Files will be accessible at ``/web/extension-static/{virtual_path}/<filename>``.

    On decoration the method is also registered immediately on the
    ``WebModuleProtocol`` implementer (if already available), so that
    extensions loaded after the Web extension do not need the scanner pass.

    Args:
        virtual_path: The URL sub-path under /web/extension-static/ where
                      files will be served.
    """
    def decorator(func: Callable[..., List[str]]) -> Callable[..., List[str]]:
        func._web_static_prefix = virtual_path
        return func
    return decorator


def expose_web_page(
    page_id: str,
    title: "str | dict[str, str]",
    icon: str = "fa-circle",
    description: "str | dict[str, str]" = "",
    required_roles: Optional[List[str]] = None,
    priority: int = 0,
    section: Optional["str | dict[str, str]"] = None,
    is_embed: bool = False,
    enabled: bool = True,
):
    """
    Decorator to register a method as a Web Page provider.

    The decorated method should return the HTML content (or a Response) for
    the page.  The page is registered on the ``WebModuleProtocol`` implementer
    immediately when the method is called via ``register_web_page`` at
    extension ``configure_appSyncTask`` time.

    Args:
        page_id:        Unique identifier (used in the ``/web/pages/{page_id}`` URL).
        title:          Navigation title. String or ``{lang: title}`` dict.
        icon:           FontAwesome icon class (e.g. ``"fa-layer-group"``).
        description:    Short page description. String or ``{lang: desc}`` dict.
        required_roles: Optional list of roles allowed to see/access this page.
        priority:       Sorting priority for navigation (lower is earlier).
        section:        Optional section name. String or ``{lang: section}`` dict.
        is_embed:       If True, the content will be embedded into the parent page/section.
        enabled:        If False, the page is registered but not shown in UI.

    Usage in an extension::

        from dynastore.extensions.web.decorators import expose_web_page

        class MyExtension(ExtensionProtocol):

            @expose_web_page(page_id="my_page", title="My Page", icon="fa-star", required_roles=["admin"], priority=10)
            def provide_my_page(self, request):
                return HTMLResponse("<h1>Hello</h1>")
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func._web_page_config = {
            "id": page_id,
            "title": title,
            "icon": icon,
            "description": description,
            "required_roles": required_roles,
            "priority": priority,
            "section": section,
            "is_embed": is_embed,
            "enabled": enabled,
        }
        return func
    return decorator



