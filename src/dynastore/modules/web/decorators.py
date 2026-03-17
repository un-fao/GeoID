from typing import List, Callable, Any, Union, Dict, Optional


def expose_static(virtual_path: str):
    """
    Decorator to mark a method as a provider of static files.
    The decorated method must return a list of absolute file paths
    that are allowed to be served under the given virtual_path.

    Args:
        virtual_path (str): The URL sub-path under /web/ where files will be exposed.
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
    """
    Decorator to register a method as a Web Page provider.
    The method should return the HTML content for the page.

    Args:
        page_id (str): Unique identifier for the page (used in URL fragment).
        title (str | dict): Display title for navigation. Can be a string or a dict of {lang: title}.
        icon (str): FontAwesome icon class (e.g., "fa-layer-group").
        description (str | dict): Short description of the page functionality. Can be a string or a dict of {lang: desc}.
        required_roles (list): Optional roles allowed to access this page.
        priority (int): Sorting priority (lower is earlier).
        section (str | dict): Optional section for sidebar grouping.
        is_embed (bool): If True, content is embedded into the parent.
        enabled (bool): If False, the page is registered but not shown in UI.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            func,
            "_web_page_config",
            {
                "id": page_id,
                "title": title,
                "icon": icon,
                "description": description,
                "required_roles": required_roles,
                "priority": priority,
                "section": section,
                "is_embed": is_embed,
                "enabled": enabled,
            },
        )
        return func

    return decorator


