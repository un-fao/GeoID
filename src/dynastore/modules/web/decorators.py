from typing import List, Callable, Any, Union, Dict
from dynastore.models.localization import _LANGUAGE_METADATA


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
):
    """
    Decorator to register a method as a Web Page provider.
    The method should return the HTML content for the page.

    Args:
        page_id (str): Unique identifier for the page (used in URL fragment).
        title (str | dict): Display title for navigation. Can be a string or a dict of {lang: title}.
        icon (str): FontAwesome icon class (e.g., "fa-layer-group").
        description (str | dict): Short description of the page functionality. Can be a string or a dict of {lang: desc}.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            func,
            "_web_page_config",
            {"id": page_id, "title": title, "icon": icon, "description": description},
        )
        return func

    return decorator
