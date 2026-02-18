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

from typing import List, Callable, Any

def expose_static(virtual_path: str):
    """
    Decorator to mark a method as a provider of static files.
    The decorated method must return a list of absolute file paths 
    that are allowed to be served under the given virtual_path.
    
    Args:
        virtual_path (str): The URL sub-path under /web/ where files will be exposed.
    """
    def decorator(func: Callable[..., List[str]]) -> Callable[..., List[str]]:
        func._web_static_prefix = virtual_path
        return func
    return decorator

def expose_web_page(page_id: str, title: str | dict[str, str], icon: str = "fa-circle", description: str | dict[str, str] = ""):
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
        func._web_page_config = {
            "id": page_id,
            "title": title,
            "icon": icon,
            "description": description
        }
        return func
    return decorator
