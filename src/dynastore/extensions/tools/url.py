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

from fastapi import Request
from urllib.parse import urlparse, urljoin
import os

FORCE_HTTPS: bool = False
if os.getenv("FORCE_HTTPS", "false").lower() in ("true", "1", "yes"):
    FORCE_HTTPS = True

def get_parent_url(request: Request, levels_up = 0) -> str:
    """Constructs the base URL from the request for STAC link generation."""
    return get_parent_from_url(str(request.url), levels_up=levels_up)

def get_parent_from_url(current_url, levels_up = 0):
    """
    Finds the URL n levels up from a given URL.
    """
    parsed_url = urlparse(current_url.rstrip('/'))
    path_components = parsed_url.path.split('/')[:-1]
    parent_path = '/'.join(path_components)

    relative_path = '../' * levels_up
    
    scheme = parsed_url.scheme
    if FORCE_HTTPS:
        scheme = "https"
    base_url = f"{scheme}://{parsed_url.netloc}{parent_path}/"
    
    new_url = urljoin(base_url, relative_path)
    
    return new_url.rstrip('/')

def get_root_url(request: Request) -> str:
    """Constructs the base URL from the request for STAC link generation."""
    url = request.base_url
    scheme = url.scheme
    if FORCE_HTTPS:
        scheme = "https"
    return f"{scheme}://{url.netloc}{request.scope.get('root_path', '')}".rstrip('/')

def get_base_url(request: Request) -> str:
    """Constructs the base URL from the request for STAC link generation."""
    url = request.base_url
    scheme = url.scheme
    if FORCE_HTTPS:
        scheme = "https"
    return f"{scheme}://{url.netloc}{request.scope.get('path', '')}".rstrip('/')

def get_url(request: Request, remove_qp=True) -> str:
    """Constructs the full URL from the request for link generation."""
    url = request.url
    if remove_qp:
        # FIX: Pass the keys of the current query parameters to be removed.
        url = url.remove_query_params(keys=request.query_params.keys())
    
    scheme = url.scheme
    if FORCE_HTTPS:
        scheme = "https"
    return f"{scheme}://{url.netloc}{url.path.rstrip('/')}"

