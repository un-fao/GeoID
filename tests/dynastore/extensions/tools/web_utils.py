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

import os
import glob
import pytest
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import WebModuleProtocol

def register_static_data_provider(base_url, data_dir_path, provider_name="integration_data"):
    """
    Registers a static file provider with the Web extension for serving test data.
    
    Args:
        base_url (str): The base URL of the application (e.g., from 'base_url' fixture).
        data_dir_path (str): The relative or absolute path to the directory containing data files.
        provider_name (str): The unique name for the provider prefix. Defaults to "integration_data".
        
    Returns:
        str: The base URL for accessing the served files (e.g., "http://test/web/integration_data").
    """
    web_ext = get_protocol(WebModuleProtocol)
    if not web_ext:
        pytest.fail("Web extension not found/enabled. Ensure 'web' is in enable_extensions/enable_modules.")

    abs_data_dir = os.path.abspath(data_dir_path)
    
    if not os.path.isdir(abs_data_dir):
        pytest.fail(f"Data directory not found: {abs_data_dir}")

    # Register provider for the data directory
    # The provider must return a list of absolute file paths
    # We use a lambda that re-scans the directory to capture new files if needed
    web_ext.register_static_provider(
        provider_name, 
        lambda: glob.glob(os.path.join(abs_data_dir, "**", "*"), recursive=True)
    )
    
    return f"{base_url}/web/{provider_name}"
