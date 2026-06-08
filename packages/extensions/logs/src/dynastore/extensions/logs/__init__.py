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

"""
Thin wrapper that re-exports the catalog module's log_manager.
This maintains backward compatibility for code that imports from extensions/logs.
The actual implementation lives in modules/catalog/log_manager.py to respect
the Three Pillars architecture (modules don't import extensions).
"""
from dynastore.modules.catalog.log_manager import (
    log_event,
    log_info,
    log_warning,
    log_error,
    LogEntryCreate,
    LOG_SERVICE
)

__all__ = ['log_event', 'log_info', 'log_warning', 'log_error', 'LogEntryCreate', 'LOG_SERVICE']

from . import config  # noqa: F401  -- service-exposure plugin registration
from . import presets as _logs_presets  # noqa: F401  -- preset registration side-effect
