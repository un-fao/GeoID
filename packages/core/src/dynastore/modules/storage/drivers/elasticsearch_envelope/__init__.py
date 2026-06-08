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
Standardized-envelope Elasticsearch storage driver subpackage — items-tier only.

This driver writes a canonical document envelope carrying typed identity and
access fields (``visibility`` / ``owner``) and ANDs a
row-level access filter into every search by translating a neutral
``AccessFilter`` into ES Query DSL.

The subpackage is self-contained so it can be opted in or out via SCOPE without
touching the platform layer or the existing ES drivers.
"""

from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
    ItemsElasticsearchEnvelopeDriver,
)

__all__ = [
    "ItemsElasticsearchEnvelopeDriver",
]
