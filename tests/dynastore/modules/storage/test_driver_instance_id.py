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

from __future__ import annotations


def test_driver_instance_id_is_stable_across_calls():
    from dynastore.modules.storage.driver_instance_id import compute_driver_instance_id
    a = compute_driver_instance_id("items_elasticsearch_driver", "cat1", "col1")
    b = compute_driver_instance_id("items_elasticsearch_driver", "cat1", "col1")
    assert a == b


def test_driver_instance_id_disambiguates_per_collection():
    from dynastore.modules.storage.driver_instance_id import compute_driver_instance_id
    a = compute_driver_instance_id("items_elasticsearch_driver", "cat1", "col1")
    b = compute_driver_instance_id("items_elasticsearch_driver", "cat1", "col2")
    c = compute_driver_instance_id("items_elasticsearch_driver", "cat2", "col1")
    assert a != b and a != c and b != c


def test_driver_instance_id_format_is_uuidv5():
    from uuid import UUID
    from dynastore.modules.storage.driver_instance_id import compute_driver_instance_id
    s = compute_driver_instance_id("d", "c", "cc")
    parsed = UUID(s)
    assert parsed.version == 5


def test_driver_instance_id_namespace_is_pinned():
    """Snapshot test for the namespace literal — would catch any future
    edit that changes the UUIDv5 namespace and breaks ID stability
    across the deployed fleet."""
    from dynastore.modules.storage.driver_instance_id import compute_driver_instance_id
    # Known input -> known UUID under namespace 4f5b8c12-7a3e-4f1a-9b2d-3a6c8d1e7f04
    # Re-derive the expected value once if you ever (legitimately) rotate the
    # namespace; otherwise it must remain constant.
    assert compute_driver_instance_id("d", "c", "cc") == "9b9b9ce5-6793-5b46-83db-2f2bb260532e"
