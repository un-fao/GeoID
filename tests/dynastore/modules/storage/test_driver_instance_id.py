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
