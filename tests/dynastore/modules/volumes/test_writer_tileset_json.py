import json

from dynastore.modules.volumes.writers.tileset_json import write_tileset_json


def test_writer_emits_valid_json_bytes():
    ts = {"asset": {"version": "1.0"}, "root": {"geometricError": 1.0}}
    buf = b"".join(write_tileset_json(ts))
    doc = json.loads(buf.decode())
    assert doc["asset"]["version"] == "1.0"


def test_writer_strips_feature_ids_by_default():
    ts = {
        "root": {
            "geometricError": 1.0,
            "_feature_ids": ["a", "b"],
            "children": [{"_feature_ids": ["c"], "geometricError": 0.5}],
        }
    }
    doc = json.loads(b"".join(write_tileset_json(ts)).decode())
    assert "_feature_ids" not in doc["root"]
    assert "_feature_ids" not in doc["root"]["children"][0]


def test_writer_preserves_feature_ids_when_asked():
    ts = {"root": {"_feature_ids": ["a"], "geometricError": 1.0}}
    doc = json.loads(
        b"".join(write_tileset_json(ts, strip_feature_ids=False)).decode(),
    )
    assert doc["root"]["_feature_ids"] == ["a"]
