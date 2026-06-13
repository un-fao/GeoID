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
