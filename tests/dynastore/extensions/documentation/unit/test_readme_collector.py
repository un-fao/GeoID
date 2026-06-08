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

from pathlib import Path

from dynastore.extensions.documentation.readme_collector import (
    collect_readmes, ReadmeEntry,
)


def test_skips_modules_without_readme(tmp_path):
    pkg = tmp_path / "mod_a"; pkg.mkdir()
    (pkg / "__init__.py").touch()
    entries = collect_readmes(roots=[tmp_path], installed_module_ids={"mod_a"}, priority_by_id={"mod_a": 10})
    assert entries == []


def test_reads_readme_when_present_and_installed(tmp_path):
    pkg = tmp_path / "mod_a"; pkg.mkdir()
    (pkg / "README.md").write_text("# Mod A\n\nHello.")
    entries = collect_readmes(roots=[tmp_path], installed_module_ids={"mod_a"}, priority_by_id={"mod_a": 10})
    assert len(entries) == 1
    assert entries[0].module_id == "mod_a"
    assert entries[0].content.startswith("# Mod A")
    assert entries[0].priority == 10


def test_skips_modules_not_installed(tmp_path):
    pkg = tmp_path / "mod_a"; pkg.mkdir()
    (pkg / "README.md").write_text("content")
    entries = collect_readmes(roots=[tmp_path], installed_module_ids=set(), priority_by_id={})
    assert entries == []


def test_orders_by_priority_first_loaded_first_listed(tmp_path):
    for name, prio in [("b", 20), ("a", 10), ("c", 30)]:
        pkg = tmp_path / name; pkg.mkdir()
        (pkg / "README.md").write_text(f"# {name}")
    entries = collect_readmes(
        roots=[tmp_path],
        installed_module_ids={"a", "b", "c"},
        priority_by_id={"a": 10, "b": 20, "c": 30},
    )
    assert [e.module_id for e in entries] == ["a", "b", "c"]  # ascending priority
