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

# tests/dynastore/tasks/unit/test_mandatory_attrs.py
"""Mandatory/affinity class attrs + derived kind helper.

These are pure-static (no app boot). They pin the contract the
mandatory-ownership guarantee and the Processes invocation guard depend on:
cascade_cleanup is mandatory + catalog-affine, and a task's kind is derived
from whether it ships a Process definition.
"""
from __future__ import annotations

from dynastore.tasks import TaskConfig, task_kind
from dynastore.tasks.cascade_cleanup.task import CascadeCleanupTask
from dynastore.tasks.protocols import TaskProtocol


def test_taskprotocol_defaults_are_non_mandatory():
    assert TaskProtocol.mandatory is False
    assert TaskProtocol.affinity_tier is None


def test_cascade_cleanup_is_mandatory_catalog_affine():
    assert CascadeCleanupTask.mandatory is True
    assert CascadeCleanupTask.affinity_tier == "catalog"


def test_task_kind_derives_from_definition_presence():
    process_cfg = TaskConfig(cls=object, module_name="m", name="p", definition=object())
    system_cfg = TaskConfig(cls=object, module_name="m", name="t", definition=None)
    assert task_kind(process_cfg) == "process"
    assert task_kind(system_cfg) == "task"
