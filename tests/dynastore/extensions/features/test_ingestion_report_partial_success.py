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

"""Unit tests for :class:`IngestionReport` and :class:`SidecarRejection`."""

import pytest

from dynastore.extensions.features.ogc_models import (
    IngestionReport,
    SidecarRejection,
)
from dynastore.modules.storage.errors import SidecarRejectedError


class TestSidecarRejection:
    def test_required_fields_only(self):
        r = SidecarRejection(reason="duplicate_external_id", message="dup")
        assert r.reason == "duplicate_external_id"
        assert r.message == "dup"
        assert r.geoid is None
        assert r.external_id is None
        assert r.sidecar_id is None
        assert r.matcher is None
        assert r.policy_source is None

    def test_populated_fields_preserve_policy_pointer(self):
        pointer = (
            "/configs/catalogs/demo/collections/c/plugins/"
            "items_write_policy"
        )
        r = SidecarRejection(
            reason="duplicate_external_id",
            message="dup",
            external_id="ext-1",
            sidecar_id="write_policy",
            matcher="external_id",
            policy_source=pointer,
        )
        assert r.external_id == "ext-1"
        assert r.matcher == "external_id"
        assert r.policy_source == pointer


class TestIngestionReport:
    def test_all_accepted_is_neither_partial_nor_full_reject(self):
        rep = IngestionReport(accepted_ids=["a", "b"], total=2)
        assert rep.is_partial is False
        assert rep.is_fully_rejected is False

    def test_partial_success_flag(self):
        rep = IngestionReport(
            accepted_ids=["a"],
            rejections=[
                SidecarRejection(reason="duplicate_external_id", message="dup")
            ],
            total=2,
        )
        assert rep.is_partial is True
        assert rep.is_fully_rejected is False

    def test_fully_rejected_flag(self):
        rep = IngestionReport(
            accepted_ids=[],
            rejections=[
                SidecarRejection(reason="duplicate_external_id", message="dup"),
                SidecarRejection(reason="duplicate_external_id", message="dup"),
            ],
            total=2,
        )
        assert rep.is_partial is False
        assert rep.is_fully_rejected is True

    def test_empty_batch(self):
        rep = IngestionReport(accepted_ids=[], rejections=[], total=0)
        assert rep.is_partial is False
        assert rep.is_fully_rejected is False

    def test_round_trip_serialisation(self):
        rep = IngestionReport(
            accepted_ids=["g1"],
            rejections=[
                SidecarRejection(
                    reason="duplicate_external_id",
                    message="dup",
                    external_id="x1",
                    matcher="external_id",
                )
            ],
            total=2,
        )
        dumped = rep.model_dump(by_alias=True)
        revived = IngestionReport.model_validate(dumped)
        assert revived.accepted_ids == ["g1"]
        assert revived.rejections[0].external_id == "x1"
        assert revived.rejections[0].matcher == "external_id"


class TestSidecarRejectedErrorShape:
    """Tests that `SidecarRejectedError` carries the structured fields the
    batch aggregator needs to build `IngestionReport` entries."""

    def test_defaults_and_reason(self):
        err = SidecarRejectedError("dup")
        assert err.reason == "sidecar_rejected"
        assert err.geoid is None
        assert err.external_id is None
        assert err.sidecar_id is None
        assert err.matcher is None

    def test_full_payload(self):
        err = SidecarRejectedError(
            "duplicate",
            geoid="g1",
            external_id="ext-1",
            sidecar_id="write_policy",
            matcher="external_id",
            reason="duplicate_external_id",
        )
        assert err.geoid == "g1"
        assert err.external_id == "ext-1"
        assert err.sidecar_id == "write_policy"
        assert err.matcher == "external_id"
        assert err.reason == "duplicate_external_id"
        assert str(err) == "duplicate"
