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

"""Retry/backoff for transient PERMISSION_DENIED on ``create_subscription``.

Binding a Pub/Sub push subscription needs ``pubsub.topics.attachSubscription``
on the topic, which can transiently return 403 right after the topic and its
IAM policy are created (IAM eventual consistency). ``setup_push_subscription``
must retry with bounded exponential backoff so a propagation race self-heals
instead of propagating the error and dead-lettering the catalog provisioning
task — which would leave the catalog stuck ``provisioning`` and 409-reject every
write. Regression for a review-env catalog whose provisioning dead-lettered
after three quick retries on a transient attachSubscription 403.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dynastore.modules.gcp.gcp_eventing_ops as ops_mod
from dynastore.modules.gcp.gcp_eventing_ops import (
    GcpEventingOpsMixin,
    _SUBSCRIPTION_CREATE_MAX_ATTEMPTS,
)
from dynastore.modules.gcp.models import PushSubscriptionConfig


PROJECT = "proj-x"
TOPIC = "projects/proj-x/topics/ds-c-events"
SUB_PATH = "projects/proj-x/subscriptions/ds-c-events-sub"


class _StubMixin(GcpEventingOpsMixin):
    """Concrete fill-in for the protocol stubs so the mixin's methods can be
    called directly. GCP clients are MagicMocks injected per test."""

    def __init__(self):
        self._publisher = MagicMock()
        self._subscriber = MagicMock()
        self._storage = MagicMock()
        self._bucket_service = MagicMock()

    def get_project_id(self):
        return PROJECT

    def get_region(self):
        return "europe-west1"

    def get_account_email(self):
        return "svc@proj-x.iam"

    async def get_self_url(self):
        return "https://catalog.example/api/catalog"

    def get_publisher_client(self):
        return self._publisher

    def get_subscriber_client(self):
        return self._subscriber

    def get_storage_client(self):
        return self._storage

    def get_bucket_service(self):
        return self._bucket_service


@pytest.fixture
def mixin(monkeypatch):
    m = _StubMixin()
    m._subscriber.subscription_path.return_value = SUB_PATH

    # run_in_thread is the awaitable wrapper around blocking GCP calls; make it
    # an immediate sync caller so the tests stay synchronous.
    async def _run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    monkeypatch.setattr(ops_mod, "run_in_thread", _run_in_thread)

    # No real backoff sleeps in tests.
    async def _no_sleep(_delay):
        return None

    monkeypatch.setattr(ops_mod.asyncio, "sleep", _no_sleep)
    return m


@pytest.mark.asyncio
async def test_create_subscription_retries_transient_permission_denied(mixin):
    """Transient 403 then success → the method completes and create is retried,
    rather than the error bubbling up and dead-lettering provisioning."""
    from google.api_core import exceptions as ge

    calls = {"n": 0}

    def _flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ge.PermissionDenied("attachSubscription IAM not yet propagated")
        return MagicMock()

    mixin._subscriber.create_subscription.side_effect = _flaky

    sub_cfg = PushSubscriptionConfig(subscription_id="ds-c-events-sub", push_endpoint=None)
    result = await mixin.setup_push_subscription(TOPIC, sub_cfg, custom_attributes={})

    assert calls["n"] == 3  # two transient 403s, then success
    assert result.subscription_path == SUB_PATH
    # The AlreadyExists recovery path must NOT run on the success path.
    mixin._subscriber.get_subscription.assert_not_called()
    mixin._subscriber.modify_push_config.assert_not_called()


@pytest.mark.asyncio
async def test_create_subscription_raises_after_exhausting_retries(mixin):
    """A persistent 403 (genuine permission gap) still fails — after the bounded
    attempt budget, not infinitely, and not silently."""
    from google.api_core import exceptions as ge

    mixin._subscriber.create_subscription.side_effect = ge.PermissionDenied("denied")

    sub_cfg = PushSubscriptionConfig(subscription_id="ds-c-events-sub", push_endpoint=None)
    with pytest.raises(ge.PermissionDenied):
        await mixin.setup_push_subscription(TOPIC, sub_cfg, custom_attributes={})

    assert (
        mixin._subscriber.create_subscription.call_count
        == _SUBSCRIPTION_CREATE_MAX_ATTEMPTS
    )


@pytest.mark.asyncio
async def test_create_subscription_already_exists_not_retried(mixin):
    """``AlreadyExists`` is terminal-handled (clash check / push-config refresh),
    NOT swallowed by the transient-retry loop: create is called exactly once."""
    from google.api_core import exceptions as ge
    from types import SimpleNamespace

    mixin._subscriber.create_subscription.side_effect = ge.AlreadyExists("exists")
    # Bound to the expected topic → falls through to modify_push_config.
    mixin._subscriber.get_subscription.return_value = SimpleNamespace(topic=TOPIC)

    sub_cfg = PushSubscriptionConfig(subscription_id="ds-c-events-sub", push_endpoint=None)
    await mixin.setup_push_subscription(TOPIC, sub_cfg, custom_attributes={})

    assert mixin._subscriber.create_subscription.call_count == 1
    mixin._subscriber.modify_push_config.assert_called_once()
