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

"""Preset: anonymous read access to OGC conformance and landing-page endpoints.

OGC API Common Part 1 §7.4 requires ``/conformance`` to be accessible
without credentials; automated conformance suites (TEAM Engine / ETS) probe
it unauthenticated before any other call.  This preset applies a single
GET-only policy that covers:

* ``/{svc}/conformance`` — every loaded OGC service's conformance endpoint.
* ``/{svc}/`` and ``/{svc}`` — the corresponding landing pages.
* ``/conformance`` — a platform-level conformance aggregator if one is
  ever registered.

The resource patterns use non-capturing regex so the policy is
self-maintaining: new OGC services coming online inherit the anonymous
conformance grant without any manual edit.

The policy is GET-only.  Data surfaces (items, collections, coverage data,
…) are untouched.

Registered as the ``ogc_conformance_public`` preset.  Applied at boot by
``extensions/web/web.py`` when ``ConformanceExposureConfig.public`` is
``True`` (the default).
"""

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset

# Policy id is stable — the revoke descriptor stores it and uses it to
# delete exactly what apply wrote.
_POLICY_ID = "ogc_conformance_public_access"


def _conformance_policies() -> list:
    """Pure data: the anonymous-read policy for conformance + landing routes.

    Resource patterns (all anchored at start by ``re.match``):

    * ``/conformance$`` — platform-level aggregator (safe no-op if absent).
    * ``^/[^/]+/conformance$`` — any ``/{svc}/conformance``.
    * ``^/[^/]+/?$`` — any ``/{svc}/`` or ``/{svc}`` landing page.

    The third pattern is intentionally narrow: a single path segment with an
    optional trailing slash only.  It does NOT match ``/{svc}/collections/…``
    or any data path.
    """
    return [
        Policy(
            id=_POLICY_ID,
            description=(
                "Allows anonymous GET access to OGC API conformance declarations "
                "(/{svc}/conformance) and service landing pages (/{svc}/, /{svc}). "
                "OGC API Common Part 1 requires these to be publicly readable so "
                "automated conformance suites (ETS/TEAM Engine) can run without "
                "credentials. Data surfaces are unaffected."
            ),
            actions=["GET", "OPTIONS"],
            resources=[
                "/conformance$",
                "^/[^/]+/conformance$",
                "^/[^/]+/?$",
            ],
            effect="ALLOW",
        ),
    ]


def _conformance_role_bindings() -> list:
    return [
        Role(
            name=IamRolesConfig().anonymous_role_name,
            policies=[_POLICY_ID],
        ),
    ]


class _ConformancePublicContributor:
    def get_policies(self) -> list:
        return _conformance_policies()

    def get_role_bindings(self) -> list:
        return _conformance_role_bindings()


register_preset(PolicyContributorPreset(
    name="ogc_conformance_public",
    description=(
        "Anonymous GET access to OGC conformance endpoints and service landing "
        "pages.  Default-public; can be skipped by setting "
        "ConformanceExposureConfig.public=False."
    ),
    keywords=("iam", "ogc", "conformance", "platform"),
    contributor_factory=_ConformancePublicContributor,
))
