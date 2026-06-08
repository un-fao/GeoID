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

from unittest.mock import patch


def test_register_sidecar_bounds_source_calls_register_plugin():
    """Confirms both a BoundsSourceProtocol and a GeometryFetcherProtocol
    are registered against the platform's discovery layer.

    End-to-end DB test is deferred to integration fixtures; this unit
    test ensures the factory call succeeds without runtime errors.
    """
    import dynastore.extensions.volumes.platform_bounds_source as mod
    registered = []
    with patch.object(
        mod, "register_plugin", side_effect=lambda obj: registered.append(obj),
    ):
        mod.register_sidecar_bounds_source()

    assert len(registered) == 2

    from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
    from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol

    assert any(isinstance(r, BoundsSourceProtocol) for r in registered)
    assert any(isinstance(r, GeometryFetcherProtocol) for r in registered)
