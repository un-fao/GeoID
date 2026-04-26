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
