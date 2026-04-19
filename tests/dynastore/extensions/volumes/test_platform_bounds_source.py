from unittest.mock import patch


def test_register_sidecar_bounds_source_calls_register_plugin():
    """Just confirms the wiring hooks into the platform's discovery layer.

    End-to-end DB test is deferred to integration fixtures; this unit
    test ensures the factory call succeeds without runtime errors when
    DatabaseProtocol/CatalogsProtocol are monkey-patched.
    """
    import dynastore.extensions.volumes.platform_bounds_source as mod
    registered = []
    with patch.object(
        mod, "register_plugin", side_effect=lambda obj: registered.append(obj),
    ):
        mod.register_sidecar_bounds_source()
    assert len(registered) == 1
    # Registered object satisfies the protocol structurally.
    from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
    assert isinstance(registered[0], BoundsSourceProtocol)
