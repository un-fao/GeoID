"""Regression tests locking in the configs API surface after the storm.

After the configs storm (PRs #140/#145/#150/#153/#156-#162) the public surface
shrunk significantly. These invariants prevent accidental re-introduction of
routes or tag groups that were intentionally retired.
"""
import pytest
from fastapi import FastAPI


@pytest.fixture(scope="module")
def service():
    from dynastore.extensions.configs.service import ConfigsService
    return ConfigsService(FastAPI())


def _route_paths(service) -> set[str]:
    """Collect all path templates registered on the configs router."""
    return {getattr(r, "path", "") for r in service.router.routes}


class TestRetiredRoutesNotRegistered:
    def test_examples_routes_absent(self, service):
        paths = _route_paths(service)
        for retired in ("/configs/examples", "/configs/examples/{plugin_id}"):
            assert retired not in paths, f"Retired route still registered: {retired}"

    def test_no_path_contains_examples_segment(self, service):
        for path in _route_paths(service):
            assert "/examples" not in path, f"Unexpected /examples segment in {path}"

    def test_bulk_and_search_routes_absent(self, service):
        for path in _route_paths(service):
            assert not path.endswith("/bulk"), f"Retired /bulk route still registered: {path}"
            assert not path.endswith("/search"), f"Retired /search route still registered: {path}"


class TestSingleConfigurationApiTag:
    def test_router_carries_only_configuration_api_tag(self, service):
        assert service.router.tags == ["Configuration API"], (
            f"Router tag drifted to {service.router.tags!r}; "
            "the configs extension publishes a single 'Configuration API' tag."
        )

    def test_every_route_carries_only_the_router_tag(self, service):
        """Every route must publish exactly ``['Configuration API']`` so the
        Swagger UI groups the whole extension together. FastAPI propagates the
        router-level tag onto each APIRoute at registration; a per-route
        override would extend (not replace) that list and split the group.
        """
        from fastapi.routing import APIRoute

        for route in service.router.routes:
            if not isinstance(route, APIRoute):
                continue
            assert route.tags == ["Configuration API"], (
                f"Route {route.path} has tags {route.tags!r}; "
                "expected ['Configuration API'] only — a per-route tag would split the Swagger group."
            )

    def test_retired_tag_groups_not_referenced_anywhere(self, service):
        """Defence-in-depth: walk every route and confirm none of the retired
        tag names ('Bulk', 'Examples', 'Config API', 'Configurations',
        'Configurations · Discovery') ever appear, including as router tags."""
        from fastapi.routing import APIRoute

        retired = {"Bulk", "Examples", "Config API", "Configurations", "Configurations · Discovery"}
        for tag in service.router.tags:
            assert tag not in retired, f"Router still carries retired tag {tag!r}"
        for route in service.router.routes:
            if not isinstance(route, APIRoute):
                continue
            for tag in (route.tags or []):
                assert tag not in retired, (
                    f"Route {route.path} carries retired tag {tag!r}"
                )
