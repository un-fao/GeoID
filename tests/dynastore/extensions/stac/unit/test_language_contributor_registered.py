import pytest
from fastapi import FastAPI
from dynastore.tools.discovery import get_protocols
from dynastore.extensions.stac.stac_service import STACService
from dynastore.extensions.stac.stac_contributor import (
    StacContributor, LanguageStacContributor,
)


@pytest.mark.asyncio
async def test_language_contributor_registered_during_lifespan():
    svc = STACService()
    app = FastAPI()
    async with svc.lifespan(app):
        get_protocols.cache_clear()
        names = {type(c).__name__ for c in get_protocols(StacContributor)}
        assert LanguageStacContributor.__name__ in names
