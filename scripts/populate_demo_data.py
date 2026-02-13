
import asyncio
import os
import logging
import sys
from types import SimpleNamespace
from contextlib import AsyncExitStack

# Add src to path if needed
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from dynastore import modules, extensions
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("populate_demo_data")

async def populate():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    app_state = SimpleNamespace()
    
    # Enable core modules and extensions
    os.environ["DYNASTORE_MODULES"] = "db_config,db,catalog,stats,config"
    os.environ["DYNASTORE_EXTENSION_MODULES"] = "features,stac"
    
    # 1. Discover and Instantiate
    modules.discover_modules()
    modules.instantiate_modules(app_state)
    
    logger.info("Starting DynaStore modules and extensions...")
    async with modules.lifespan(app_state):
        catalogs_svc = get_protocol(CatalogsProtocol)
        
        if not catalogs_svc:
            logger.info("CatalogsProtocol not available via discovery, instantiating manually...")
            from dynastore.modules.catalog.catalog_service import CatalogService
            from dynastore.modules.catalog.collection_service import CollectionService
            from dynastore.modules.catalog.item_service import ItemService
            
            # Manual instantiation and registration
            from dynastore.tools.discovery import register_provider
            item_svc = ItemService()
            coll_svc = CollectionService()
            catalogs_svc = CatalogService(collection_service=coll_svc, item_service=item_svc)
            
            register_provider(catalogs_svc)
            logger.info("Manually registered CatalogsProtocol.")
        if not catalogs_svc:
            logger.error("CatalogsProtocol not available.")
            return

        # 1. Create Demo Catalog
        catalog_id = "demo_catalog"
        logger.info(f"Ensuring catalog '{catalog_id}' exists...")
        await catalogs_svc.delete_catalog(catalog_id, force=True) # Start clean
        
        catalog_data = {
            "id": catalog_id,
            "title": {"en": "Demo Catalog", "it": "Catalogo Demo"},
            "description": {"en": "A catalog for demonstration purposes.", "it": "Un catalogo per scopi dimostrativi."},
            "keywords": ["demo", "dynastore", "geospatial"],
            "license": "CC-BY-4.0"
        }
        await catalogs_svc.create_catalog(catalog_data, lang="*")
        logger.info(f"Catalog '{catalog_id}' created.")

        # 2. Create Demo Collection
        collection_id = "demo_collection"
        logger.info(f"Ensuring collection '{collection_id}' exists in '{catalog_id}'...")
        collection_data = {
            "id": collection_id,
            "title": {"en": "Demo Collection"},
            "description": {"en": "A demo collection of points."},
            "type": "Feature"
        }
        await catalogs_svc.create_collection(catalog_id, collection_data, lang="*")
        logger.info(f"Collection '{collection_id}' created.")

        # 3. Add Some Items
        logger.info(f"Ingesting demo items into '{catalog_id}:{collection_id}'...")
        demo_items = [
            {
                "id": "item_1",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.4964, 41.9028]}, # Rome
                "properties": {
                    "name": "Rome",
                    "description": "The capital of Italy"
                }
            },
            {
                "id": "item_2",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.8952, 52.3702]}, # Amsterdam
                "properties": {
                    "name": "Amsterdam",
                    "description": "The capital of the Netherlands"
                }
            }
        ]
        
        result = await catalogs_svc.upsert(catalog_id, collection_id, demo_items)
        logger.info(f"Successfully ingested {len(result)} items.")
        
        logger.info("Demo data population complete.")

async def cleanup():
    db_url = os.getenv("DATABASE_URL")
    if not db_url: return
    
    app_state = SimpleNamespace()
    os.environ["DYNASTORE_MODULES"] = "db_config,db,catalog"
    
    modules.discover_modules()
    modules.instantiate_modules(app_state)
    
    async with modules.lifespan(app_state):
        catalogs_svc = get_protocol(CatalogsProtocol)
        
        if not catalogs_svc:
            logger.info("CatalogsProtocol not available via discovery, instantiating manually...")
            from dynastore.modules.catalog.catalog_service import CatalogService
            from dynastore.modules.catalog.collection_service import CollectionService
            from dynastore.modules.catalog.item_service import ItemService
            from dynastore.tools.discovery import register_provider
            
            item_svc = ItemService()
            coll_svc = CollectionService()
            catalogs_svc = CatalogService(collection_service=coll_svc, item_service=item_svc)
            register_provider(catalogs_svc)

        if catalogs_svc:
            catalog_id = "demo_catalog"
            logger.info(f"Cleaning up catalog '{catalog_id}'...")
            await catalogs_svc.delete_catalog(catalog_id, force=True)
            logger.info(f"Catalog '{catalog_id}' deleted.")

if __name__ == "__main__":
    import sys
    command = sys.argv[1] if len(sys.argv) > 1 else "populate"
    
    if command == "populate":
        asyncio.run(populate())
    elif command == "cleanup":
        asyncio.run(cleanup())
    else:
        print(f"Unknown command: {command}. Use 'populate' or 'cleanup'.")
