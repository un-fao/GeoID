import pytest
from sqlalchemy import text
from dynastore.modules.storage.driver_config import PostgresCollectionDriverConfig
from dynastore.modules.catalog.sidecars.geometries_config import GeometriesSidecarConfig
from dynastore.modules.catalog.sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
    AttributeSchemaEntry,
    PostgresType,
)
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DQLQuery,
    ResultHandler,
)


@pytest.mark.asyncio
async def test_attribute_defaults_columnar(app_lifespan, catalog_id, collection_id):
    """Verifies that default values are applied in COLUMNAR mode."""
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.ensure_catalog_exists(catalog_id)

    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(
                name="status",
                type=PostgresType.TEXT,
                default="active",
            ),
            AttributeSchemaEntry(
                name="priority",
                type=PostgresType.INTEGER,
                default=10,
            ),
            AttributeSchemaEntry(
                name="is_public",
                type=PostgresType.BOOLEAN,
                default=True,
            ),
        ],
    )

    col_config = PostgresCollectionDriverConfig(
        sidecars=[GeometriesSidecarConfig(), attr_sidecar]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "Defaults Test Columnar"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )

    # Insert feature without status, priority, or is_public
    item_id = "item-defaults"
    item_data = {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "Should have defaults"},
    }

    res = await catalogs.upsert(catalog_id, collection_id, item_data)
    assert res is not None
    # res is Feature object

    # Verify in DB
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    hub_table = await catalogs.resolve_physical_table(catalog_id, collection_id)
    attr_table = f"{hub_table}_attributes"

    async with managed_transaction(app_lifespan.engine) as conn:
        # We query the only row since we don't have geoid handy
        row = await DQLQuery(
            f'SELECT status, priority, is_public FROM "{phys_schema}"."{attr_table}" LIMIT 1',
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn)

        assert row["status"] == "active"
        assert row["priority"] == 10
        assert row["is_public"] is True

    # Search and verify properties
    search_res = await catalogs.search(
        catalog_id, collection_id, properties=["status", "priority", "is_public"]
    )
    feature = search_res["features"][0]
    assert feature.properties["status"] == "active"
    assert feature.properties["priority"] == 10
    assert feature.properties["is_public"] is True


@pytest.mark.asyncio
async def test_attribute_defaults_jsonb(app_lifespan, catalog_id, collection_id):
    """Verifies that default values are applied in JSONB mode via prepare_upsert_payload."""
    catalogs = get_protocol(CatalogsProtocol)
    collection_id_jsonb = f"{collection_id}_jsonb"
    await catalogs.ensure_catalog_exists(catalog_id)

    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.JSONB,
        attribute_schema=[
            AttributeSchemaEntry(
                name="type",
                type=PostgresType.TEXT,
                default="manual",
            )
        ],
    )

    col_config = PostgresCollectionDriverConfig(
        sidecars=[GeometriesSidecarConfig(), attr_sidecar]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id_jsonb,
            "title": {"en": "Defaults Test JSONB"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )

    item_data = {
        "id": "jsonb-defaults",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "JSONB item"},
    }

    res = await catalogs.upsert(catalog_id, collection_id_jsonb, item_data)
    assert res is not None

    # Search and verify properties
    search_res = await catalogs.search(catalog_id, collection_id_jsonb)
    feature = search_res["features"][0]
    print(f"DEBUG TEST: Feature properties: {feature.properties}")
    assert feature.properties["type"] == "manual"
