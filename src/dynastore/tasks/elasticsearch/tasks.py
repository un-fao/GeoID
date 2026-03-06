import logging
from typing import Any, Dict, Optional
from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)

class ElasticsearchIndexInputs(BaseModel):
    entity_type: str  # 'catalog', 'collection', 'item'
    entity_id: str
    catalog_id: str
    collection_id: Optional[str] = None
    item_id: Optional[str] = None
    payload: Dict[str, Any]

class ElasticsearchIndexTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for indexing or updating a document in Elasticsearch.
    """
    task_type = "elasticsearch_index"

    async def run(self, payload: TaskPayload[ElasticsearchIndexInputs]) -> Dict[str, Any]:
        from .config import config
        from .mappings import get_index_name, get_mapping
        
        try:
            from elasticsearch import AsyncElasticsearch
        except ImportError:
            logger.error("Elasticsearch client is not installed. Install with `poetry add elasticsearch[async]`")
            raise RuntimeError("Missing elasticsearch dependency")

        inputs = payload.inputs
        index_name = get_index_name(config.index_prefix, inputs.entity_type)
        
        logger.info(f"ElasticsearchIndexTask: Indexing {inputs.entity_type} '{inputs.entity_id}' into '{index_name}'")

        # Create client
        client_kwargs = {
            "hosts": [config.url],
            "verify_certs": config.verify_certs
        }
        if config.username and config.password:
            client_kwargs["basic_auth"] = (config.username, config.password)

        async with AsyncElasticsearch(**client_kwargs) as es:
            # Ensure index exists
            if not await es.indices.exists(index=index_name):
                mapping = get_mapping(inputs.entity_type)
                logger.info(f"Creating index '{index_name}' with mapping for '{inputs.entity_type}'")
                await es.indices.create(index=index_name, body={"mappings": mapping}, ignore=400)
            
            response = await es.index(
                index=index_name,
                id=inputs.entity_id,
                document=inputs.payload
            )
            
            logger.debug(f"Elasticsearch indexing response: {response}")
            
        return {
            "entity_id": inputs.entity_id,
            "index": index_name,
            "status": "indexed"
        }

class ElasticsearchDeleteInputs(BaseModel):
    entity_type: str
    entity_id: str

class ElasticsearchDeleteTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for deleting a document from Elasticsearch.
    """
    task_type = "elasticsearch_delete"

    async def run(self, payload: TaskPayload[ElasticsearchDeleteInputs]) -> Dict[str, Any]:
        from .config import config
        from .mappings import get_index_name
        
        try:
            from elasticsearch import AsyncElasticsearch
            from elasticsearch.exceptions import NotFoundError
        except ImportError:
            raise RuntimeError("Missing elasticsearch dependency")

        inputs = payload.inputs
        index_name = get_index_name(config.index_prefix, inputs.entity_type)
        
        logger.info(f"ElasticsearchDeleteTask: Deleting {inputs.entity_type} '{inputs.entity_id}' from '{index_name}'")

        # Create client
        client_kwargs = {
            "hosts": [config.url],
            "verify_certs": config.verify_certs
        }
        if config.username and config.password:
            client_kwargs["basic_auth"] = (config.username, config.password)

        async with AsyncElasticsearch(**client_kwargs) as es:
            try:
                await es.delete(index=index_name, id=inputs.entity_id)
            except NotFoundError:
                logger.debug(f"Document {inputs.entity_id} not found in {index_name}, safe to ignore.")
            
        return {
            "entity_id": inputs.entity_id,
            "status": "deleted"
        }
