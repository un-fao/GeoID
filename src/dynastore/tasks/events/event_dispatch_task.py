import logging
from typing import Dict, Any, List
from pydantic import BaseModel, ConfigDict

from dynastore.tasks.protocols import TaskProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (DQLQuery, ResultHandler, managed_transaction)

logger = logging.getLogger(__name__)

class EventDispatchInputs(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_type: str
    payload: Dict[str, Any]
class EventDispatchTask(TaskProtocol[object, EventDispatchInputs, None]):
    """
    Queries event_subscriptions and enqueues webhook_delivery tasks.
    """
    priority: int = 50

    async def run(self, payload: EventDispatchInputs) -> None:
        from dynastore.models.protocols import DatabaseProtocol
        db = get_protocol(DatabaseProtocol)
        if not db:
            logger.error("EventDispatchTask: DatabaseProtocol not found.")
            return

        event_type = payload.event_type
        logger.info(f"EventDispatchTask: Processing event '{event_type}'")

        # Query subscriptions
        # Note: We reuse the query logic from events_module if possible, 
        # but here we'll define it to keep the task self-contained or import it.
        # Since we want to avoid circular deps, we'll define a local query.
        
        get_subs_query = DQLQuery(
            "SELECT subscriber_name, webhook_url, auth_config FROM event_subscriptions WHERE event_type = :event_type;",
            result_handler=ResultHandler.ALL_DICTS
        )

        try:
            async with managed_transaction(db) as conn:
                subscriptions = await get_subs_query.execute(conn, event_type=event_type)
            
            if not subscriptions:
                logger.debug(f"EventDispatchTask: No subscriptions found for event '{event_type}'")
                return

            logger.info(f"EventDispatchTask: Found {len(subscriptions)} subscriptions for '{event_type}'")

            from dynastore.modules.tasks import tasks_module
            from dynastore.modules.tasks.models import TaskCreate

            for sub in subscriptions:
                try:
                    await tasks_module.create_task(
                        engine=db,
                        task_data=TaskCreate(
                            caller_id=f"event_dispatch:{sub['subscriber_name']}",
                            task_type="webhook_delivery",
                            inputs={
                                "webhook_url": sub["webhook_url"],
                                "auth_config": sub["auth_config"],
                                "payload": payload.payload,
                                "event_type": event_type
                            }
                        ),
                        schema=tasks_module.get_task_schema()
                    )
                except Exception as e:
                    logger.error(f"EventDispatchTask: Failed to enqueue delivery for {sub['subscriber_name']}: {e}")

        except Exception as e:
            logger.error(f"EventDispatchTask: Error querying subscriptions: {e}")
            raise
