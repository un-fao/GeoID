import logging
import json
from typing import Dict, Any, Optional
from pydantic import BaseModel, ConfigDict
import httpx

from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)

class WebhookDeliveryInputs(BaseModel):
    model_config = ConfigDict(extra="ignore")
    webhook_url: str
    event_type: str
    payload: Dict[str, Any]
    auth_config: Optional[Dict[str, Any]] = None
class WebhookDeliveryTask(TaskProtocol[object, WebhookDeliveryInputs, Dict[str, Any]]):
    """
    Delivers an event payload to a webhook URL.
    """
    priority: int = 50

    async def run(self, payload: WebhookDeliveryInputs) -> Dict[str, Any]:
        url = payload.webhook_url
        event_type = payload.event_type
        logger.info(f"WebhookDeliveryTask: Delivering '{event_type}' to {url}")

        # Construct headers
        headers = {
            "Content-Type": "application/json",
            "X-DynaStore-Event": event_type,
        }

        # Handle Auth (Simple API Key or Bearer for now)
        if payload.auth_config:
            auth_type = payload.auth_config.get("type")
            auth_value = payload.auth_config.get("value")
            if auth_type == "api_key":
                header_name = payload.auth_config.get("header_name", "X-API-Key")
                headers[header_name] = auth_value
            elif auth_type == "bearer":
                headers["Authorization"] = f"Bearer {auth_value}"

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    url,
                    json=payload.payload,
                    headers=headers
                )
                response.raise_for_status()
                
                logger.info(f"WebhookDeliveryTask: Successfully delivered '{event_type}' to {url}. Status: {response.status_code}")
                
                return {
                    "status_code": response.status_code,
                    "response_body": response.text[:1000] # Cap response log
                }
            except httpx.HTTPStatusError as e:
                logger.error(f"WebhookDeliveryTask: Failed to deliver to {url}. Status: {e.response.status_code}. Response: {e.response.text}")
                # Re-raise to trigger task retry if configured
                raise
            except Exception as e:
                logger.error(f"WebhookDeliveryTask: Unexpected error during delivery to {url}: {e}")
                raise
