
#    Copyright 2025 FAO
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

# dynastore/modules/events/events_module.py
import logging
from uuid import UUID
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
import asyncio
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.modules.db_config.query_executor import (DDLQuery, DQLQuery, managed_transaction, ResultHandler, DbResource)
from dynastore.tools.protocol_helpers import resolve
from .models import (EventSubscription, EventSubscriptionCreate, AuthMethod, AuthConfigAPIKey)
from . import catalog_integration
from dynastore.models.protocols import PropertiesProtocol
import os
from dynastore.modules.events.models import API_KEY_NAME

logger = logging.getLogger(__name__)

async def create_event(event_type: str, payload: Dict[str, Any]):
    """
    Creates an event by sending a REST API request to the central event bus service.
    """
    event_bus_url = os.getenv("DYNASTORE_EVENT_BUS_URL")
    if not event_bus_url:
        logger.error("DYNASTORE_EVENT_BUS_URL is not set. Cannot create event via REST.")
        # To prevent silent failures, we raise an error.
        raise RuntimeError("Event bus URL is not configured.")

    # The endpoint on the event bus that receives new events.
    create_event_url = f"{event_bus_url.rstrip('/')}/events/create"
    
    # We need the platform API key to authenticate with the event bus.
    try:
        props = resolve(PropertiesProtocol)
        platform_api_key = await props.get_property(API_KEY_NAME)
    except RuntimeError:
        platform_api_key = None
    
    if not platform_api_key:
        raise RuntimeError(f"Cannot create event: {API_KEY_NAME} not found in shared properties (PropertiesProtocol={props is not None}).")

    headers = {
        "Content-Type": "application/json",
        API_KEY_NAME: platform_api_key
    }
    
    event_payload = {"event_type": event_type, "payload": payload}
    
    try:
        # Use the foundational httpx module to create a client.
        from dynastore.modules.httpx.httpx_module import create_httpx_client
        async with create_httpx_client() as http_client:
            response = await http_client.post(create_event_url, json=event_payload, headers=headers)
            response.raise_for_status() # Raise an exception for 4xx or 5xx status codes
            logger.info(f"Successfully sent event '{event_type}' to the event bus.")
    except Exception as e:
        logger.error(f"Failed to send event '{event_type}' to the event bus at {create_event_url}: {e}", exc_info=True)
        # Re-raise to ensure the calling operation is aware of the failure.
        raise