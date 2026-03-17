#    Copyright 2026 FAO
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

"""
Per-catalog Elasticsearch configuration plugin.

Registered under plugin_id "elasticsearch" — visible and editable via:
    PUT /configs/catalogs/{catalog_id}/elasticsearch

The on_apply hook fires immediately on every write and delegates to
ElasticsearchModule.enable_obfuscated_mode / disable_obfuscated_mode,
which applies the DENY access policy and dispatches a bulk reindex task.

At service startup ElasticsearchModule.lifespan scans all catalogs and
restores policies for those with obfuscated=True (since on_apply is not
called automatically on restart).
"""

import logging
from typing import Optional, Any

from pydantic import Field

from dynastore.modules.db_config.platform_config_service import PluginConfig, register_config

logger = logging.getLogger(__name__)

ES_CATALOG_CONFIG_ID = "elasticsearch"


async def _on_apply_es_catalog_config(
    config: "ElasticsearchCatalogConfig",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """
    Called by ConfigsService whenever ElasticsearchCatalogConfig is written.
    Applies or revokes obfuscated mode immediately in the running process.
    """
    if not catalog_id:
        # Platform-level default written — no per-catalog action needed.
        return

    from dynastore.tools.discovery import get_protocol
    from dynastore.modules.elasticsearch.module import ElasticsearchModule

    es_module = get_protocol(ElasticsearchModule)
    if not es_module:
        logger.debug(
            "ElasticsearchModule not active in this process — "
            "on_apply skipped for catalog '%s'.",
            catalog_id,
        )
        return

    if config.obfuscated:
        await es_module.enable_obfuscated_mode(catalog_id, db_resource=db_resource)
    else:
        await es_module.disable_obfuscated_mode(catalog_id, db_resource=db_resource)


@register_config(ES_CATALOG_CONFIG_ID, on_apply=_on_apply_es_catalog_config)
class ElasticsearchCatalogConfig(PluginConfig):
    """
    Per-catalog Elasticsearch indexing configuration.

    Editable at runtime via:
        PUT /configs/catalogs/{catalog_id}/elasticsearch
        body: {"obfuscated": true}

    Changes are applied immediately:
    - obfuscated False → True :
        • A DENY policy blocks all_users GET access across every protocol.
        • A bulk reindex task is dispatched to populate the geoid-only index.
        • Items are no longer added to the STAC items index.
    - obfuscated True → False :
        • The DENY policy is removed.
        • A bulk reindex task is dispatched to (re-)populate the STAC items index
          for collections that have search_index=True.
    """

    obfuscated: bool = Field(
        False,
        description=(
            "When True, items in this catalog are indexed in obfuscated mode: "
            "only the geoid UUID is stored in Elasticsearch — no geometry, no "
            "attributes, no spatial search. "
            "GET access to this catalog via any protocol is denied to all_users. "
            "Toggling this field triggers a full catalog reindex automatically."
        ),
    )
