"""
Multi-Catalog Principal Management APIs

This module provides helper methods for managing principals across multiple catalogs.
These methods support the global + catalog-specific permission model.
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
import logging
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


async def create_global_principal(
    iam_manager,
    identity: Dict[str, Any],
    roles: List[str],
    policies: List[Any]
) -> Dict[str, Any]:
    """
    Create a global principal with platform-wide permissions.
    
    Args:
        iam_manager: IamService instance
        identity: Identity dict with 'provider' and 'sub' keys
        roles: List of global role names (e.g., ["DataSteward", "Auditor"])
        policies: List of Policy objects with catalog:* wildcards
        
    Returns:
        Created principal dict
    """
    from dynastore.modules.iam.models import Principal
    from uuid import uuid4
    
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    email = identity.get("email", f"{subject_id}@{provider}")
    
    # Create global principal
    principal = Principal(
        id=uuid4(),
        provider=provider,
        subject_id=subject_id,
        display_name=email,
        roles=roles,
        custom_policies=policies,
        is_active=True,
        attributes={}
    )
    
    # Store in catalog schema (global)
    await iam_manager.storage.create_principal(principal, schema="catalog")
    
    # Create identity link
    await iam_manager.storage.create_identity_link(
        principal_id=principal.id,
        provider=provider,
        subject_id=subject_id,
        schema="catalog"
    )
    
    logger.info(f"Created global principal {principal.id} for {provider}:{subject_id}")
    
    return {
        "principal_id": str(principal.id),
        "provider": provider,
        "subject_id": subject_id,
        "roles": roles,
        "scope": "global"
    }


async def sync_principal_across_catalogs(
    iam_manager,
    identity: Dict[str, Any],
    catalog_ids: List[str],
    roles: List[str],
    policies: List[Any]
) -> Dict[str, Any]:
    """
    Synchronize principal configuration across multiple catalogs.
    
    Creates or updates principals in each specified catalog with the same
    roles and policies.
    
    Args:
        iam_manager: IamService instance
        identity: Identity dict with 'provider' and 'sub' keys
        catalog_ids: List of catalog codes to sync to
        roles: List of role names
        policies: List of Policy objects
        
    Returns:
        Summary of sync operation
    """
    from dynastore.modules.iam.models import Principal
    from uuid import uuid4
    
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    email = identity.get("email", f"{subject_id}@{provider}")
    
    results = {
        "created": [],
        "updated": [],
        "failed": []
    }
    
    for catalog_id in catalog_ids:
        try:
            # Resolve schema
            schema = await iam_manager._resolve_schema(catalog_id)
            
            # Check if principal exists
            existing = await iam_manager.storage.get_principal_by_identity(
                provider=provider,
                subject_id=subject_id,
                schema=schema
            )
            
            if existing:
                # Update existing
                existing.roles = roles
                existing.custom_policies = policies
                await iam_manager.storage.update_principal(existing, schema=schema)
                results["updated"].append(catalog_id)
                logger.info(f"Updated principal in {catalog_id}")
            else:
                # Create new
                new_principal = Principal(
                    id=uuid4(),
                    provider=provider,
                    subject_id=subject_id,
                    display_name=email,
                    roles=roles,
                    custom_policies=policies,
                    is_active=True,
                    attributes={}
                )
                
                await iam_manager.storage.create_principal(new_principal, schema=schema)
                await iam_manager.storage.create_identity_link(
                    principal_id=new_principal.id,
                    provider=provider,
                    subject_id=subject_id,
                    schema=schema
                )
                results["created"].append(catalog_id)
                logger.info(f"Created principal in {catalog_id}")
                
        except Exception as e:
            logger.error(f"Failed to sync principal to {catalog_id}: {e}")
            results["failed"].append({"catalog": catalog_id, "error": str(e)})
    
    return results


async def list_catalogs_for_identity(
    iam_manager,
    provider: str,
    subject_id: str
) -> Dict[str, Any]:
    """
    List all catalogs where an identity has a principal.
    
    Args:
        iam_manager: IamService instance
        provider: Identity provider (e.g., "keycloak", "local")
        subject_id: Subject ID from provider
        
    Returns:
        Dict with global flag and list of catalog codes
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol

    # Check for global principal
    global_principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider,
        subject_id=subject_id,
        schema="catalog"  # Global schema
    )

    # Get all catalogs
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise RuntimeError("CatalogsProtocol implementation not registered.")
    db = get_protocol(DatabaseProtocol)
    if db is None:
        raise RuntimeError("DatabaseProtocol implementation not registered.")
    all_catalogs = await catalogs.list_catalogs(ctx=DriverContext(db_resource=db.engine))

    # Check each catalog for principal
    catalog_list = []
    for catalog in all_catalogs:
        try:
            schema = await iam_manager._resolve_schema(catalog.id)
            principal = await iam_manager.storage.get_principal_by_identity(
                provider=provider,
                subject_id=subject_id,
                schema=schema
            )
            if principal:
                catalog_list.append(catalog.id)
        except Exception as e:
            logger.warning(f"Failed to check catalog {catalog.id}: {e}")
    
    return {
        "global": global_principal is not None,
        "catalogs": catalog_list,
        "total": len(catalog_list)
    }


async def get_effective_permissions_for_catalog(
    iam_manager,
    provider: str,
    subject_id: str,
    catalog_id: str
) -> Dict[str, Any]:
    """
    Get detailed permission breakdown for a specific catalog.
    
    Shows global permissions, catalog permissions, and effective merged permissions.
    
    Args:
        iam_manager: IamService instance
        provider: Identity provider
        subject_id: Subject ID
        catalog_id: Target catalog code
        
    Returns:
        Detailed permission breakdown
    """
    identity = {"provider": provider, "sub": subject_id}
    
    # Get global principal
    global_principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider,
        subject_id=subject_id,
        schema="catalog"
    )
    
    # Get catalog principal
    schema = await iam_manager._resolve_schema(catalog_id)
    catalog_principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider,
        subject_id=subject_id,
        schema=schema
    )
    
    # Get effective (merged)
    effective_principal = await iam_manager.get_effective_permissions(
        identity=identity,
        catalog_id=catalog_id
    )
    
    return {
        "global_roles": global_principal.roles if global_principal else [],
        "catalog_roles": catalog_principal.roles if catalog_principal else [],
        "effective_roles": effective_principal.roles if effective_principal else [],
        "global_policies": [p.dict() for p in (global_principal.custom_policies or [])] if global_principal else [],
        "catalog_policies": [p.dict() for p in (catalog_principal.custom_policies or [])] if catalog_principal else [],
        "effective_policies": [p.dict() for p in (effective_principal.custom_policies or [])] if effective_principal else [],
        "has_access": effective_principal is not None
    }
