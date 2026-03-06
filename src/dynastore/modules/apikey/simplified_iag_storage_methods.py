

from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timezone

# --- Simplified IAG Methods (v2.1) ---

async def resolve_identity(self, email: str) -> tuple[str, str]:
    """
    Resolve email to (provider, subject_id).
    Checks local users first, then external providers.
    
    Args:
        email: User email address
        
    Returns:
        Tuple of (provider, subject_id)
        
    Raises:
        ValueError: If no identity found for email
    """
    # 1. Check local users (users schema)
    user = await self.get_user_by_email(email, schema="users")
    if user:
        return ("local", user["username"])
    
    # 2. TODO: Check Keycloak (if configured)
    # if self.keycloak_provider:
    #     keycloak_user = await self.keycloak_provider.get_user_by_email(email)
    #     if keycloak_user:
    #         return ("keycloak", keycloak_user["sub"])
    
    # 3. Not found
    raise ValueError(f"No identity found for email: {email}")

async def get_user_by_email(self, email: str, schema: str = "users") -> Optional[Dict[str, Any]]:
    """Get user by email address."""
    async with managed_transaction(self.engine) as db:
        return await GET_USER_BY_EMAIL.execute(db, schema=schema, email=email)

async def get_identity_roles(
    self, 
    provider: str, 
    subject_id: str, 
    schema: str = "catalog"
) -> List[str]:
    """
    Get roles for an identity in a specific schema.
    
    Args:
        provider: Identity provider (e.g., 'local', 'keycloak')
        subject_id: Subject ID (username, sub, etc.)
        schema: Schema to query ('catalog' for global, tenant code for catalog-specific)
        
    Returns:
        List of role names
    """
    async with managed_transaction(self.engine) as db:
        roles = await GET_IDENTITY_ROLES.execute(
            db, 
            schema=schema, 
            provider=provider, 
            subject_id=subject_id
        )
        return roles or []

async def grant_roles(
    self,
    provider: str,
    subject_id: str,
    roles: List[str],
    schema: str = "catalog",
    granted_by: Optional[str] = None
) -> None:
    """
    Grant roles to an identity.
    
    Args:
        provider: Identity provider
        subject_id: Subject ID
        roles: List of role names to grant
        schema: Schema to grant in ('catalog' for global, tenant for catalog-specific)
        granted_by: Who granted the roles (optional)
    """
    async with managed_transaction(self.engine) as db:
        for role_name in roles:
            await INSERT_IDENTITY_ROLE.execute(
                db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                role_name=role_name,
                granted_by=granted_by
            )

async def revoke_role(
    self,
    provider: str,
    subject_id: str,
    role_name: str,
    schema: str = "catalog"
) -> None:
    """
    Revoke a role from an identity.
    
    Args:
        provider: Identity provider
        subject_id: Subject ID
        role_name: Role name to revoke
        schema: Schema to revoke from
    """
    async with managed_transaction(self.engine) as db:
        await DELETE_IDENTITY_ROLE.execute(
            db,
            schema=schema,
            provider=provider,
            subject_id=subject_id,
            role_name=role_name
        )

async def get_identity_authorization(
    self,
    provider: str,
    subject_id: str,
    schema: str = "catalog"
) -> Optional[Dict[str, Any]]:
    """
    Get authorization metadata for an identity (optional).
    
    Returns:
        Authorization metadata dict or None if not found
    """
    async with managed_transaction(self.engine) as db:
        return await GET_IDENTITY_AUTHORIZATION.execute(
            db,
            schema=schema,
            provider=provider,
            subject_id=subject_id
        )

async def set_identity_authorization(
    self,
    provider: str,
    subject_id: str,
    display_name: Optional[str] = None,
    is_active: bool = True,
    valid_from: Optional[datetime] = None,
    valid_until: Optional[datetime] = None,
    attributes: Optional[Dict[str, Any]] = None,
    schema: str = "catalog"
) -> Dict[str, Any]:
    """
    Set authorization metadata for an identity.
    Creates or updates the record.
    """
    async with managed_transaction(self.engine) as db:
        return await UPSERT_IDENTITY_AUTHORIZATION.execute(
            db,
            schema=schema,
            provider=provider,
            subject_id=subject_id,
            display_name=display_name,
            is_active=is_active,
            valid_from=valid_from or datetime.now(timezone.utc),
            valid_until=valid_until,
            attributes=json.dumps(attributes or {})
        )

async def get_identity_policies(
    self,
    provider: str,
    subject_id: str,
    schema: str = "catalog"
) -> List[Dict[str, Any]]:
    """Get custom policies for an identity."""
    async with managed_transaction(self.engine) as db:
        policies = await GET_IDENTITY_POLICIES.execute(
            db,
            schema=schema,
            provider=provider,
            subject_id=subject_id
        )
        return policies or []

async def add_identity_policy(
    self,
    provider: str,
    subject_id: str,
    policy_name: Optional[str],
    actions: List[str],
    resources: List[str],
    effect: str = "ALLOW",
    conditions: Optional[List[Dict[str, Any]]] = None,
    schema: str = "catalog"
) -> Dict[str, Any]:
    """Add a custom policy to an identity."""
    async with managed_transaction(self.engine) as db:
        return await INSERT_IDENTITY_POLICY.execute(
            db,
            schema=schema,
            provider=provider,
            subject_id=subject_id,
            policy_name=policy_name,
            actions=actions,
            resources=resources,
            effect=effect,
            conditions=json.dumps(conditions or [])
        )

async def delete_identity_policy(
    self,
    policy_id: str,
    schema: str = "catalog"
) -> None:
    """Delete a custom policy."""
    async with managed_transaction(self.engine) as db:
        await DELETE_IDENTITY_POLICY.execute(
            db,
            schema=schema,
            policy_id=policy_id
        )

async def get_catalogs_for_identity(
    self,
    provider: str,
    subject_id: str
) -> List[str]:
    """
    Get list of catalog codes where identity has any roles.
    
    Returns:
        List of catalog codes (schema names)
    """
    async with managed_transaction(self.engine) as db:
        # Query all schemas with identity_roles table
        catalogs = await GET_CATALOGS_FOR_IDENTITY.execute(
            db,
            schema="catalog",  # Use catalog schema for the query
            provider=provider,
            subject_id=subject_id
        )
        return catalogs or []
