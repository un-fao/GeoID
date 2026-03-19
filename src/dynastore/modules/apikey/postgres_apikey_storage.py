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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/modules/apikey/postgres_apikey_storage.py

from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from uuid import UUID
import json
import secrets
import hashlib
import random
import logging

from dynastore.modules.db_config.exceptions import ForeignKeyViolationError, TableNotFoundError
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
    managed_transaction,
    managed_nested_transaction,
)
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.tools.protocol_helpers import get_engine
from .models import (
    Principal,
    ApiKey,
    ApiKeyCreate,
    ApiKeyStatusFilter,
    Role,
    RefreshToken,
    IdentityLink,
    Policy,
)
from .exceptions import PrincipalNotFoundError
from .apikey_storage import AbstractApiKeyStorage
from .interfaces import AuthorizationStorageProtocol

logger = logging.getLogger(__name__)

# --- Constants ---

USAGE_SHARD_COUNT = 16

# --- Queries (V2 + V1 Merged) ---

# V2: Principals (Enhanced)
CREATE_PRINCIPALS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.principals (
        id UUID PRIMARY KEY,
        identifier VARCHAR(512), -- Legacy/Display Name
        display_name VARCHAR(255), -- V2
        is_active BOOLEAN DEFAULT TRUE,
        valid_until TIMESTAMPTZ,
        roles JSONB DEFAULT '[]'::jsonb,
        custom_policies JSONB DEFAULT '[]'::jsonb,
        attributes JSONB DEFAULT '{{}}'::jsonb,
        metadata JSONB DEFAULT '{{}}'::jsonb, -- Legacy support
        policy JSONB, -- Legacy support
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(identifier)
    );
""")

# V2: Identity Links
CREATE_IDENTITY_LINKS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.identity_links (
        provider VARCHAR(64) NOT NULL,
        subject_id VARCHAR(255) NOT NULL,
        principal_id UUID NOT NULL REFERENCES {schema}.principals(id) ON DELETE CASCADE,
        email VARCHAR(255),
        last_login TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (provider, subject_id)
    );
""")

# V1: API Keys (Key Auth Credential)
CREATE_API_KEYS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.api_keys (
        key_hash VARCHAR(64) NOT NULL,
        key_prefix VARCHAR(10) NOT NULL,
        principal_id UUID NOT NULL REFERENCES {schema}.principals(id) ON DELETE CASCADE,
        name VARCHAR(255),
        note TEXT,
        allowed_domains JSONB DEFAULT '[]'::jsonb,
        max_usage BIGINT,
        total_usage BIGINT DEFAULT 0,
        referer_match TEXT,
        catalog_match TEXT,
        collection_match TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        expires_at TIMESTAMPTZ,
        policy JSONB, 
        conditions JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (key_hash, principal_id)
    );
""")

# V1: Usage Counters
CREATE_USAGE_COUNTERS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.usage_counters (
        key_hash VARCHAR(128) NOT NULL,
        period_start TIMESTAMPTZ NOT NULL,
        shard_id INTEGER NOT NULL DEFAULT 0,
        count BIGINT DEFAULT 0,
        last_accessed_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (period_start, key_hash, shard_id)
    ) PARTITION BY RANGE (period_start);
""")

CREATE_QUOTA_COUNTERS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.quota_counters (
        key_hash VARCHAR(128) NOT NULL,
        shard_id INTEGER NOT NULL DEFAULT 0,
        count BIGINT DEFAULT 0,
        last_accessed_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (key_hash, shard_id)
    );
""")

# V2: Dynamic Roles
CREATE_ROLES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.roles (
        id VARCHAR(128) PRIMARY KEY, -- Renamed from name or separate ID? Assuming ID=Name for simplicity in transition
        name VARCHAR(128) NOT NULL,
        description TEXT,
        level INTEGER DEFAULT 0, -- Legacy support
        parent_roles JSONB DEFAULT '[]'::jsonb, -- V2 Inheritance
        policies JSONB DEFAULT '[]'::jsonb, -- V2 Policy Links
        is_system BOOLEAN DEFAULT FALSE,
        metadata JSONB DEFAULT '{{}}'::jsonb, -- Legacy support
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

CREATE_ROLE_HIERARCHY_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.role_hierarchy (
        parent_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        child_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        PRIMARY KEY (parent_role, child_role)
    );
""")

CREATE_POLICIES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.policies (
        id VARCHAR(128) NOT NULL,
        version VARCHAR(16) DEFAULT '1.0',
        description TEXT,
        effect VARCHAR(16) DEFAULT 'ALLOW',
        actions JSONB NOT NULL DEFAULT '[]'::jsonb,
        resources JSONB DEFAULT '["*"]'::jsonb,
        conditions JSONB DEFAULT '[]'::jsonb,
        partition_key VARCHAR(64) NOT NULL DEFAULT 'global',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, partition_key)
    ) PARTITION BY LIST (partition_key);
""")

CREATE_PARTITION_GLOBAL = DDLQuery(
    "CREATE TABLE IF NOT EXISTS {schema}.policies_global PARTITION OF {schema}.policies FOR VALUES IN ('global');"
)

CREATE_REFRESH_TOKENS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.refresh_tokens (
        id VARCHAR(128) PRIMARY KEY,
        key_hash VARCHAR(64) NOT NULL,
        principal_id UUID NOT NULL REFERENCES {schema}.principals(id) ON DELETE CASCADE,
        api_key_hash VARCHAR(64),
        is_active BOOLEAN DEFAULT TRUE,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

# --- Local Authentication Tables (System Schema) ---

CREATE_USERS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.users (
        id UUID PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        email VARCHAR(255),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

CREATE_OAUTH_CODES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.oauth_codes (
        code VARCHAR(128) PRIMARY KEY,
        user_id UUID NOT NULL REFERENCES {schema}.users(id) ON DELETE CASCADE,
        redirect_uri TEXT NOT NULL,
        scope TEXT,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

CREATE_OAUTH_TOKENS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.oauth_tokens (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES {schema}.users(id) ON DELETE CASCADE,
        token_hash VARCHAR(64) NOT NULL,
        token_type VARCHAR(16) DEFAULT 'access',
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

# --- Simplified IAG Tables (v2.1) ---

CREATE_IDENTITY_AUTHORIZATION_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.identity_authorization (
        provider VARCHAR(64) NOT NULL,
        subject_id VARCHAR(255) NOT NULL,
        
        -- Authorization metadata
        display_name VARCHAR(255),
        is_active BOOLEAN DEFAULT TRUE,
        valid_from TIMESTAMPTZ DEFAULT NOW(),
        valid_until TIMESTAMPTZ,
        
        -- ABAC attributes
        attributes JSONB DEFAULT '{{}}'::jsonb,
        
        -- Audit
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        
        PRIMARY KEY (provider, subject_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_identity_auth_active 
        ON {schema}.identity_authorization(is_active);
    CREATE INDEX IF NOT EXISTS idx_identity_auth_expiry 
        ON {schema}.identity_authorization(valid_until) 
        WHERE valid_until IS NOT NULL;
""")

CREATE_IDENTITY_ROLES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.identity_roles (
        provider VARCHAR(64) NOT NULL,
        subject_id VARCHAR(255) NOT NULL,
        role_name VARCHAR(128) NOT NULL,
        
        -- Audit
        granted_by VARCHAR(255),
        granted_at TIMESTAMPTZ DEFAULT NOW(),
        
        PRIMARY KEY (provider, subject_id, role_name)
        -- NO foreign key - identity_authorization is optional!
    );
    
    CREATE INDEX IF NOT EXISTS idx_identity_roles_lookup 
        ON {schema}.identity_roles(provider, subject_id);
""")

CREATE_IDENTITY_POLICIES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.identity_policies (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        provider VARCHAR(64) NOT NULL,
        subject_id VARCHAR(255) NOT NULL,
        
        -- Policy definition
        policy_name VARCHAR(128),
        actions TEXT[] NOT NULL,
        resources TEXT[] NOT NULL DEFAULT ARRAY['*'],
        effect VARCHAR(16) DEFAULT 'ALLOW',
        conditions JSONB DEFAULT '[]'::jsonb,
        
        -- Audit
        created_at TIMESTAMPTZ DEFAULT NOW()
        
        -- NO foreign key - identity_authorization is optional!
    );
    
    CREATE INDEX IF NOT EXISTS idx_identity_policies_lookup 
        ON {schema}.identity_policies(provider, subject_id);
""")


# --- DML / DQL ---

# Simplified IAG Queries (v2.1)

GET_USER_BY_EMAIL = DQLQuery(
    "SELECT * FROM {schema}.users WHERE email = :email;",
    result_handler=ResultHandler.ONE_DICT,
)

GET_IDENTITY_ROLES = DQLQuery(
    "SELECT role_name FROM {schema}.identity_roles WHERE provider = :provider AND subject_id = :subject_id;",
    result_handler=ResultHandler.ALL_SCALARS,
)

INSERT_IDENTITY_ROLE = DQLQuery(
    """
    INSERT INTO {schema}.identity_roles (provider, subject_id, role_name, granted_by)
    VALUES (:provider, :subject_id, :role_name, :granted_by)
    ON CONFLICT (provider, subject_id, role_name) DO UPDATE SET
        granted_by = EXCLUDED.granted_by,
        granted_at = NOW();
    """,
    result_handler=ResultHandler.NONE,
)

DELETE_IDENTITY_ROLE = DQLQuery(
    """
    DELETE FROM {schema}.identity_roles  
    WHERE provider = :provider AND subject_id = :subject_id AND role_name = :role_name;
    """,
    result_handler=ResultHandler.NONE,
)

GET_IDENTITY_AUTHORIZATION = DQLQuery(
    "SELECT * FROM {schema}.identity_authorization WHERE provider = :provider AND subject_id = :subject_id;",
    result_handler=ResultHandler.ONE_DICT,
)

UPSERT_IDENTITY_AUTHORIZATION = DQLQuery(
    """
    INSERT INTO {schema}.identity_authorization 
    (provider, subject_id, display_name, is_active, valid_from, valid_until, attributes)
    VALUES (:provider, :subject_id, :display_name, :is_active, :valid_from, :valid_until, :attributes)
    ON CONFLICT (provider, subject_id) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        is_active = EXCLUDED.is_active,
        valid_from = EXCLUDED.valid_from,
        valid_until = EXCLUDED.valid_until,
        attributes = EXCLUDED.attributes,
        updated_at = NOW()
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

GET_IDENTITY_POLICIES = DQLQuery(
    "SELECT * FROM {schema}.identity_policies WHERE provider = :provider AND subject_id = :subject_id;",
    result_handler=ResultHandler.ALL_DICTS,
)

INSERT_IDENTITY_POLICY = DQLQuery(
    """
    INSERT INTO {schema}.identity_policies 
    (provider, subject_id, policy_name, actions, resources, effect, conditions)
    VALUES (:provider, :subject_id, :policy_name, :actions, :resources, :effect, :conditions)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

DELETE_IDENTITY_POLICY = DQLQuery(
    "DELETE FROM {schema}.identity_policies WHERE id = :policy_id;",
    result_handler=ResultHandler.NONE,
)

GET_CATALOGS_FOR_IDENTITY = DQLQuery(
    """
    SELECT DISTINCT table_schema 
    FROM information_schema.tables 
    WHERE table_name = 'identity_roles' 
    AND table_schema != 'apikey'
    AND EXISTS (
        SELECT 1 FROM {schema}.identity_roles 
        WHERE provider = :provider AND subject_id = :subject_id
    );
    """,
    result_handler=ResultHandler.ALL_SCALARS,
)

GET_CATALOG_USERS = DQLQuery(
    """
    SELECT DISTINCT ir.provider, ir.subject_id, ia.display_name, ia.is_active
    FROM {schema}.identity_roles ir
    LEFT JOIN {schema}.identity_authorization ia ON ir.provider = ia.provider AND ir.subject_id = ia.subject_id;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

CHECK_TABLE_EXISTS = DQLQuery(
    """
    SELECT EXISTS (
        SELECT 1 
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema 
        AND c.relname = :table_name
        AND c.relkind IN ('r', 'v', 'm', 'f') -- table, view, matview, foreign table
    );
    """,
    result_handler=ResultHandler.SCALAR_ONE,
)

# Legacy Principal Queries

INSERT_PRINCIPAL = DQLQuery(
    """
    INSERT INTO {schema}.principals 
    (id, identifier, display_name, is_active, valid_until, roles, custom_policies, attributes, metadata, policy)
    VALUES 
    (:id, :identifier, :display_name, :is_active, :valid_until, :roles, :custom_policies, :attributes, :metadata, :policy)
    ON CONFLICT (id) DO UPDATE SET
        identifier = EXCLUDED.identifier,
        display_name = EXCLUDED.display_name,
        is_active = EXCLUDED.is_active,
        valid_until = EXCLUDED.valid_until,
        roles = EXCLUDED.roles,
        custom_policies = EXCLUDED.custom_policies,
        attributes = EXCLUDED.attributes,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

UPDATE_PRINCIPAL = DQLQuery(
    """
    UPDATE {schema}.principals 
    SET identifier = :identifier, 
        display_name = :display_name,
        metadata = :metadata, 
        roles = :roles, 
        policy = :policy,
        custom_policies = :custom_policies,
        attributes = :attributes
    WHERE id = :id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_ID_BY_IDENTIFIER = DQLQuery(
    "SELECT id FROM {schema}.principals WHERE identifier = :identifier;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    post_processor=lambda row: UUID(str(row)) if row else None,
)

GET_PRINCIPAL_BY_ID = DQLQuery(
    "SELECT * FROM {schema}.principals WHERE id = :id;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_BY_IDENTIFIER = DQLQuery(
    "SELECT * FROM {schema}.principals WHERE identifier = :identifier;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_BY_IDENTITY = DQLQuery(
    """
    SELECT p.* FROM {schema}.principals p
    JOIN {schema}.identity_links l ON p.id = l.principal_id
    WHERE l.provider = :provider AND l.subject_id = :subject_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

INSERT_IDENTITY_LINK = DQLQuery(
    """
    INSERT INTO {schema}.identity_links
    (provider, subject_id, principal_id, email)
    VALUES
    (:provider, :subject_id, :principal_id, :email)
    ON CONFLICT (provider, subject_id) DO NOTHING;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

INSERT_API_KEY = DQLQuery(
    """
    INSERT INTO {schema}.api_keys (
        key_hash, key_prefix, principal_id, name, note, expires_at, policy, conditions,
        max_usage, allowed_domains, referer_match, catalog_match, collection_match
    ) 
    VALUES (
        :key_hash, :key_prefix, :principal_id, :name, :note, :expires_at, :policy, :conditions,
        :max_usage, :allowed_domains, :referer_match, :catalog_match, :collection_match
    ) 
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: ApiKey(**row) if row else None,
)

GET_KEY_METADATA = DQLQuery(
    "SELECT * FROM {schema}.api_keys WHERE key_hash = :key_hash;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: ApiKey(**row) if row else None,
)

LIST_KEYS_BY_PRINCIPAL = DQLQuery(
    "SELECT * FROM {schema}.api_keys WHERE principal_id = :principal_id;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [ApiKey(**row) for row in rows],
)

LIST_PRINCIPALS = DQLQuery(
    "SELECT * FROM {schema}.principals ORDER BY created_at DESC LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Principal(**row) for row in rows],
)

DELETE_PRINCIPAL = DQLQuery(
    "DELETE FROM {schema}.principals WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

DELETE_KEY = DQLQuery(
    "DELETE FROM {schema}.api_keys WHERE key_hash = :key_hash;",
    result_handler=ResultHandler.ROWCOUNT,
)

INVALIDATE_KEY = DQLQuery(
    "UPDATE {schema}.api_keys SET is_active = FALSE WHERE key_hash = :key_hash RETURNING is_active;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

# --- Usage (Rate Limit) Queries ---
INCREMENT_USAGE = DQLQuery(
    """
    INSERT INTO {schema}.usage_counters (key_hash, period_start, shard_id, count, last_accessed_at)
    VALUES (:key_hash, :period_start, :shard_id, :amount, :last_access)
    ON CONFLICT (period_start, key_hash, shard_id) 
    DO UPDATE SET 
        count = {schema}.usage_counters.count + :amount,
        last_accessed_at = GREATEST({schema}.usage_counters.last_accessed_at, EXCLUDED.last_accessed_at);
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

CHECK_USAGE = DQLQuery(
    "SELECT SUM(count) FROM {schema}.usage_counters WHERE key_hash = :key_hash AND period_start = :period_start;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

MIGRATE_USAGE = DQLQuery(
    "UPDATE {schema}.usage_counters SET key_hash = :new_hash WHERE key_hash = :old_hash;",
    result_handler=ResultHandler.ROWCOUNT,
)

INCREMENT_QUOTA = DQLQuery(
    """
    INSERT INTO {schema}.quota_counters (key_hash, shard_id, count, last_accessed_at)
    VALUES (:key_hash, :shard_id, :amount, :last_access)
    ON CONFLICT (key_hash, shard_id) 
    DO UPDATE SET 
        count = {schema}.quota_counters.count + :amount,
        last_accessed_at = GREATEST({schema}.quota_counters.last_accessed_at, EXCLUDED.last_accessed_at);
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

CHECK_QUOTA = DQLQuery(
    "SELECT SUM(count) FROM {schema}.quota_counters WHERE key_hash = :key_hash;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

MIGRATE_QUOTA = DQLQuery(
    "UPDATE {schema}.quota_counters SET key_hash = :new_hash WHERE key_hash = :old_hash;",
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_KEYS_FILTERED = """
    SELECT * FROM {schema}.api_keys 
    WHERE (:principal_id::UUID IS NULL OR principal_id = :principal_id)
      AND (:is_active::BOOLEAN IS NULL OR is_active = :is_active)
    ORDER BY created_at DESC 
    LIMIT :limit OFFSET :offset;
"""

DELETE_STALE_KEYS = DQLQuery(
    """
    DELETE FROM {schema}.api_keys 
    WHERE (expires_at < NOW() - CAST(:retention AS INTERVAL))
       OR (is_active = FALSE AND created_at < NOW() - CAST(:retention AS INTERVAL));
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

# --- Role & Hierarchy Queries ---

INSERT_ROLE = DQLQuery(
    """
    INSERT INTO {schema}.roles (id, name, description, level, metadata, parent_roles, policies) 
    VALUES (:id, :name, :description, :level, :metadata, :parent_roles, :policies) 
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        level = EXCLUDED.level,
        metadata = EXCLUDED.metadata,
        parent_roles = EXCLUDED.parent_roles,
        policies = EXCLUDED.policies
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

GET_ROLE = DQLQuery(
    "SELECT * FROM {schema}.roles WHERE id = :name OR name = :name;",  # Support ID or Name lookup
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

LIST_ROLES = DQLQuery(
    "SELECT * FROM {schema}.roles ORDER BY level DESC, name ASC;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Role(**row) for row in rows],
)

INSERT_ROLE_HIERARCHY = DQLQuery(
    "INSERT INTO {schema}.role_hierarchy (parent_role, child_role) VALUES (:parent_role, :child_role) ON CONFLICT DO NOTHING;",
    result_handler=ResultHandler.ROWCOUNT,
)

DELETE_ROLE_HIERARCHY = DQLQuery(
    "DELETE FROM {schema}.role_hierarchy WHERE parent_role = :parent_role AND child_role = :child_role;",
    result_handler=ResultHandler.ROWCOUNT,
)

UPDATE_ROLE = DQLQuery(
    """
    UPDATE {schema}.roles 
    SET description = :description, 
        level = :level, 
        metadata = :metadata,
        parent_roles = :parent_roles,
        policies = :policies
    WHERE id = :name OR name = :name
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

DELETE_ROLE = DQLQuery(
    "DELETE FROM {schema}.roles WHERE id = :name OR name = :name;",
    result_handler=ResultHandler.ROWCOUNT,
)

REMOVE_ROLE_FROM_PRINCIPALS = DQLQuery(
    """
    UPDATE {schema}.principals 
    SET roles = (
        SELECT jsonb_agg(r) 
        FROM jsonb_array_elements(roles) r 
        WHERE r #>> '{{}}' != :role_name
    )
    WHERE roles ? :role_name;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

# Recursive CTE to find all child roles (descendants)
GET_FULL_ROLE_HIERARCHY = DQLQuery(
    """
    WITH RECURSIVE hierarchy AS (
        -- Base case: the initial roles
        SELECT child_role FROM {schema}.role_hierarchy WHERE parent_role = ANY(:role_names)
        UNION
        -- Recursive step: find children of the roles already found
        SELECT rh.child_role 
        FROM {schema}.role_hierarchy rh 
        JOIN hierarchy h ON rh.parent_role = h.child_role
    )
    SELECT DISTINCT child_role FROM hierarchy;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [row["child_role"] for row in rows],
)

GET_EFFECTIVE_ROLES = DQLQuery(
    """
    WITH RECURSIVE hierarchy AS (
        -- Base case: Direct roles assigned to the principal
        SELECT role_id FROM (
            SELECT jsonb_array_elements_text(roles) as role_id 
            FROM {schema}.principals 
            WHERE id = :principal_id::uuid
        ) initial_roles
        
        UNION
        
        -- Recursive step: Find parent roles (Inheritance)
        SELECT p.parent_id
        FROM hierarchy h
        JOIN {schema}.roles r ON r.id = h.role_id
        CROSS JOIN LATERAL jsonb_array_elements_text(r.parent_roles) as p(parent_id)
    )
    SELECT DISTINCT role_id FROM hierarchy;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [row["role_id"] for row in rows],
)

INSERT_REFRESH_TOKEN = DQLQuery(
    "INSERT INTO {schema}.refresh_tokens (id, key_hash, principal_id, api_key_hash, expires_at) VALUES (:id, :key_hash, :principal_id, :api_key_hash, :expires_at) RETURNING *;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: RefreshToken(**row) if row else None,
)

GET_REFRESH_TOKEN = DQLQuery(
    "SELECT * FROM {schema}.refresh_tokens WHERE id = :id;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: RefreshToken(**row) if row else None,
)

INVALIDATE_REFRESH_TOKEN = DQLQuery(
    "UPDATE {schema}.refresh_tokens SET is_active = FALSE WHERE id = :id RETURNING is_active;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

PRUNE_EXPIRED_REFRESH_TOKENS = DQLQuery(
    "DELETE FROM {schema}.refresh_tokens WHERE expires_at < NOW();",
    result_handler=ResultHandler.ROWCOUNT,
)

PRUNE_EXPIRED_KEYS = DQLQuery(
    "DELETE FROM {schema}.api_keys WHERE expires_at < NOW() AND is_active = FALSE;",
    result_handler=ResultHandler.ROWCOUNT,
)

# --- Policy CRUD Queries ---

INSERT_POLICY = DQLQuery(
    """
    INSERT INTO {schema}.policies (id, version, description, effect, actions, resources, conditions, partition_key)
    VALUES (:id, :version, :description, :effect, :actions, :resources, :conditions, :partition_key)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

GET_POLICY = DQLQuery(
    "SELECT * FROM {schema}.policies WHERE id = :id;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

LIST_POLICIES = DQLQuery(
    """SELECT * FROM {schema}.policies 
    ORDER BY created_at DESC 
    LIMIT :limit OFFSET :offset;""",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Policy(**row) for row in rows],
)

UPDATE_POLICY = DQLQuery(
    """
    UPDATE {schema}.policies
    SET version = :version,
        description = :description,
        effect = :effect,
        actions = :actions,
        resources = :resources,
        conditions = :conditions
    WHERE id = :id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

DELETE_POLICY = DQLQuery(
    "DELETE FROM {schema}.policies WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

# --- Identity Link Queries ---

DELETE_IDENTITY_LINK = DQLQuery(
    "DELETE FROM {schema}.identity_links WHERE provider = :provider AND subject_id = :subject_id;",
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_IDENTITY_LINKS = DQLQuery(
    """
    SELECT provider, subject_id, principal_id, email, created_at
    FROM {schema}.identity_links
    WHERE principal_id = :principal_id
    ORDER BY created_at DESC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [IdentityLink(**row) for row in rows],
)

# --- Local Authentication Queries ---

INSERT_USER = DQLQuery(
    """
    INSERT INTO {schema}.users (id, username, password_hash, email, is_active)
    VALUES (:id, :username, :password_hash, :email, :is_active)
    RETURNING id, username, email, is_active, created_at, updated_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

GET_USER_BY_USERNAME = DQLQuery(
    """
    SELECT id, username, password_hash, email, is_active, created_at, updated_at
    FROM {schema}.users
    WHERE username = :username;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

GET_USER_BY_ID = DQLQuery(
    """
    SELECT id, username, password_hash, email, is_active, created_at, updated_at
    FROM {schema}.users
    WHERE id = :id;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

INSERT_OAUTH_CODE = DQLQuery(
    """
    INSERT INTO {schema}.oauth_codes (code, user_id, redirect_uri, scope, expires_at)
    VALUES (:code, :user_id, :redirect_uri, :scope, :expires_at)
    RETURNING code, user_id, redirect_uri, scope, expires_at, created_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

GET_OAUTH_CODE = DQLQuery(
    """
    SELECT code, user_id, redirect_uri, scope, expires_at, created_at
    FROM {schema}.oauth_codes
    WHERE code = :code;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

DELETE_OAUTH_CODE = DQLQuery(
    """
    DELETE FROM {schema}.oauth_codes
    WHERE code = :code;
    """,
    result_handler=ResultHandler.NONE,
)

INSERT_OAUTH_TOKEN = DQLQuery(
    """
    INSERT INTO {schema}.oauth_tokens (user_id, token_hash, token_type, expires_at)
    VALUES (:user_id, :token_hash, :token_type, :expires_at)
    RETURNING id, user_id, token_hash, token_type, expires_at, created_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

GET_OAUTH_TOKEN_BY_HASH = DQLQuery(
    """
    SELECT id, user_id, token_hash, token_type, expires_at, created_at
    FROM {schema}.oauth_tokens
    WHERE token_hash = :token_hash AND token_type = :token_type;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

DELETE_EXPIRED_OAUTH_TOKENS = DQLQuery(
    """
    DELETE FROM {schema}.oauth_tokens
    WHERE expires_at < NOW();
    """,
    result_handler=ResultHandler.NONE,
)


def build_search_keys_query(
    principal_id: Optional[UUID],
    is_active: Optional[bool],
    limit: int,
    offset: int,
    schema: str = "apikey",
):
    clauses = []
    params = {"limit": limit, "offset": offset}

    if principal_id:
        clauses.append("principal_id = :principal_id")
        params["principal_id"] = principal_id

    if is_active is not None:
        clauses.append("is_active = :is_active")
        params["is_active"] = is_active

    where_clause = " AND ".join(clauses) if clauses else "1=1"

    sql = f"""
        SELECT * FROM {schema}.api_keys 
        WHERE {where_clause}
        ORDER BY created_at DESC 
        LIMIT :limit OFFSET :offset;
    """
    return DQLQuery(
        sql,
        result_handler=ResultHandler.ALL_DICTS,
        post_processor=lambda rows: [ApiKey(**row) for row in rows],
    ), params


def build_search_principals_query(
    identifier: Optional[str],
    role: Optional[str],
    limit: int,
    offset: int,
    schema: str = "apikey",
):
    clauses = []
    params = {"limit": limit, "offset": offset}

    if identifier:
        clauses.append(
            "identifier LIKE :identifier_pattern OR display_name LIKE :identifier_pattern"
        )
        params["identifier_pattern"] = f"%{identifier}%"

    # V1/V2 Role filtering: Check both JSON array and Metadata
    if role:
        clauses.append("(metadata->>'role' = :role OR roles ? :role)")
        params["role"] = role

    where_clause = " AND ".join(clauses) if clauses else "1=1"

    sql = f"""
        SELECT * FROM {schema}.principals 
        WHERE {where_clause}
        ORDER BY created_at DESC 
        LIMIT :limit OFFSET :offset;
    """
    return DQLQuery(
        sql,
        result_handler=ResultHandler.ALL_DICTS,
        post_processor=lambda rows: [Principal(**row) for row in rows],
    ), params


class PostgresApiKeyStorage(AbstractApiKeyStorage, AuthorizationStorageProtocol):
    engine: Optional[DbResource] = None

    def __init__(self, app_state: Optional[object] = None) -> None:
        super().__init__()
        self.engine = get_engine()
        self._known_partitions = set()

    async def initialize(self, conn: DbResource, schema: str = "apikey"):
        """Compatibility alias for _initialize_schema."""
        return await self._initialize_schema(conn, schema=schema)

    async def _initialize_schema(self, conn: DbResource, schema: str = "apikey"):
        """
        Initializes the API Key storage backend for a specific schema.
        Includes V1 (Keys) and V2 (Identity/RBAC) tables.
        """
        from dynastore.modules.db_config.locking_tools import (
            check_table_exists,
        )

        # Strip quotes just in case, to prevent double quoting
        schema = schema.strip('"')

        logger.info(
            f"Initializing PostgresApiKeyStorage schemas and tables for '{schema}'..."
        )

        # 0. Ensure Schema
        await maintenance_tools.ensure_schema_exists(conn, schema)

        # 1. Base Tables (Check-Before-Lock Pattern)

        # Principals (Enhanced V2)
        await DDLQuery(
            CREATE_PRINCIPALS_TABLE.template,
            lock_key=f"{schema}_principals_table",
            check_query=lambda: check_table_exists(conn, "principals", schema=schema),
        ).execute(conn, schema=schema)

        # Identity Links (V2)
        await DDLQuery(
            CREATE_IDENTITY_LINKS_TABLE.template,
            lock_key=f"{schema}_identity_links_table",
            check_query=lambda: check_table_exists(conn, "identity_links", schema=schema),
        ).execute(conn, schema=schema)

        # Simplified IAG Tables (v2.1) - All Schemas
        await DDLQuery(
            CREATE_IDENTITY_AUTHORIZATION_TABLE.template,
            lock_key=f"{schema}_identity_authorization_table",
            check_query=lambda: check_table_exists(conn, "identity_authorization", schema=schema),
        ).execute(conn, schema=schema)

        await DDLQuery(
            CREATE_IDENTITY_ROLES_TABLE.template,
            lock_key=f"{schema}_identity_roles_table",
            check_query=lambda: check_table_exists(conn, "identity_roles", schema=schema),
        ).execute(conn, schema=schema)

        await DDLQuery(
            CREATE_IDENTITY_POLICIES_TABLE.template,
            lock_key=f"{schema}_identity_policies_table",
            check_query=lambda: check_table_exists(conn, "identity_policies", schema=schema),
        ).execute(conn, schema=schema)

        # Roles (V2)
        await DDLQuery(
            CREATE_ROLES_TABLE.template,
            lock_key=f"{schema}_roles_table",
            check_query=lambda: check_table_exists(conn, "roles", schema=schema),
        ).execute(conn, schema=schema)

        # Role Hierarchy
        await DDLQuery(
            CREATE_ROLE_HIERARCHY_TABLE.template,
            lock_key=f"{schema}_role_hierarchy_table",
            check_query=lambda: check_table_exists(conn, "role_hierarchy", schema=schema),
        ).execute(conn, schema=schema)

        # API Keys (V1 Legacy)
        await DDLQuery(
            CREATE_API_KEYS_TABLE.template,
            lock_key=f"{schema}_api_keys_table",
            check_query=lambda: check_table_exists(conn, "api_keys", schema=schema),
        ).execute(conn, schema=schema)

        # Usage Counters
        await DDLQuery(
            CREATE_USAGE_COUNTERS_TABLE.template,
            lock_key=f"{schema}_usage_counters_table",
            check_query=lambda: check_table_exists(conn, "usage_counters", schema=schema),
        ).execute(conn, schema=schema)

        # Quota Counters
        await DDLQuery(
            CREATE_QUOTA_COUNTERS_TABLE.template,
            lock_key=f"{schema}_quota_counters_table",
            check_query=lambda: check_table_exists(conn, "quota_counters", schema=schema),
        ).execute(conn, schema=schema)

        # Refresh Tokens
        await DDLQuery(
            CREATE_REFRESH_TOKENS_TABLE.template,
            lock_key=f"{schema}_refresh_tokens_table",
            check_query=lambda: check_table_exists(conn, "refresh_tokens", schema=schema),
        ).execute(conn, schema=schema)

        # Policies (V2)
        await DDLQuery(
            CREATE_POLICIES_TABLE.template,
            lock_key=f"{schema}_policies_table",
            check_query=lambda: check_table_exists(conn, "policies", schema=schema),
        ).execute(conn, schema=schema)

        # Local Authentication Tables (users schema)
        # Identities are global - stored in users schema (managed by auth extension)
        if schema in ["users", "apikey"]:
            # OAuth2 Authentication Tables
            await DDLQuery(
                CREATE_USERS_TABLE.template,
                lock_key=f"{schema}_users_table",
                check_query=lambda: check_table_exists(conn, "users", schema=schema),
            ).execute(conn, schema=schema)

            await DDLQuery(
                CREATE_OAUTH_CODES_TABLE.template,
                lock_key=f"{schema}_oauth_codes_table",
                check_query=lambda: check_table_exists(conn, "oauth_codes", schema=schema),
            ).execute(conn, schema=schema)

            await DDLQuery(
                CREATE_OAUTH_TOKENS_TABLE.template,
                lock_key=f"{schema}_oauth_tokens_table",
                check_query=lambda: check_table_exists(conn, "oauth_tokens", schema=schema),
            ).execute(conn, schema=schema)

            # Ensure global partition
            await DDLQuery(
                CREATE_PARTITION_GLOBAL.template,
                lock_key=f"{schema}_global_policies_partition",
                check_query=lambda: check_table_exists(conn, "policies_global", schema=schema),
            ).execute(conn, schema=schema)

        # 2. Maintenance (System Schema Only)
        if schema == "apikey":
            # 3. Proactive Maintenance (Usage Counters)
            await maintenance_tools.ensure_future_partitions(
                conn,
                schema=schema,
                table="usage_counters",
                interval="monthly",
                periods_ahead=2,
            )

            # 4. Register Partition Creation Policy (ongoing, via pg_cron)
            await maintenance_tools.register_partition_creation_policy(
                conn,
                schema=schema,
                table="usage_counters",
                interval="monthly",
                periods_ahead=2,
                schedule_cron="0 2 1 * *",
            )

            # 5. Register Retention Policy (prune old partitions)
            await maintenance_tools.register_retention_policy(
                conn,
                schema=schema,
                table="usage_counters",
                retention_period="24 months",
                schedule_cron="15 3 * * 0",
            )

            # 6. Register nightly pruning of expired rows in flat tables
            _prune_job_name = f"prune_expired_{schema}"
            _prune_func_name = f"prune_expired_rows_{schema}"

            async def _check_prune_job_exists():
                from dynastore.modules.db_config.locking_tools import check_cron_job_exists
                return await check_cron_job_exists(conn, _prune_job_name)

            _prune_ddl = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{_prune_func_name}"() RETURNS void AS $$
            BEGIN
                -- Expired + deactivated API keys
                DELETE FROM "{schema}".api_keys
                WHERE expires_at IS NOT NULL AND expires_at < NOW() AND is_active = FALSE;

                -- Expired refresh tokens
                DELETE FROM "{schema}".refresh_tokens WHERE expires_at < NOW();

                -- Expired OAuth2 authorisation codes (short-lived, ~10 min)
                DELETE FROM "{schema}".oauth_codes WHERE expires_at < NOW();

                -- Expired OAuth2 access/bearer tokens
                DELETE FROM "{schema}".oauth_tokens WHERE expires_at < NOW();
            END;
            $$ LANGUAGE plpgsql;

            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{_prune_job_name}') THEN
                    PERFORM cron.unschedule('{_prune_job_name}');
                END IF;
            END;
            $$;

            SELECT cron.schedule('{_prune_job_name}', '0 4 * * *',
                $CMD$SELECT "{schema}"."{_prune_func_name}"()$CMD$);
            """

            await DDLQuery(
                _prune_ddl,
                check_query=_check_prune_job_exists,
                lock_key=f"prune_expired_rows_{schema}",
            ).execute(conn)

        logger.info(f"PostgresApiKeyStorage initialization complete for '{schema}'.")

    async def initialize_partitions(
        self, conn: DbResource, for_date: datetime, schema: str = "apikey"
    ):
        await self._ensure_partition_for_timestamp(conn, for_date, schema=schema)

    async def _ensure_partition_for_timestamp(
        self, conn: DbResource, timestamp: datetime, schema: str = "apikey"
    ):
        """Proactive JIT Partition Creation for Usage Counters (Monthly)."""
        year, month = timestamp.year, timestamp.month
        partition_key = f"{schema}_{year}_{month:02d}"

        if partition_key in self._known_partitions:
            return

        start_of_month = timestamp.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        await ensure_partition_exists(
            conn,
            table_name="usage_counters",
            schema=schema,
            strategy="RANGE",
            partition_value=start_of_month,
            interval="monthly",
        )
        self._known_partitions.add(partition_key)

    async def list_keys(
        self,
        principal_id: Optional[UUID] = None,
        is_active: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        """Lists API keys with filtering."""
        async with managed_transaction(conn or self.engine) as db:
            rows = await DQLQuery(
                LIST_KEYS_FILTERED, result_handler=ResultHandler.ALL_DICTS
            ).execute(
                db,
                schema=schema,
                principal_id=principal_id,
                is_active=is_active,
                limit=limit,
                offset=offset,
            )
            return [ApiKey(**row) for row in rows]

    async def prune_stale_keys(
        self,
        retention_period: str = "30 days",
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> int:
        async with managed_transaction(conn or self.engine) as db:
            return await DELETE_STALE_KEYS.execute(
                db, schema=schema, retention=retention_period
            )

    # --- Standard CRUD Implementation ---

    async def create_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> Principal:
        async with managed_transaction(conn or self.engine) as db:
            # Map Pydantic model to DB columns. V2 Principal has new fields.
            # We treat 'identifier' as display_name if legacy, or use display_name if present.
            # V2 roles is already a List[str], which maps to JSONB.

            # Logic: If principal has provider AND subject_id, we use "provider:subject_id" as the identifier
            # for backward compatibility with systems that expect a single string identifier.
            # Otherwise we fall back to the explicit identifier attribute or the display_name.
            if principal.provider and principal.subject_id:
                identifier = f"{principal.provider}:{principal.subject_id}"
            else:
                identifier = getattr(principal, "identifier", principal.display_name)

            return await INSERT_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                identifier=identifier,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                metadata=json.dumps(getattr(principal, "metadata", {})),  # Compat
                roles=json.dumps(principal.roles),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
                policy=None,  # Legacy policy field
            )

    async def get_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_ID.execute(db, schema=schema, id=principal_id)

    async def update_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                identifier=getattr(principal, "identifier", principal.display_name),
                display_name=principal.display_name,
                metadata=json.dumps(getattr(principal, "metadata", {})),
                roles=json.dumps(principal.roles),
                policy=None,
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
            )

    async def get_principal_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTIFIER.execute(
                db, schema=schema, identifier=identifier
            )

    async def delete_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_PRINCIPAL.execute(db, schema=schema, id=principal_id)
            return count > 0

    async def create_api_key(
        self,
        key_data: ApiKeyCreate,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> tuple[ApiKey, str]:
        raw_key = f"sk_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:6]

        async with managed_transaction(conn or self.engine) as db:
            principal_uuid = key_data.principal_id
            if key_data.principal_identifier:
                principal_uuid = await GET_PRINCIPAL_ID_BY_IDENTIFIER.execute(
                    db, schema=schema, identifier=key_data.principal_identifier
                )
                if not principal_uuid:
                    raise PrincipalNotFoundError(
                        f"Principal '{key_data.principal_identifier}' not found."
                    )

            policy_json = key_data.policy.model_dump_json() if key_data.policy else None
            conditions_json = (
                json.dumps([c.model_dump() for c in key_data.conditions])
                if key_data.conditions
                else None
            )

            try:
                api_key = await INSERT_API_KEY.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    key_prefix=key_prefix,
                    principal_id=principal_uuid,
                    name=key_data.name,
                    note=key_data.note,
                    expires_at=key_data.expires_at,
                    policy=policy_json,
                    conditions=conditions_json,
                    max_usage=key_data.max_usage,
                    allowed_domains=json.dumps(key_data.allowed_domains)
                    if key_data.allowed_domains
                    else "[]",
                    referer_match=key_data.referer_match,
                    catalog_match=key_data.catalog_match,
                    collection_match=key_data.collection_match,
                )
                return api_key, raw_key
            except ForeignKeyViolationError as e:
                raise PrincipalNotFoundError(
                    f"Principal '{key_data.principal_id}' not found."
                ) from e

    async def delete_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_KEY.execute(db, schema=schema, key_hash=key_hash)
            return count > 0

    async def get_key_metadata(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[ApiKey]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_KEY_METADATA.execute(db, schema=schema, key_hash=key_hash)

    async def list_keys_for_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_KEYS_BY_PRINCIPAL.execute(
                db, schema=schema, principal_id=principal_id
            )

    async def get_principal_id_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[UUID]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_ID_BY_IDENTIFIER.execute(
                db, schema=schema, identifier=identifier
            )

    async def list_principals(
        self,
        offset: int,
        limit: int,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_PRINCIPALS.execute(
                db, schema=schema, offset=offset, limit=limit
            )

    async def increment_usage(
        self,
        key_hash: str,
        period_start: datetime,
        amount: int = 1,
        last_access: Optional[datetime] = None,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ):
        shard_id = random.randint(0, USAGE_SHARD_COUNT - 1)
        if not last_access:
            last_access = datetime.now(timezone.utc)

        async with managed_transaction(conn or self.engine) as db:
            if period_start.year == 1970:
                await INCREMENT_QUOTA.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    shard_id=shard_id,
                    amount=amount,
                    last_access=last_access,
                )
            else:
                await self._ensure_partition_for_timestamp(
                    db, period_start, schema=schema
                )
                await INCREMENT_USAGE.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    period_start=period_start,
                    shard_id=shard_id,
                    amount=amount,
                    last_access=last_access,
                )

    async def get_usage(
        self,
        key_hash: str,
        period_start: datetime,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> int:
        async with managed_transaction(conn or self.engine) as db:
            if period_start.year == 1970:
                count = await CHECK_QUOTA.execute(db, schema=schema, key_hash=key_hash)
            else:
                count = await CHECK_USAGE.execute(
                    db, schema=schema, key_hash=key_hash, period_start=period_start
                )
            return int(count) if count else 0

    async def regenerate_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> tuple[Optional[ApiKey], Optional[str]]:
        async with managed_transaction(conn or self.engine) as db:
            old_key = await GET_KEY_METADATA.execute(
                db, schema=schema, key_hash=key_hash
            )
            if not old_key:
                return None, None

            raw_key = f"sk_{secrets.token_urlsafe(32)}"
            new_hash = hashlib.sha256(raw_key.encode()).hexdigest()
            new_prefix = raw_key[:6]

            await MIGRATE_USAGE.execute(
                db, schema=schema, old_hash=key_hash, new_hash=new_hash
            )
            await MIGRATE_QUOTA.execute(
                db, schema=schema, old_hash=key_hash, new_hash=new_hash
            )

            await DELETE_KEY.execute(db, schema=schema, key_hash=key_hash)

            policy_json = old_key.policy.model_dump_json() if old_key.policy else None

            new_key_obj = await INSERT_API_KEY.execute(
                db,
                schema=schema,
                key_hash=new_hash,
                key_prefix=new_prefix,
                principal_id=old_key.principal_id,
                name=old_key.name,
                note=old_key.note,
                expires_at=old_key.expires_at,
                policy=policy_json,
                max_usage=old_key.max_usage,
                allowed_domains=json.dumps(old_key.allowed_domains),
                referer_match=old_key.referer_match,
                catalog_match=old_key.catalog_match,
                collection_match=old_key.collection_match,
            )

            return new_key_obj, raw_key

    # --- Enhanced Methods ---

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[Principal]:
        query, params = build_search_principals_query(
            identifier, role, limit, offset, schema=schema
        )
        async with managed_transaction(conn or self.engine) as db:
            return await query.execute(db, **params)

    async def search_keys(
        self,
        principal_id: Optional[UUID] = None,
        status_filter: ApiKeyStatusFilter = ApiKeyStatusFilter.ALL,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        is_active_param = None
        if status_filter == ApiKeyStatusFilter.ACTIVE:
            is_active_param = True

        query, params = build_search_keys_query(
            principal_id, is_active_param, limit, offset, schema=schema
        )

        async with managed_transaction(conn or self.engine) as db:
            keys = await query.execute(db, **params)

            if status_filter == ApiKeyStatusFilter.EXPIRED:
                now = datetime.now(timezone.utc)
                return [k for k in keys if k.expires_at and k.expires_at < now]
            elif status_filter == ApiKeyStatusFilter.ACTIVE:
                now = datetime.now(timezone.utc)
                return [
                    k
                    for k in keys
                    if k.is_active and (k.expires_at is None or k.expires_at > now)
                ]

            return keys

    async def invalidate_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_KEY.execute(db, schema=schema, key_hash=key_hash)
            return result is not None

    # --- RBAC & Refresh Token SPI Implementation ---

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Role:
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_ROLE.execute(
                db,
                schema=schema,
                id=role.id,
                name=role.name,
                description=role.description,
                level=0,  # Legacy
                metadata=json.dumps(
                    role.dict(exclude={"parent_roles", "policies"})
                ),  # Store remaining as meta
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def get_role(
        self, name: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=name)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> List[Role]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_ROLE.execute(
                db,
                schema=schema,
                name=role.name,
                description=role.description,
                level=0,
                metadata=json.dumps(role.dict(exclude={"parent_roles", "policies"})),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def delete_role(
        self,
        name: str,
        cascade: bool = False,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            if cascade:
                # Remove from principals first
                await REMOVE_ROLE_FROM_PRINCIPALS.execute(
                    db, schema=schema, role_name=name
                )

            # The role_hierarchy has ON DELETE CASCADE in DDL, so children entries will be removed automatically.
            count = await DELETE_ROLE.execute(db, schema=schema, name=name)
            return count > 0

    async def add_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ):
        async with managed_transaction(conn or self.engine) as db:
            await INSERT_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )

    async def remove_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            return count > 0

    async def get_role_hierarchy(
        self,
        role_names: List[str],
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[str]:
        if not role_names:
            return []
        async with managed_transaction(conn or self.engine) as db:
            children = await GET_FULL_ROLE_HIERARCHY.execute(
                db, schema=schema, role_names=role_names
            )
            # Combine original roles with their descendants
            return list(set(role_names + children))

    async def create_refresh_token(
        self,
        token: RefreshToken,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> RefreshToken:
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_REFRESH_TOKEN.execute(
                db,
                schema=schema,
                id=token.id,
                key_hash=token.key_hash,
                principal_id=token.principal_id,
                api_key_hash=token.api_key_hash,
                expires_at=token.expires_at,
            )

    async def get_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[RefreshToken]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_REFRESH_TOKEN.execute(db, schema=schema, id=token_id)

    async def invalidate_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_REFRESH_TOKEN.execute(
                db, schema=schema, id=token_id
            )
            return result is not None

    async def run_maintenance(
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        logger.info(
            f"Running maintenance for PostgresApiKeyStorage (schema: {schema})..."
        )
        async with managed_transaction(conn or self.engine) as db:
            pruned_tokens = await PRUNE_EXPIRED_REFRESH_TOKENS.execute(
                db, schema=schema
            )
            pruned_keys = await PRUNE_EXPIRED_KEYS.execute(db, schema=schema)

            return {
                "pruned_refresh_tokens": pruned_tokens,
                "pruned_expired_inactive_keys": pruned_keys,
            }

    # --- SPI V2 Implementation ---

    async def get_principal_by_identity(
        self,
        provider: str,
        subject_id: str,
        schema: str,
        conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTITY.execute(
                db, schema=schema, provider=provider, subject_id=subject_id
            )

    async def create_principal_link(
        self,
        principal: Principal,
        identity: Dict[str, Any],
        schema: str,
        conn: Optional[DbResource] = None,
    ) -> Principal:
        async with managed_transaction(conn or self.engine) as db:
            # 1. Insert Principal
            p = await INSERT_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                roles=json.dumps(principal.roles),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
                identifier=getattr(
                    principal, "identifier", principal.display_name
                ),  # Compat
                metadata=json.dumps(getattr(principal, "metadata", {})),  # Compat
                policy=None,
            )

            # 2. Insert Link
            await INSERT_IDENTITY_LINK.execute(
                db,
                schema=schema,
                provider=identity.get("provider"),
                subject_id=identity.get("sub"),
                principal_id=principal.id,
                email=identity.get("email"),
            )
            return p

    async def get_effective_roles(
        self, principal_id: str, schema: str, conn: Optional[DbResource] = None
    ) -> List[str]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_EFFECTIVE_ROLES.execute(
                db, schema=schema, principal_id=principal_id
            )

    # --- Policy Management ---

    async def create_policy(
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Policy:
        """Creates a new policy."""
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_POLICY.execute(
                db,
                schema=schema,
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]),
                partition_key=policy.partition_key or "global",
            )

    async def get_policy(
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Policy]:
        """Retrieves a policy by ID."""
        async with managed_transaction(conn or self.engine) as db:
            return await GET_POLICY.execute(db, schema=schema, id=policy_id)

    async def list_policies(
        self,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[Policy]:
        """Lists all policies with pagination."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_POLICIES.execute(
                db, schema=schema, limit=limit, offset=offset
            )

    async def update_policy(
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Policy]:
        """Updates an existing policy."""
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_POLICY.execute(
                db,
                schema=schema,
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]),
            )

    async def delete_policy(
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        """Deletes a policy."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_POLICY.execute(db, schema=schema, id=policy_id)
            return count > 0

    # --- Identity Link Management ---

    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        """Creates an identity link for a principal."""
        async with managed_transaction(conn or self.engine) as db:
            count = await INSERT_IDENTITY_LINK.execute(
                db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                principal_id=principal_id,
                email=email,
            )
            return count > 0

    async def list_identity_links(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[IdentityLink]:
        """Lists all identity links for a principal."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_IDENTITY_LINKS.execute(
                db, schema=schema, principal_id=principal_id
            )

    async def delete_identity_link(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        """Deletes an identity link."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_IDENTITY_LINK.execute(
                db, schema=schema, provider=provider, subject_id=subject_id
            )
            return count > 0

    # --- Role Management (Enhanced) ---

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Role:
        """Creates a new role."""
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_ROLE.execute(
                db,
                schema=schema,
                id=role.id,
                name=role.name,
                description=role.description,
                level=0,  # Legacy support
                metadata=json.dumps(role.metadata),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def get_role(
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        """Retrieves a role by ID."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=role_id)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> List[Role]:
        """Lists all roles."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        """Updates an existing role."""
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_ROLE.execute(
                db,
                schema=schema,
                name=role.name,
                description=role.description,
                level=0,
                metadata=json.dumps(role.metadata),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def delete_role(
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        """Deletes a role."""
        async with managed_transaction(conn or self.engine) as db:
            await DELETE_ROLE.execute(db, schema=schema, name=role_id)
            return True

    # --- Local Authentication Methods ---

    async def create_local_user(
        self,
        user_id: UUID,
        username: str,
        password_hash: str,
        email: str,
        schema: str = "apikey",
    ) -> Dict[str, Any]:
        """Creates a new local user."""
        logger.info(f"Creating local user: {username} ({email}) in schema {schema}")
        async with managed_transaction(self.engine) as db:
            result = await INSERT_USER.execute(
                db,
                schema=schema,
                id=user_id,
                username=username,
                password_hash=password_hash,
                email=email,
                is_active=True,
            )
            logger.info(f"Created user result: {result}")
            return result

    async def get_local_user_by_username(
        self, username: str, schema: str = "apikey"
    ) -> Optional[Dict[str, Any]]:
        """Retrieves a local user by username."""
        async with managed_transaction(self.engine) as db:
            return await GET_USER_BY_USERNAME.execute(
                db, schema=schema, username=username
            )

    async def get_local_user_by_id(
        self, user_id: UUID, schema: str = "apikey"
    ) -> Optional[Dict[str, Any]]:
        """Retrieves a local user by ID."""
        async with managed_transaction(self.engine) as db:
            return await GET_USER_BY_ID.execute(db, schema=schema, id=user_id)

    async def create_oauth_code(
        self,
        code: str,
        user_id: UUID,
        redirect_uri: str,
        scope: Optional[str],
        expires_at: datetime,
        schema: str = "apikey",
    ) -> Dict[str, Any]:
        """Creates an OAuth2 authorization code."""
        async with managed_transaction(self.engine) as db:
            return await INSERT_OAUTH_CODE.execute(
                db,
                schema=schema,
                code=code,
                user_id=user_id,
                redirect_uri=redirect_uri,
                scope=scope,
                expires_at=expires_at,
            )

    async def get_oauth_code(
        self, code: str, schema: str = "apikey"
    ) -> Optional[Dict[str, Any]]:
        """Retrieves an OAuth2 authorization code."""
        async with managed_transaction(self.engine) as db:
            return await GET_OAUTH_CODE.execute(db, schema=schema, code=code)

    async def delete_oauth_code(self, code: str, schema: str = "apikey") -> bool:
        """Deletes an OAuth2 authorization code."""
        async with managed_transaction(self.engine) as db:
            await DELETE_OAUTH_CODE.execute(db, schema=schema, code=code)
            return True

    async def create_oauth_token(
        self,
        user_id: UUID,
        token_hash: str,
        token_type: str,
        expires_at: datetime,
        schema: str = "apikey",
    ) -> Dict[str, Any]:
        """Creates an OAuth2 token."""
        async with managed_transaction(self.engine) as db:
            return await INSERT_OAUTH_TOKEN.execute(
                db,
                schema=schema,
                user_id=user_id,
                token_hash=token_hash,
                token_type=token_type,
                expires_at=expires_at,
            )

    async def get_oauth_token_by_hash(
        self, token_hash: str, schema: str = "apikey"
    ) -> Optional[Dict[str, Any]]:
        """Retrieves an OAuth2 token by hash."""
        async with managed_transaction(self.engine) as db:
            return await GET_OAUTH_TOKEN_BY_HASH.execute(
                db, schema=schema, token_hash=token_hash, token_type="refresh"
            )

    async def delete_expired_oauth_tokens(self, schema: str = "apikey") -> int:
        """Deletes expired OAuth2 tokens."""
        async with managed_transaction(self.engine) as db:
            await DELETE_EXPIRED_OAUTH_TOKENS.execute(db, schema=schema)
            return 0  # TODO: Return count

    # --- Simplified IAG Methods (v2.1) ---

    async def resolve_identity(self, email: str) -> tuple[str, str]:
        """Resolve email to (provider, subject_id)."""
        logger.info(f"Resolving identity for email: {email}")
        async with managed_transaction(self.engine) as db:
            user = await GET_USER_BY_EMAIL.execute(db, schema="users", email=email)
            if user:
                logger.info(f"Found identity for {email}: {user['username']}")
                return ("local", user["username"])

            # Fallback: Check 'apikey' schema for legacy/dev environments
            user = await GET_USER_BY_EMAIL.execute(db, schema="apikey", email=email)
            if user:
                logger.info(
                    f"Found identity for {email} in apikey schema: {user['username']}"
                )
                return ("local", user["username"])

            logger.warning(f"No identity found for email: {email}")
            raise ValueError(f"No identity found for email: {email}")

    async def get_user_by_email(
        self, email: str, schema: str = "users"
    ) -> Optional[Dict[str, Any]]:
        """Get user by email address."""
        async with managed_transaction(self.engine) as db:
            result = await GET_USER_BY_EMAIL.execute(db, schema=schema, email=email)
            logger.info(f"get_user_by_email({email}, schema={schema}) -> {result}")
            return result

    async def get_identity_roles(
        self,
        provider: str,
        subject_id: str,
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """Get roles for an identity in a specific schema."""
        async with managed_transaction(conn or self.engine) as db:
            # Check if table exists to avoid noisy ERROR logs in PG
            if not await self.table_exists(schema, "identity_roles", conn=db):
                return []
            roles = await GET_IDENTITY_ROLES.execute(
                conn=db, schema=schema, provider=provider, subject_id=subject_id
            )
            return roles or []

    async def grant_roles(
        self,
        provider: str,
        subject_id: str,
        roles: List[str],
        schema: str = "apikey",
        granted_by: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> None:
        """Grant roles to an identity."""
        async with managed_transaction(conn or self.engine) as db:
            for role_name in roles:
                await INSERT_IDENTITY_ROLE.execute(
                    conn=db,
                    schema=schema,
                    provider=provider,
                    subject_id=subject_id,
                    role_name=role_name,
                    granted_by=granted_by,
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
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """Set authorization metadata for an identity."""
        async with managed_transaction(conn or self.engine) as db:
            return await UPSERT_IDENTITY_AUTHORIZATION.execute(
                db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                display_name=display_name,
                is_active=is_active,
                valid_from=valid_from or datetime.now(timezone.utc),
                valid_until=valid_until,
                attributes=json.dumps(attributes or {}),
            )

    async def revoke_role(
        self,
        provider: str,
        subject_id: str,
        role_name: str,
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> None:
        """Revoke a role from an identity."""
        async with managed_transaction(conn or self.engine) as db:
            await DELETE_IDENTITY_ROLE.execute(
                conn=db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                role_name=role_name,
            )

    async def get_identity_authorization(
        self,
        provider: str,
        subject_id: str,
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get authorization metadata for an identity (optional)."""
        async with managed_transaction(conn or self.engine) as db:
            # Check if table exists to avoid noisy ERROR logs in PG
            if not await self.table_exists(schema, "identity_authorization", conn=db):
                return None
            return await GET_IDENTITY_AUTHORIZATION.execute(
                conn=db, schema=schema, provider=provider, subject_id=subject_id
            )

    async def get_identity_policies(
        self,
        provider: str,
        subject_id: str,
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Get custom policies for an identity."""
        async with managed_transaction(conn or self.engine) as db:
            # Check if table exists to avoid noisy ERROR logs in PG
            if not await self.table_exists(schema, "identity_policies", conn=db):
                return []
            policies = await GET_IDENTITY_POLICIES.execute(
                conn=db, schema=schema, provider=provider, subject_id=subject_id
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
        schema: str = "apikey",
        conn: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """Add a custom policy to an identity."""
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_IDENTITY_POLICY.execute(
                conn=db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                policy_name=policy_name,
                actions=actions,
                resources=resources,
                effect=effect,
                conditions=json.dumps(conditions or []),
            )

    async def get_catalogs_for_identity(
        self, provider: str, subject_id: str
    ) -> List[str]:
        """Get list of catalog codes where identity has any roles in any schema."""
        # This query finds all schemas (physical_schema) where an identity_roles table 
        # exists and contains at least one record for the given identity.
        # It excludes the base 'apikey' schema as we want actual catalog schemas.
        query = """
        SELECT DISTINCT c.id
        FROM catalog.catalogs c
        JOIN information_schema.tables t ON c.physical_schema = t.table_schema
        WHERE t.table_name = 'identity_roles'
        AND EXISTS (
            SELECT 1 
            FROM information_schema.columns col 
            WHERE col.table_schema = c.physical_schema 
            AND col.table_name = 'identity_roles' 
            AND col.column_name = 'subject_id'
        )
        AND (
            SELECT count(*) > 0 
            FROM (
                SELECT 1 FROM pg_catalog.pg_namespace n
                JOIN pg_catalog.pg_class c_idx ON n.oid = c_idx.relnamespace
                WHERE n.nspname = c.physical_schema AND c_idx.relname = 'identity_roles'
            ) exists_check
        )
        """
        # The above complex SQL is to safely check across schemas. 
        # However, a simpler approach is to iterate over known schemas if the above is too slow or complex.
        # Let's use a more direct catalog-driven approach.
        
        async with managed_transaction(self.engine) as db:
            # 1. Get all active catalogs and their physical schemas
            catalogs_res = await DQLQuery(
                "SELECT id, physical_schema FROM catalog.catalogs WHERE deleted_at IS NULL;",
                result_handler=ResultHandler.ALL_DICTS
            ).execute(conn=db)
            
            if not catalogs_res:
                return []

            matching_catalogs = []
            logger.info(f"get_catalogs_for_identity: Searching {len(catalogs_res)} catalogs for {provider}:{subject_id}")
            
            # Step 1: Find all catalogs that have the identity_roles table
            # We do this in one shot to avoid noisy logs and improve performance
            schema_names = [cat["physical_schema"] for cat in catalogs_res]
            if not schema_names:
                return []

            # Check which of these schemas actually have the table
            # Using pg_catalog for speed and accuracy
            valid_schemas_query = DQLQuery(
                """
                SELECT n.nspname
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(:schemas)
                AND c.relname = 'identity_roles'
                AND c.relkind IN ('r', 'v', 'm', 'f');
                """,
                result_handler=ResultHandler.ALL_SCALARS
            )
            
            valid_schemas = set(await valid_schemas_query.execute(conn=db, schemas=schema_names))

            # Step 2: Query only the valid ones
            for cat in catalogs_res:
                cat_id = cat["id"]
                schema = cat["physical_schema"]
                
                if schema not in valid_schemas:
                    continue

                try:
                    # Even if it exists, use a nested transaction to be safe
                    # against concurrent drops (rare but possible in tests)
                    async with managed_nested_transaction(db) as nested:
                        exists = await DQLQuery(
                            f"SELECT 1 FROM {schema}.identity_roles WHERE provider = :provider AND subject_id = :subject_id LIMIT 1;",
                            result_handler=ResultHandler.SCALAR_ONE_OR_NONE
                        ).execute(conn=nested, provider=provider, subject_id=subject_id)
                        
                        if exists:
                            matching_catalogs.append(cat_id)
                except Exception as e:
                    logger.debug(f"get_catalogs_for_identity: skipping catalog {cat_id} (schema {schema}): {e}")
                    continue
                    
            return matching_catalogs

    async def get_catalog_users(self, schema: str = "apikey") -> List[Dict[str, Any]]:
        """Get all users with access to a catalog."""
        async with managed_transaction(self.engine) as db:
            return await GET_CATALOG_USERS.execute(conn=db, schema=schema)

    async def table_exists(self, schema: str, table_name: str, conn: Optional[DbResource] = None) -> bool:
        """Checks if a table exists in a specific schema."""
        async with managed_transaction(conn or self.engine) as db:
            return await CHECK_TABLE_EXISTS.execute(db, schema=schema, table_name=table_name)
