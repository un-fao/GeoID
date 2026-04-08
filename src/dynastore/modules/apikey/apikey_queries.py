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

from typing import Optional
from uuid import UUID

from dynastore.modules.db_config.query_executor import DDLQuery, DDLBatch, DQLQuery, ResultHandler
from .models import Principal, ApiKey, Role, RefreshToken, IdentityLink, Policy

USAGE_SHARD_COUNT = 16

# --- Queries (V2 + V1 Merged) ---

# V2: Principals (Enhanced)
CREATE_PRINCIPALS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.principals (
        id UUID PRIMARY KEY,
        identifier VARCHAR(512),
        display_name VARCHAR(255), -- V2
        is_active BOOLEAN DEFAULT TRUE,
        valid_from TIMESTAMPTZ DEFAULT NOW(),
        valid_until TIMESTAMPTZ,
        roles JSONB DEFAULT '[]'::jsonb,
        custom_policies JSONB DEFAULT '[]'::jsonb,
        attributes JSONB DEFAULT '{}'::jsonb,
        metadata JSONB DEFAULT '{}'::jsonb,
        policy JSONB,
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
        level INTEGER DEFAULT 0,
        parent_roles JSONB DEFAULT '[]'::jsonb, -- V2 Inheritance
        policies JSONB DEFAULT '[]'::jsonb, -- V2 Policy Links
        metadata JSONB DEFAULT '{}'::jsonb,
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
        family_id VARCHAR(128),
        is_active BOOLEAN DEFAULT TRUE,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family
        ON {schema}.refresh_tokens (family_id) WHERE family_id IS NOT NULL;
""")

# --- Audit Log Table ---

CREATE_AUDIT_LOG_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.audit_log (
        id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(64) NOT NULL,
        principal_id VARCHAR(255),
        ip_address VARCHAR(45),
        detail JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_audit_log_event_type
        ON {schema}.audit_log (event_type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_log_principal
        ON {schema}.audit_log (principal_id, created_at DESC)
        WHERE principal_id IS NOT NULL;
""")

INSERT_AUDIT_EVENT = DQLQuery(
    """INSERT INTO {schema}.audit_log (event_type, principal_id, ip_address, detail)
       VALUES (:event_type, :principal_id, :ip_address, CAST(:detail AS jsonb))
       RETURNING id;""",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)




# --- DML / DQL ---

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
    """SELECT p.*, l.provider, l.subject_id
    FROM {schema}.principals p
    LEFT JOIN {schema}.identity_links l ON p.id = l.principal_id
    WHERE p.id = :id;""",
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
    """SELECT p.*, l.provider, l.subject_id
    FROM {schema}.principals p
    LEFT JOIN {schema}.identity_links l ON p.id = l.principal_id
    ORDER BY p.created_at DESC LIMIT :limit OFFSET :offset;""",
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
    WHERE (CAST(:principal_id AS UUID) IS NULL OR principal_id = :principal_id)
      AND (CAST(:is_active AS BOOLEAN) IS NULL OR is_active = :is_active)
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
        WHERE r #>> '{}' != :role_name
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
            WHERE id = CAST(:principal_id AS uuid)
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
    "INSERT INTO {schema}.refresh_tokens (id, key_hash, principal_id, api_key_hash, family_id, expires_at) VALUES (:id, :key_hash, :principal_id, :api_key_hash, :family_id, :expires_at) RETURNING *;",
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

INVALIDATE_REFRESH_TOKEN_FAMILY = DQLQuery(
    "UPDATE {schema}.refresh_tokens SET is_active = FALSE WHERE family_id = :family_id AND is_active = TRUE RETURNING id;",
    result_handler=ResultHandler.ALL,
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
