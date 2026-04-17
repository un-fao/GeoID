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

from typing import Any, Dict, Optional
from uuid import UUID

from dynastore.modules.db_config.query_executor import DDLQuery, DDLBatch, DQLQuery, ResultHandler
from .models import Principal, Role, RefreshToken, IdentityLink, Policy

# --- Queries (IAM Tables) ---

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
    # ON CONFLICT on `identifier` (not `id`) because JIT auto-registration
    # generates a fresh UUID on every request — the logical key for
    # "is this user already known?" is the identifier. Keyed-on-id conflicts
    # would never fire for the repeat-auth case and instead surface the
    # duplicate-identifier unique-constraint as a 401.
    """
    INSERT INTO {schema}.principals
    (id, identifier, display_name, is_active, valid_until, roles, custom_policies, attributes, metadata, policy)
    VALUES
    (:id, :identifier, :display_name, :is_active, :valid_until, :roles, :custom_policies, :attributes, :metadata, :policy)
    ON CONFLICT (identifier) DO UPDATE SET
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

RESOLVE_IDENTITY_BY_EMAIL = DQLQuery(
    """
    SELECT l.provider, l.subject_id
    FROM {schema}.identity_links l
    LEFT JOIN {schema}.principals p ON p.id = l.principal_id
    WHERE l.email = :email
       OR p.identifier = :email
       OR p.display_name = :email
    ORDER BY l.created_at ASC
    LIMIT 1;
    """,
    result_handler=ResultHandler.ONE_DICT,
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
    "INSERT INTO {schema}.refresh_tokens (id, key_hash, principal_id, family_id, expires_at) VALUES (:id, :key_hash, :principal_id, :family_id, :expires_at) RETURNING *;",
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



def build_search_principals_query(
    identifier: Optional[str],
    role: Optional[str],
    limit: int,
    offset: int,
    schema: str = "iam",
):
    clauses = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

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
