import logging
logger = logging.getLogger(__name__)

async def create_table_using_ddl(spanner_client, database):
    from google.cloud.spanner_admin_database_v1.types import spanner_database_admin


    request = spanner_database_admin.UpdateDatabaseDdlRequest(
        database=database.name,
        statements=[

            """
                CREATE SEQUENCE IF NOT EXISTS language_seq OPTIONS (sequence_kind = 'bit_reversed_positive')
            """,
            """
                CREATE TABLE IF NOT EXISTS language (
                    id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE language_seq)),
                    code STRING(10) NOT NULL,
                    name STRING(100) NOT NULL,
                    state INT64 NOT NULL
                ) PRIMARY KEY (id)
            """,

            """
                CREATE SEQUENCE IF NOT EXISTS term_seq OPTIONS (sequence_kind = 'bit_reversed_positive')
            """,

            """
                CREATE SEQUENCE if not exists  asset_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists  asset_edge_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists  asset_class_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists  asset_type_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE TABLE IF NOT EXISTS asset_class (
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_class_seq)),
                    class_code STRING(36) NOT NULL, -- rename
                    description STRING(2048)
                )
                PRIMARY KEY (id)
            """,

            """
                CREATE TABLE IF NOT EXISTS asset_type (
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_type_seq)),
                    type_code STRING(36) NOT NULL,
                    description STRING(2048)
                )
                PRIMARY KEY (id)
            """,
            """
                CREATE TABLE IF NOT EXISTS asset_workflow_state_type (
                    id INT64 NOT NULL,
                    name STRING(128) NOT NULL,        --  withdrawn   -> draft (deleteable) -> wip -> QAR -> released -> published
                    state INT64 NOT NULL
                )
                PRIMARY KEY (id)
            """,
            """
            CREATE SEQUENCE IF NOT EXISTS principal_type_seq OPTIONS (
              sequence_kind = 'bit_reversed_positive'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS principal_type (
              id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE principal_type_seq)),
              type_code STRING(50) NOT NULL,
              description STRING(2048)
            ) PRIMARY KEY(id)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_principal_type_code ON principal_type(type_code)
            """,

            """
            CREATE TABLE if not exists principal (
                -- RENAMED PK: uuid -> principal_uuid
                principal_uuid BYTES(16) NOT NULL,
                
                principal_id STRING(256) NOT NULL,  -- Reminder https://docs.cloud.google.com/iam/docs/principal-identifiers
                type_id INT64 NOT NULL, 
                
                display_name STRING(256),
                description STRING(MAX),
                creation_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                email STRING(256),
                project_id STRING(128),
                identity_pool_id STRING(256),
                identity_pool_provider_id STRING(256),
                federated_subject STRING(512),
                cluster_name STRING(256),
                namespace STRING(256),
                kubernetes_service_account_name STRING(256),
                state INT64 NOT NULL,
                
                metadata JSON, -- Additional metadata about the principal
                FOREIGN KEY(type_id) REFERENCES principal_type(id) ON DELETE NO ACTION
                
            ) PRIMARY KEY (principal_uuid)
            """,
            """
                CREATE INDEX if not exists idx_principals_type_id ON principal (type_id)
            """,
            """
                CREATE INDEX if not exists  idx_principals_email ON principal (email)
            """,
            """
                CREATE UNIQUE INDEX if not exists idx_unique_principal_id ON principal (principal_id)
            """,
            """
                CREATE TABLE IF NOT EXISTS asset_relation_type (
                    id INT64 NOT NULL,
                    relation_name STRING(128) NOT NULL,
                    description STRING(4096),
                    state INT64 NOT NULL
                )
                PRIMARY KEY (id)
            """,
            """
                CREATE UNIQUE INDEX if not exists idx_unique_asset_relation_type ON asset_relation_type(relation_name)
            """,
            """
                CREATE SEQUENCE if not exists asset_workflow_type_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE TABLE IF NOT EXISTS asset_workflow_type(
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_workflow_type_seq)),
                    name STRING(128),
                    description STRING(4096),
                    principal_uuid BYTES(16) NOT NULL,
                    creation_timestamp TIMESTAMP,
                    state INT64 NOT NULL,
                    FOREIGN KEY (principal_uuid) REFERENCES principal (principal_uuid) ON DELETE NO ACTION
                )
                PRIMARY KEY (id)
            """,
            
            """
                CREATE SEQUENCE if not exists asset_workflow_edge_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,

            """
                CREATE TABLE IF NOT EXISTS asset_workflow_edge (
                    -- RENAMED PK: id -> asset_workflow_edge_id
                    asset_workflow_edge_id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_workflow_edge_seq)),
                    
                    source_asset_workflow_state_type_id INT64 NOT NULL, 
                    target_asset_workflow_state_type_id INT64 NOT NULL,
                    asset_workflow_type_id INT64 NOT NULL,
                    state INT64 NOT NULL, 

                    FOREIGN KEY (source_asset_workflow_state_type_id) REFERENCES asset_workflow_state_type (id) ON DELETE NO ACTION, 
                    FOREIGN KEY (target_asset_workflow_state_type_id) REFERENCES asset_workflow_state_type (id) ON DELETE NO ACTION, 
                    FOREIGN KEY (asset_workflow_type_id) REFERENCES asset_workflow_type (id) ON DELETE CASCADE 
                )
                PRIMARY KEY (asset_workflow_edge_id)
            """,
            """
                CREATE UNIQUE INDEX if not exists idx_unique_asset_workflow_edge ON asset_workflow_edge(source_asset_workflow_state_type_id, target_asset_workflow_state_type_id, asset_workflow_type_id)
            """,
            """
                CREATE SEQUENCE if not exists asset_workflow_edge_attribute_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                -- INTERLEAVED
                CREATE TABLE IF NOT EXISTS asset_workflow_edge_attribute(
                    asset_workflow_edge_id INT64 NOT NULL, -- Match Parent PK
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_workflow_edge_attribute_seq)),
                    attribute_code STRING(1024) NOT NULL, 
                    attribute STRING(1024) NOT NULL
                    
                ) PRIMARY KEY(asset_workflow_edge_id, id),
                  INTERLEAVE IN PARENT asset_workflow_edge ON DELETE CASCADE
            """,
            """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_edge_attr_id_unique ON asset_workflow_edge_attribute(id)
            """,
            """
                CREATE INDEX if not exists idx_asset_workflow_edge_attribute_key_value_edge_id on asset_workflow_edge_attribute(attribute_code, attribute, asset_workflow_edge_id)
            """,

            """ 
            CREATE TABLE IF NOT EXISTS asset (
                -- RENAMED PK: uuid -> asset_uuid
                asset_uuid BYTES(16) NOT NULL,
                
                asset_class_id INT64 NOT NULL,
                asset_type_id INT64 NOT NULL,
                state INT64 NOT NULL, 
                change_timestamp TIMESTAMP,
                FOREIGN KEY (asset_class_id) REFERENCES asset_class (id) ON DELETE NO ACTION, 
                FOREIGN KEY (asset_type_id) REFERENCES asset_type (id) ON DELETE NO ACTION 
                )
                PRIMARY KEY (asset_uuid)
            """,
            """
                CREATE INDEX if not exists idx_asset_type_id on asset(asset_type_id)
            """,
            """
                CREATE INDEX if not exists idx_asset_class_id on asset(asset_class_id)
            """,
            """
                CREATE INDEX if not exists idx_asset_state on asset(state)
            """,
            
            """
                -- INTERLEAVED
                CREATE TABLE IF NOT EXISTS term (
                    asset_uuid BYTES(16) NOT NULL, -- Match Parent PK
                    
                    -- RENAMED PK: id -> term_id
                    term_id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE term_seq)),
                    
                    term_code STRING(128),
                    state INT64 NOT NULL
                    
                ) PRIMARY KEY (asset_uuid, term_id),
                  INTERLEAVE IN PARENT asset ON DELETE CASCADE
            """,
            """
                -- No longer necessary due to interleaving
                --CREATE INDEX if not exists idx_term_asset_uuid ON term(asset_uuid)
            """,
            """
                CREATE INDEX if not exists idx_term_code_asset_uuid ON term(term_code, asset_uuid)
            """,
            """
                -- Restore Global Uniqueness & Fast Point Lookups
                CREATE UNIQUE INDEX IF NOT EXISTS idx_term_global_id ON term(term_id)
            """,
            
            # --- REVERTED TO ORIGINAL MODEL (NOT INTERLEAVED) ---
            # --- MODIFIED: NOW INTERLEAVED FOR PERFORMANCE ---
            """
                CREATE TABLE IF NOT EXISTS term_translation (
                    -- Removed asset_uuid. Standard FK to Term.
                    -- RE-ADDED asset_uuid to support INTERLEAVE
                    asset_uuid BYTES(16) NOT NULL,
                    
                    term_id INT64 NOT NULL, 
                    language_id INT64 NOT NULL,
                    value STRING(MAX) NOT NULL,
                    FOREIGN KEY (language_id) REFERENCES language (id) ON DELETE NO ACTION 
                    -- FK to term: Works because we have a UNIQUE INDEX on term(term_id)
                    -- FOREIGN KEY (term_id) REFERENCES term(term_id) ON DELETE CASCADE
                ) PRIMARY KEY (asset_uuid, term_id, language_id),
                  INTERLEAVE IN PARENT term ON DELETE CASCADE
            """,
            """
                -- Restore logic: One translation per term per language (Globally)
                CREATE UNIQUE INDEX IF NOT EXISTS idx_term_translation_global_pk ON term_translation(term_id, language_id)
            """,
            
            """
                CREATE SEQUENCE if not exists asset_property_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                -- INTERLEAVED
                CREATE TABLE IF NOT EXISTS asset_property (
                    asset_uuid BYTES(16) NOT NULL, -- Match Parent PK
                    id INT64 DEFAULT(GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_property_seq)),
                    name STRING(255),
                    
                    -- essential for filtering: WHERE value_numeric > 100
                    value_string STRING(MAX),
                    value_numeric NUMERIC,
                    value_bool BOOL,
                    --value_timestamp TIMESTAMP,     --could be necessary for "EAV" pattern

                    body JSON NOT NULL,
                    type_code STRING(128) NOT NULL,
                    state INT64 NOT NULL,

                    --created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                    --updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true),

                )
                PRIMARY KEY(asset_uuid, id),
                INTERLEAVE IN PARENT asset ON DELETE CASCADE
            """,
            """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_property_id_unique ON asset_property(id)
            """,
            """   
                CREATE UNIQUE INDEX if not exists idx_asset_property_asset_uuid_name ON asset_property(asset_uuid, name), INTERLEAVE IN asset
            """,
            """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_property_global_name ON asset_property(name)
            """,
            """
                CREATE SEQUENCE if not exists asset_edge_attribute_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists asset_workflow_state_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists asset_workflow_config_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
            """
                CREATE SEQUENCE if not exists json_history_seq OPTIONS (
                sequence_kind='bit_reversed_positive'
                )
            """,
"""
                CREATE TABLE IF NOT EXISTS asset_workflow_config (
                    -- 1. Move Parent Key to top for Interleaving
                    asset_uuid BYTES(16) NOT NULL,

                    id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_workflow_config_seq)),
                    asset_workflow_type_id INT64 NOT NULL,
                    state INT64 NOT NULL,

                    -- FOREIGN KEY (asset_uuid) is handled by INTERLEAVE
                    FOREIGN KEY (asset_workflow_type_id) REFERENCES asset_workflow_type (id) ON DELETE NO ACTION
                )
                PRIMARY KEY (asset_uuid, id),
                INTERLEAVE IN PARENT asset ON DELETE CASCADE
            """,
            """
                -- Restore global uniqueness lookup for legacy logic
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_workflow_config_global_id ON asset_workflow_config(id)
            """,

            """
                CREATE TABLE IF NOT EXISTS asset_workflow_state (
                    -- 1. Move Parent Key to top and make NOT NULL
                    asset_uuid BYTES(16) NOT NULL, 

                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_workflow_state_seq)),
                    asset_workflow_type_id INT64 NOT NULL,
                    current_asset_workflow_state_type_id INT64 NOT NULL,
                    principal_uuid BYTES(16) NOT NULL,
                    reason STRING(MAX),
                    change_timestamp TIMESTAMP,

                    -- FOREIGN KEY (asset_uuid) is handled by INTERLEAVE
                    FOREIGN KEY (asset_workflow_type_id) REFERENCES asset_workflow_type (id) ON DELETE NO ACTION, 
                    FOREIGN KEY (current_asset_workflow_state_type_id) REFERENCES asset_workflow_state_type (id) ON DELETE NO ACTION, 
                    FOREIGN KEY (principal_uuid) REFERENCES principal (principal_uuid) ON DELETE NO ACTION
                )
                PRIMARY KEY (asset_uuid, id),
                INTERLEAVE IN PARENT asset ON DELETE CASCADE
            """,
            """
                -- Restore global uniqueness lookup for legacy logic
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_workflow_state_global_id ON asset_workflow_state(id)
            """,
            """
                CREATE TABLE IF NOT EXISTS property_change (
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE json_history_seq)),
                    asset_property_id INT64 NOT NULL,
                    principal_uuid BYTES(16) NOT NULL,
                    reason STRING(4096),
                    current_asset_workflow_state_id INT64 NOT NULL,
                    diff JSON NOT NULL,
                    change_timestamp TIMESTAMP,

                    FOREIGN KEY (principal_uuid) REFERENCES principal (principal_uuid) ON DELETE NO ACTION,  
                    FOREIGN KEY (current_asset_workflow_state_id) REFERENCES asset_workflow_state (id) ON DELETE CASCADE
                )
                PRIMARY KEY (id)
            """,
            """
                CREATE INDEX idx_property_change_property_id ON property_change(asset_property_id)
            """,
            """
                CREATE TABLE IF NOT EXISTS asset_edge (
                    -- RENAMED PK: id -> asset_edge_id
                    asset_edge_id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_edge_seq)),
                    
                    source_asset_uuid BYTES(16) NOT NULL,
                    target_asset_uuid BYTES(16) NOT NULL,
                    asset_relation_type_id INT64 NOT NULL,
                    state INT64 NOT NULL,
                    FOREIGN KEY (source_asset_uuid) REFERENCES asset (asset_uuid),
                    FOREIGN KEY (target_asset_uuid) REFERENCES asset (asset_uuid),
                    FOREIGN KEY (asset_relation_type_id) REFERENCES asset_relation_type (id) ON DELETE CASCADE
                    
                )PRIMARY KEY (asset_edge_id)
            """,
            """
                CREATE UNIQUE INDEX if not exists idx_unique_asset_edge ON asset_edge (source_asset_uuid, target_asset_uuid, asset_relation_type_id) 
            """,
            """
                CREATE INDEX if not exists idx_asset_edge_target_asset_uuid on asset_edge(target_asset_uuid)
            """,
            """
                -- INTERLEAVED
                CREATE TABLE IF NOT EXISTS asset_edge_attribute(
                    asset_edge_id INT64 NOT NULL, -- Match Parent PK
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_edge_attribute_seq)),
                    attribute_code STRING(1024) NOT NULL, 
                    attribute STRING(1024) NOT NULL
                    
                ) PRIMARY KEY(asset_edge_id, id),
                  INTERLEAVE IN PARENT asset_edge ON DELETE CASCADE
            """,
            """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_edge_attribute_id_unique ON asset_edge_attribute(id)
            """,

            """
                CREATE INDEX if not exists idx_asset_edge_attribute_key_value_edge_id on asset_edge_attribute(attribute_code, attribute, asset_edge_id)
            """,
            
            # --- AUTH TABLES (Original structure) ---
            
            """ CREATE SEQUENCE IF NOT EXISTS principal_edge_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS permission_edge_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS condition_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS context_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS permission_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS generic_permission_bit_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS asset_principal_link_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE SEQUENCE IF NOT EXISTS asset_principal_permission_seq OPTIONS (sequence_kind = 'bit_reversed_positive') """,
            """ CREATE TABLE IF NOT EXISTS context (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE context_seq)),
                -- The unique name (e.g., 'bucket', 'bigquery')
                name STRING(255) NOT NULL
                ) PRIMARY KEY(id) 
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_context_name ON context(name) """,
            """
            CREATE TABLE IF NOT EXISTS permission (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE permission_seq)),
                code STRING(255) NOT NULL,
                label STRING(2048) NOT NULL,
                description STRING(2048), -- Could be of type json later !!
                permission_type STRING(50) NOT NULL,
                --priority INT64 NOT NULL DEFAULT (200),
                definition JSON, -- deny or allow rules will also be stored here
                state INT64 NOT NULL,
                CHECK (permission_type IN ('ROLE', 'PERMISSION')) 
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_permission_code ON permission(code) """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_permission_label ON permission(label) """,
            """
                CREATE TABLE IF NOT EXISTS generic_permission_bit (
                    id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE generic_permission_bit_seq)),
                    
                    name STRING(255) NOT NULL,

                    -- The bitmask value. MUST be BIGINT to support up to 2^63
                    bit_value INT64 NOT NULL,
                    -- storing them as bits is not possible in Spanner

                    -- CONSTRAINT: Ensure that it is a power of two
                    context_id INT64,
                    state INT64 NOT NULL,
                    CHECK (bit_value > 0 AND (bit_value & (bit_value - 1)) = 0),
                    FOREIGN KEY (context_id) REFERENCES context(id) ON DELETE NO ACTION
                ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_generic_permission_bit_value_and_context ON generic_permission_bit(bit_value, context_id) """,
            """
                CREATE TABLE IF NOT EXISTS permission_bit_map (
                    permission_id INT64 NOT NULL,
                    generic_permission_bit_id INT64 NOT NULL,
                    FOREIGN KEY (permission_id) REFERENCES permission(id) ON DELETE CASCADE,
                    FOREIGN KEY (generic_permission_bit_id) REFERENCES generic_permission_bit(id) ON DELETE NO ACTION -- Deleting a bit might be dangerous
                ) PRIMARY KEY (permission_id, generic_permission_bit_id)""",
            """
            CREATE TABLE IF NOT EXISTS condition (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE condition_seq)),
                name STRING(256) NOT NULL,
                description STRING(2048),
                expression JSON NOT NULL,
                state INT64 NOT NULL
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_condition_name ON condition(name) """,
            # """
            #     CREATE SEQUENCE IF NOT EXISTS asset_permission_link_seq OPTIONS (sequence_kind = 'bit_reversed_positive')
            # """,
            # """
            #     CREATE TABLE IF NOT EXISTS asset_permission_link (
            #         id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_permission_link_seq)),
            #         asset_uuid BYTES(16) NOT NULL,
            #         permission_id INT64 NOT NULL,
            #         state INT64 NOT NULL,

            #         FOREIGN KEY(asset_uuid) REFERENCES asset(uuid) ON DELETE CASCADE,
            #         FOREIGN KEY(permission_id) REFERENCES permission(id) ON DELETE CASCADE
            #     ) PRIMARY KEY(id)
            # """,
            # """    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_asset_permission_link ON asset_permission_link(asset_uuid, permission_id)""",
            # """    CREATE INDEX IF NOT EXISTS idx_asset_permission_link_permission ON asset_permission_link(permission_id)""",
            """
            CREATE TABLE IF NOT EXISTS permission_edge (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE permission_edge_seq)),
                parent_permission_id INT64 NOT NULL,
                child_permission_id INT64 NOT NULL,
                --asset_uuid BYTES(16), -- link to the config defining the role/permission for asset-scoped permissions
                state INT64 NOT NULL,
                FOREIGN KEY (parent_permission_id) REFERENCES permission(id) ON DELETE CASCADE,
                FOREIGN KEY (child_permission_id) REFERENCES permission(id) ON DELETE CASCADE
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_permission_edge ON permission_edge(parent_permission_id, child_permission_id)""",
            """
            CREATE TABLE IF NOT EXISTS principal_edge (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE principal_edge_seq)),
                parent_principal_uuid BYTES(16) NOT NULL,
                child_principal_uuid BYTES(16) NOT NULL,
                state INT64 NOT NULL,
                condition_id INT64,
                FOREIGN KEY (parent_principal_uuid) REFERENCES principal(principal_uuid) ON DELETE CASCADE,
                FOREIGN KEY (child_principal_uuid) REFERENCES principal(principal_uuid) ON DELETE CASCADE,
                FOREIGN KEY (condition_id) REFERENCES condition(id) ON DELETE NO ACTION
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_principal_edge ON principal_edge(parent_principal_uuid, child_principal_uuid) """,
            """ CREATE INDEX IF NOT EXISTS idx_principal_edge_child_uuid ON principal_edge(child_principal_uuid) """,
            """
            CREATE TABLE IF NOT EXISTS asset_principal_link (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_principal_link_seq)),
                asset_uuid BYTES(16) NOT NULL,
                principal_uuid BYTES(16) NOT NULL,
                FOREIGN KEY(asset_uuid) REFERENCES asset(asset_uuid) ON DELETE CASCADE,
                FOREIGN KEY(principal_uuid) REFERENCES principal(principal_uuid) ON DELETE CASCADE
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_asset_principal_link ON asset_principal_link(asset_uuid, principal_uuid) """,
            """ CREATE INDEX IF NOT EXISTS idx_asset_principal_link_principal ON asset_principal_link(principal_uuid) """,
            """
            CREATE TABLE IF NOT EXISTS asset_principal_permission (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE asset_principal_permission_seq)),
                asset_principal_link_id INT64 NOT NULL,
                permission_id INT64 NOT NULL,
                action_type STRING(50) NOT NULL,
                condition_id INT64,
                FOREIGN KEY(asset_principal_link_id) REFERENCES asset_principal_link(id) ON DELETE CASCADE,
                FOREIGN KEY(permission_id) REFERENCES permission(id) ON DELETE CASCADE,
                FOREIGN KEY(condition_id) REFERENCES condition(id) ON DELETE NO ACTION,
                CHECK (action_type IN ('ALLOW', 'DENY')) 
            ) PRIMARY KEY(id)
            """,
            """ CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_link_permission ON asset_principal_permission(asset_principal_link_id, permission_id) """,
            """ CREATE INDEX IF NOT EXISTS idx_asset_principal_permission_permission_id ON asset_principal_permission(permission_id) """,

            # --- VIEW DEFINITIONS ---
            """
            CREATE OR REPLACE VIEW view_attributed_edges AS
             SELECT 
                attr.id AS view_key,
                
                -- 1. Attribute Data
                ANY_VALUE(attr.attribute_code) AS attribute_code,
                ANY_VALUE(attr.attribute) AS attribute,

                -- 2. Relation Data
                ANY_VALUE(e.asset_relation_type_id) AS asset_relation_type_id,
                ANY_VALUE(e.state) AS state,
                
                ANY_VALUE(e.asset_edge_id) AS original_edge_id, 

                -- 3. Topology
                ANY_VALUE(e.source_asset_uuid) AS source_asset_uuid,
                ANY_VALUE(e.target_asset_uuid) AS target_asset_uuid

            FROM asset_edge_attribute AS attr
            JOIN asset_edge AS e 
            ON attr.asset_edge_id = e.asset_edge_id
            GROUP BY attr.id
            """,
            # --- GRAPH DEFINITIONS ---
            """
            CREATE OR REPLACE PROPERTY GRAPH workflow_graph
            NODE TABLES (
                asset_workflow_state_type
                    KEY(id)
                    LABEL asset_workflow_state_type PROPERTIES(id, name, state)
            )
            EDGE TABLES (
                asset_workflow_edge
                    KEY(asset_workflow_edge_id)
                    SOURCE KEY(source_asset_workflow_state_type_id) REFERENCES asset_workflow_state_type(id)
                    DESTINATION KEY(target_asset_workflow_state_type_id) REFERENCES asset_workflow_state_type(id)
                    LABEL next_asset_workflow_step
                    PROPERTIES(asset_workflow_type_id)
                    )
            """,
            """
            CREATE OR REPLACE PROPERTY GRAPH asset_graph
                NODE TABLES (
                    asset
                        KEY (asset_uuid)
                        LABEL asset
                        PROPERTIES (asset_uuid, state, asset_type_id, asset_class_id)
                )
                EDGE TABLES (
                    -- 1. Standard Edge (Raw Topology)
                    asset_edge AS related_to
                        KEY (asset_edge_id)
                        SOURCE KEY (source_asset_uuid) REFERENCES asset (asset_uuid)
                        DESTINATION KEY (target_asset_uuid) REFERENCES asset (asset_uuid)
                        LABEL related_to
                        PROPERTIES (asset_edge_id, asset_relation_type_id, state),

                    -- 2. Attributed Edge (The Optimization View)
                    view_attributed_edges AS attributed_link
                        KEY (view_key)
                        SOURCE KEY (source_asset_uuid) REFERENCES asset (asset_uuid)
                        DESTINATION KEY (target_asset_uuid) REFERENCES asset (asset_uuid)
                        LABEL attributed_link
                        PROPERTIES (
                            attribute_code, 
                            attribute, 
                            asset_relation_type_id, 
                            state, 
                            original_edge_id
                        )
                )
            """,
            """
                CREATE INDEX IF NOT EXISTS idx_asset_edge_source_rel_target
                ON asset_edge (source_asset_uuid, target_asset_uuid)
                STORING (asset_relation_type_id)
            """,

            """
            CREATE OR REPLACE PROPERTY GRAPH principal_graph
                NODE TABLES(
                    principal
                        KEY(principal_uuid)
                        LABEL principal PROPERTIES(principal_id, type_id, display_name, email, state, principal_uuid)
                )
                EDGE TABLES(
                    principal_edge
                        KEY(id)
                        SOURCE KEY(parent_principal_uuid) REFERENCES principal(principal_uuid)
                        DESTINATION KEY(child_principal_uuid) REFERENCES principal(principal_uuid)
                        LABEL has_member PROPERTIES(id, state)
                )
            """,
            """
            CREATE OR REPLACE PROPERTY GRAPH permission_graph
                NODE TABLES(
                    permission
                        KEY(id)
                        LABEL permission PROPERTIES(id, code, label, permission_type, state)
                )
                EDGE TABLES(
                    permission_edge
                        KEY(id)
                        SOURCE KEY(parent_permission_id) REFERENCES permission(id)
                        DESTINATION KEY(child_permission_id) REFERENCES permission(id)
                        --LABEL includes PROPERTIES(asset_uuid, state)
                        LABEL includes PROPERTIES(state)
                )
            """            
        ],
    )
    
    logger.info("Updating database with new schema. This may take a few minutes...")
    operation = spanner_client.database_admin_api.update_database_ddl(request)
    result = operation.result()
    logger.info(f"Database schema updated successfully: {result}")