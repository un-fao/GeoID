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

from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, DDLQuery
import os

from .models import ShortURL, URLAnalytics

PROXY_URL_HASH_PARTITIONS = int(os.getenv("PROXY_URL_HASH_PARTITIONS", "64"))
PROXY_ANALYTICS_HASH_PARTITIONS = int(os.getenv("PROXY_ANALYTICS_HASH_PARTITIONS", "16"))
PROXY_ANALYTICS_PARTITION_INTERVAL = os.getenv("PROXY_ANALYTICS_PARTITION_INTERVAL", "MONTHLY").upper()


# --- Schema Creation ---


CREATE_SHORT_URLS_TABLE = DDLQuery(
    sql_template="""
    CREATE TABLE IF NOT EXISTS {schema}.short_urls (
        id BIGINT NOT NULL DEFAULT nextval('{schema}.short_url_id_seq'),
        short_key VARCHAR(20) NOT NULL,
        long_url TEXT NOT NULL,
        collection_id VARCHAR(255) NOT NULL DEFAULT '_catalog_',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        comment TEXT,
        PRIMARY KEY (collection_id, short_key)
    ) PARTITION BY LIST (collection_id);
    """
)

CREATE_URL_ANALYTICS_TABLE = DDLQuery(
    sql_template="""
    CREATE TABLE IF NOT EXISTS {schema}.url_analytics (
        id BIGSERIAL,
        short_key_ref VARCHAR(20) NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ip_address VARCHAR(45),
        user_agent TEXT,
        referer TEXT,
        PRIMARY KEY (timestamp, id)
    ) PARTITION BY RANGE (timestamp);
    """
)

# NEW: Sharded Aggregates for Proxy
# Optimizes "Total Clicks" and "Clicks Per Day" queries
CREATE_PROXY_AGGREGATES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.proxy_hourly_aggregates (
        period_start TIMESTAMPTZ NOT NULL, -- Rounded to hour
        short_key_ref VARCHAR(20) NOT NULL,
        shard_id INT NOT NULL DEFAULT 0,
        click_count BIGINT DEFAULT 0,
        PRIMARY KEY (period_start, short_key_ref, shard_id)
    );
""")

# Composite index for analytics queries (reverse column order)
CREATE_PROXY_AGGREGATES_INDEX_KEY_PERIOD = DDLQuery("""
    CREATE INDEX IF NOT EXISTS idx_proxy_hourly_agg_key_period 
    ON {schema}.proxy_hourly_aggregates (short_key_ref, period_start DESC);
""")

# BRIN index for time-range filtering (minimal storage overhead)
CREATE_PROXY_AGGREGATES_INDEX_PERIOD_BRIN = DDLQuery("""
    CREATE INDEX IF NOT EXISTS idx_proxy_hourly_agg_period_brin 
    ON {schema}.proxy_hourly_aggregates USING BRIN (period_start) WITH (pages_per_range = 128);
""")

# --- Partition Management ---

# Removed create_short_urls_partitions as we now use LIST partitioning by collection_id
# and manage partitions via lifecycle hooks.


def create_analytics_partition_for_month(year: int, month: int, schema: str = "proxy"):
    partition_name = f"{{schema}}.url_analytics_{year}_{month:02d}"
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{next_year}-{next_month:02d}-01"

    queries = [DDLQuery(
        sql_template=f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {schema}.url_analytics
        FOR VALUES FROM ('{start_date}') TO ('{end_date}')
        PARTITION BY HASH (short_key_ref);
        """
    )]

    for i in range(PROXY_ANALYTICS_HASH_PARTITIONS):
        # Create hash partition
        queries.append(DDLQuery(
            sql_template=f"""
            CREATE TABLE IF NOT EXISTS {partition_name}_{i} PARTITION OF {partition_name}
            FOR VALUES WITH (MODULUS {PROXY_ANALYTICS_HASH_PARTITIONS}, REMAINDER {i});
            """
        ))
        # BRIN index on timestamp for efficient time-range queries (minimal storage)
        queries.append(DDLQuery(
            sql_template=f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_{year}_{month:02d}_{i}_ts_brin 
            ON {partition_name}_{i} USING BRIN (timestamp) WITH (pages_per_range = 128);
            """
        ))
        # Composite index for analytics queries (short_key + timestamp)
        queries.append(DDLQuery(
            sql_template=f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_{year}_{month:02d}_{i}_key_ts 
            ON {partition_name}_{i} (short_key_ref, timestamp DESC) 
            INCLUDE (ip_address, user_agent, referrer);
            """
        ))
    return queries


# --- Data Operations ---

CREATE_BASE62_FUNCTION = DDLQuery(
    sql_template="""
    CREATE OR REPLACE FUNCTION {schema}.base62(n BIGINT) RETURNS TEXT AS $$
    DECLARE
        chars TEXT[] := ARRAY['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];
        result TEXT := '';
        num NUMERIC;
        remainder_int INT;
    BEGIN
        IF n = 0 THEN RETURN '0'; END IF;
        num := n;
        WHILE num > 0 LOOP
            remainder_int := (num % 62)::INT;
            result := chars[remainder_int + 1] || result;
            num := trunc(num / 62);
        END LOOP;
        RETURN result;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """
)

CREATE_OBFUSCATE_FUNCTION = DDLQuery(
    sql_template="""
    CREATE OR REPLACE FUNCTION {schema}.obfuscate_id(n BIGINT) RETURNS BIGINT AS $$
    DECLARE
        PRIME_CONSTANT BIGINT := 1299709::BIGINT;
        POSITIVE_MASK BIGINT := 9223372036854775807::BIGINT;
    BEGIN
        RETURN (n # PRIME_CONSTANT) & POSITIVE_MASK;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """
)

CREATE_SHORT_URL_SEQUENCE = DDLQuery(
    sql_template="""CREATE SEQUENCE IF NOT EXISTS {schema}.short_url_id_seq;"""
)

INSERT_SHORT_URL_WITH_GENERATED_KEY = DQLQuery(
    sql_template="""
    WITH new_vals AS (
        SELECT
            nextval('proxy.short_url_id_seq') as id,
            proxy.base62(proxy.obfuscate_id(currval('proxy.short_url_id_seq'))) as s_key
    )
    INSERT INTO {schema}.short_urls (id, short_key, long_url, collection_id, comment)
    SELECT id, s_key, :long_url, :collection_id, :comment FROM new_vals
    RETURNING id, short_key, long_url, created_at, comment;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda d: ShortURL(**d) if d else None
)

INSERT_SHORT_URL_WITH_CUSTOM_KEY = DQLQuery(
    sql_template="""
    INSERT INTO {schema}.short_urls (short_key, long_url, collection_id, comment)
    VALUES (:custom_key, :long_url, :collection_id, :comment)
    RETURNING id, short_key, long_url, created_at, comment;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda d: ShortURL(**d) if d else None
)

GET_LONG_URL = DQLQuery(
    sql_template="SELECT long_url FROM {schema}.short_urls WHERE short_key = :short_key;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)



# Bulk Insert for Logs
INSERT_LOG_REDIRECT_BATCH = """
    INSERT INTO {schema}.url_analytics (short_key_ref, ip_address, user_agent, referrer, timestamp)
    VALUES 
"""

# Upsert for Aggregates
UPSERT_PROXY_AGGREGATE = DDLQuery("""
    INSERT INTO {schema}.proxy_hourly_aggregates 
    (period_start, short_key_ref, shard_id, click_count)
    VALUES 
    (:period, :short_key, :shard, :count)
    ON CONFLICT (period_start, short_key_ref, shard_id)
    DO UPDATE SET 
        click_count = {schema}.proxy_hourly_aggregates.click_count + EXCLUDED.click_count;
""")

GET_ANALYTICS = DQLQuery(
    sql_template="""
    SELECT id, short_key_ref, timestamp, ip_address, user_agent, referrer
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key AND id > :cursor
    ORDER BY id
    LIMIT :page_size;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [URLAnalytics(**r) for r in rows]
)

GET_ANALYTICS_INITIAL = DQLQuery(
    sql_template="""
    SELECT id, short_key_ref, timestamp, ip_address, user_agent, referrer
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key
    ORDER BY id
    LIMIT :page_size;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [URLAnalytics(**r) for r in rows]
)

GET_ANALYTICS_FILTERED = DQLQuery(
    sql_template="""
    SELECT id, short_key_ref, timestamp, ip_address, user_agent, referrer
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key AND id > :cursor AND timestamp BETWEEN :start_date AND :end_date
    ORDER BY id
    LIMIT :page_size;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [URLAnalytics(**r) for r in rows]
)

GET_ANALYTICS_INITIAL_FILTERED = DQLQuery(
    sql_template="""
    SELECT id, short_key_ref, timestamp, ip_address, user_agent, referrer
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key AND timestamp BETWEEN :start_date AND :end_date
    ORDER BY id
    LIMIT :page_size;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [URLAnalytics(**r) for r in rows]
)

DELETE_SHORT_URL = DQLQuery(
    sql_template="DELETE FROM {schema}.short_urls WHERE short_key = :short_key RETURNING short_key;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

# Optimized: Read from Aggregates
GET_ANALYTICS_TOTAL_CLICKS = DQLQuery(
    sql_template='SELECT COALESCE(SUM(click_count), 0) FROM {schema}.proxy_hourly_aggregates WHERE short_key_ref = :short_key;',
    result_handler=ResultHandler.SCALAR_ONE
)

# Optimized: Read from Aggregates
GET_ANALYTICS_CLICKS_PER_DAY = DQLQuery(
    sql_template="""
    SELECT date_trunc('day', period_start)::date AS day, SUM(click_count) AS clicks
    FROM {schema}.proxy_hourly_aggregates
    WHERE short_key_ref = :short_key
    GROUP BY day ORDER BY day;
    """,
    result_handler=ResultHandler.ALL_DICTS
)

# Keep raw for these as cardinality is too high for simple pre-aggregation
GET_ANALYTICS_TOP_REFERRERS = DQLQuery(
    sql_template="""
    SELECT referrer, COUNT(*) as clicks
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key AND referrer IS NOT NULL
    GROUP BY referrer
    ORDER BY clicks DESC
    LIMIT 5;
    """,
    result_handler=ResultHandler.ALL_DICTS
)

GET_ANALYTICS_TOP_USER_AGENTS = DQLQuery(
    sql_template="""
    SELECT user_agent, COUNT(*) as clicks
    FROM {schema}.url_analytics
    WHERE short_key_ref = :short_key AND user_agent IS NOT NULL
    GROUP BY user_agent
    ORDER BY clicks DESC
    LIMIT 5;
    """,
    result_handler=ResultHandler.ALL_DICTS
)
GET_URLS_BY_COLLECTION = DQLQuery(
    sql_template="""
    SELECT id, short_key, long_url, collection_id, created_at, comment
    FROM {schema}.short_urls
    WHERE collection_id = :collection_id
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [ShortURL(**r) for r in rows]
)
