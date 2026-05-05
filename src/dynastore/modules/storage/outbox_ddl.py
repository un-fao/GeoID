"""storage_outbox + index_failure_log DDL.

Created at per-catalog schema bootstrap (alongside other tenant DDLs).
The outbox table carries an AFTER INSERT trigger that emits
``pg_notify('outbox_<driver_id>_<schema>', json)`` so per-driver-per-tenant
drain workers wake on commit instead of polling.

Both ``ensure_*`` helpers accept either a SQLAlchemy ``DbResource`` (the
production path — called from the catalog ``create_catalog`` outer
transaction) or a raw ``asyncpg.Connection`` (the test path — needed so
LISTEN / NOTIFY can be exercised against the same physical connection
without the SQLAlchemy proxy).
"""

from __future__ import annotations

from typing import Any

from dynastore.modules.db_config.query_executor import DDLQuery


STORAGE_OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS storage_outbox (
    op_id              UUID         PRIMARY KEY,
    driver_id          TEXT         NOT NULL,
    driver_instance_id TEXT         NOT NULL,
    collection_id      TEXT         NOT NULL,
    op                 TEXT         NOT NULL,
    item_id            TEXT,
    payload            JSONB        NOT NULL,
    idempotency_key    TEXT         NOT NULL,
    status             TEXT         NOT NULL DEFAULT 'ready',
    attempts           INT          NOT NULL DEFAULT 0,
    last_error         TEXT,
    ready_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at         TIMESTAMPTZ,
    claimed_by         TEXT,
    finished_at        TIMESTAMPTZ,
    CONSTRAINT storage_outbox_status_chk
        CHECK (status IN ('ready', 'in_flight', 'done', 'failed'))
);

CREATE INDEX IF NOT EXISTS storage_outbox_drain_idx
    ON storage_outbox (driver_id, status, ready_at)
    WHERE status IN ('ready', 'in_flight');

CREATE INDEX IF NOT EXISTS storage_outbox_done_idx
    ON storage_outbox (finished_at)
    WHERE status = 'done';

CREATE OR REPLACE FUNCTION storage_outbox_notify() RETURNS trigger AS $$
DECLARE
    cat TEXT := current_schema();
    payload TEXT;
BEGIN
    payload := json_build_object(
        'driver_id', NEW.driver_id,
        'catalog_id', cat,
        'op_id', NEW.op_id::text
    )::text;
    PERFORM pg_notify('outbox_' || NEW.driver_id || '_' || cat, payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS storage_outbox_notify_trg ON storage_outbox;
CREATE TRIGGER storage_outbox_notify_trg
    AFTER INSERT ON storage_outbox
    FOR EACH ROW EXECUTE FUNCTION storage_outbox_notify();
"""


INDEX_FAILURE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS index_failure_log (
    failure_id          UUID         PRIMARY KEY,
    occurred_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    collection_id       TEXT         NOT NULL,
    driver_id           TEXT         NOT NULL,
    driver_instance_id  TEXT         NOT NULL,
    op_id               UUID,
    item_id             TEXT,
    op                  TEXT         NOT NULL,
    attempts            INT          NOT NULL,
    error_class         TEXT         NOT NULL,
    error_message       TEXT         NOT NULL,
    status              TEXT         NOT NULL,
    correlation_id      TEXT,
    CONSTRAINT index_failure_log_status_chk
        CHECK (status IN ('retrying', 'failed'))
);

CREATE INDEX IF NOT EXISTS index_failure_log_browse_idx
    ON index_failure_log (collection_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS index_failure_log_status_idx
    ON index_failure_log (status, occurred_at DESC)
    WHERE status = 'failed';
"""


def _is_asyncpg_connection(conn: Any) -> bool:
    """Detect a raw ``asyncpg.Connection`` without a hard import.

    We avoid importing :mod:`asyncpg` at module top-level because this DDL
    file is imported during catalog bootstrap; matching by module path keeps
    the dependency surface narrow.
    """
    return type(conn).__module__.startswith("asyncpg")


async def _execute_raw_asyncpg_ddl(conn: Any, ddl: str) -> None:
    """Run multi-statement DDL on a raw asyncpg connection.

    asyncpg's ``Connection.execute`` accepts multi-statement scripts when no
    parameters are bound, which is the case for these idempotent CREATE
    blocks. The split_ddl helper used by ``DDLQuery`` is a SQLAlchemy-side
    workaround; here we hand the script to asyncpg as a single execute.
    """
    await conn.execute(ddl)


def _scoped(ddl: str, schema: str | None) -> str:
    """Wrap ``ddl`` so unqualified names resolve to ``schema``.

    The DDL templates intentionally use unqualified names + ``current_schema()``
    so the same source string works in two contexts:

    1. Tests / scripts that ``SET search_path`` themselves before calling
       ``ensure_*`` (``schema is None`` — pass through verbatim).
    2. Catalog bootstrap, which calls ``ensure_*`` with the per-catalog
       physical schema. There the outer transaction's search_path may still
       be the platform default, so we prepend ``SET LOCAL search_path``.
       This is transaction-local; once the bootstrap commits, normal
       ``search_path`` resolution resumes for subsequent inserts (which
       set their own search_path before writing).
    """
    if schema is None:
        return ddl
    # Quote-double the schema name to keep mixed-case / reserved-word safety.
    return f'SET LOCAL search_path TO "{schema}";\n{ddl}'


async def ensure_storage_outbox(conn: Any, schema: str | None = None) -> None:
    """Apply storage_outbox DDL (table + indexes + NOTIFY trigger).

    Idempotent. ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT
    EXISTS`` plus ``DROP TRIGGER IF EXISTS`` then ``CREATE TRIGGER`` makes
    the trigger re-creation safe across calls; ``CREATE OR REPLACE
    FUNCTION`` keeps the handler in lockstep with the latest definition.

    Pass ``schema`` to land the objects in a specific catalog schema; omit
    it to fall back to the connection's current ``search_path``.
    """
    ddl = _scoped(STORAGE_OUTBOX_DDL, schema)
    if _is_asyncpg_connection(conn):
        await _execute_raw_asyncpg_ddl(conn, ddl)
        return
    await DDLQuery(ddl).execute(conn)


async def ensure_index_failure_log(conn: Any, schema: str | None = None) -> None:
    """Apply index_failure_log DDL (table + browse / status indexes).

    Idempotent. See :func:`ensure_storage_outbox` for the ``schema``
    semantics.
    """
    ddl = _scoped(INDEX_FAILURE_LOG_DDL, schema)
    if _is_asyncpg_connection(conn):
        await _execute_raw_asyncpg_ddl(conn, ddl)
        return
    await DDLQuery(ddl).execute(conn)
