"""
Elasticsearch singleton client — initialized once at lifespan startup.

All ES_* connection parameters are read directly from environment variables.
No Pydantic model; no per-request client creation. The single
AsyncElasticsearch instance manages its own connection pool.

Usage
-----
In ElasticsearchModule.lifespan:
    from dynastore.modules.elasticsearch import client as es_client
    await es_client.init()
    yield
    await es_client.close()

Everywhere else:
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    es = get_client()        # may be None when ES is not configured
    prefix = get_index_prefix()
"""
import logging
import os
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)

_client: Optional["AsyncElasticsearch"] = None
_index_prefix: str = "dynastore"


def get_client() -> Optional["AsyncElasticsearch"]:
    """Return the shared AsyncElasticsearch instance, or None if not initialized."""
    return _client


def get_index_prefix() -> str:
    return _index_prefix


def _build_client() -> "AsyncElasticsearch":
    """
    Build an AsyncElasticsearch from ES_* environment variables.

    Supported variables:
        ES_HOST           host name or IP   (default: localhost)
        ES_PORT           port              (default: 9200)
        ES_USE_SSL        true/false        (default: false)
        ES_VERIFY_CERTS   true/false        (default: true)
        ES_API_KEY        API key string    (preferred auth)
        ES_USERNAME       basic-auth user
        ES_PASSWORD       basic-auth password
    """
    try:
        from elasticsearch import AsyncElasticsearch
    except ImportError:
        raise RuntimeError(
            "elasticsearch package is not installed. "
            "Run: poetry add 'elasticsearch[async]'"
        )

    host = os.environ.get("ES_HOST", "localhost")
    port = int(os.environ.get("ES_PORT", "9200"))
    use_ssl = os.environ.get("ES_USE_SSL", "false").strip().lower() in ("1", "true", "yes")
    scheme = "https" if use_ssl else "http"
    verify_certs = os.environ.get("ES_VERIFY_CERTS", "true").strip().lower() in ("1", "true", "yes")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    kwargs = {
        "hosts": [f"{scheme}://{host}:{port}"],
        "verify_certs": verify_certs,
        # Connection pool — reused across all requests in this process
        "connections_per_node": int(os.environ.get("ES_CONNECTIONS_PER_NODE", "10")),
        "retry_on_timeout": True,
        "max_retries": 3,
    }

    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)

    return AsyncElasticsearch(**kwargs)


async def init(index_prefix: Optional[str] = None) -> None:
    """
    Initialize the singleton client.  Called once from ElasticsearchModule.lifespan.
    Safe to call when ES env vars are not set — client will be created but may
    fail at first use (lazy connection; does not block startup).
    """
    global _client, _index_prefix
    _index_prefix = index_prefix or os.environ.get("ES_INDEX_PREFIX", "dynastore")
    _client = _build_client()
    logger.info(
        "Elasticsearch client initialised (prefix=%r, ssl=%s).",
        _index_prefix,
        os.environ.get("ES_USE_SSL", "false"),
    )


async def close() -> None:
    """Gracefully close the singleton client.  Called from lifespan teardown."""
    global _client
    if _client is not None:
        try:
            await _client.close()
        except Exception as exc:
            logger.warning("Elasticsearch client close error: %s", exc)
        finally:
            _client = None
        logger.info("Elasticsearch client closed.")
