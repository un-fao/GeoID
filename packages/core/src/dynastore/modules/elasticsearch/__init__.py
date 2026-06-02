from typing import TYPE_CHECKING

__all__ = [
    "ElasticsearchModule",
]

if TYPE_CHECKING:
    from .module import ElasticsearchModule


def __getattr__(name: str):
    # Lazy re-export. ElasticsearchModule pulls the opensearch client at import
    # time, so importing a dependency-light submodule of this package (e.g.
    # ``items_projection`` for the pure ``strip_reserved_members`` helper used by
    # the OGC Features extension) must NOT eagerly drag ``opensearchpy`` in via
    # this package ``__init__``. Resolve the module only on explicit attribute
    # access so packages without the elasticsearch extras can still import the
    # helper submodules.
    if name == "ElasticsearchModule":
        from .module import ElasticsearchModule

        return ElasticsearchModule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
