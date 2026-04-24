#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO

"""SourceReaderProtocol — the capability contract every reader implements."""

from __future__ import annotations

import logging
from typing import Any, ClassVar, ContextManager, Iterable, Tuple, Type

logger = logging.getLogger(__name__)


class SourceReaderProtocol:
    """Read features from a vector source identified by URI.

    Subclasses register via :func:`register_reader` (or the
    :class:`ReaderRegistry` directly).  ``can_read(uri)`` performs a
    cheap extension/scheme check; the registry picks the highest-priority
    reader whose check passes.

    The reader is opened as a context manager so cleanup of any
    underlying file handle / driver state is deterministic.
    """

    # Lower priority wins (matches existing SidecarRegistry convention).
    priority: ClassVar[int] = 100
    # File extensions this reader is preferred for.  Lower-cased,
    # leading-dot-included ('.parquet', '.shp', '.csv', '.zip').
    extensions: ClassVar[Tuple[str, ...]] = ()
    # Stable identifier surfaced in logs / errors.
    reader_id: ClassVar[str] = ""

    @classmethod
    def can_read(cls, uri: str) -> bool:
        """Cheap match — lowercased URI ends with one of ``cls.extensions``."""
        if not cls.extensions:
            return False
        u = uri.lower()
        return any(u.endswith(ext) for ext in cls.extensions)

    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        **opts: Any,
    ) -> ContextManager[Iterable[dict]]:
        """Open the source and return a ctx-mgr yielding dict-shaped records.

        Each record is a plain dict with at least a ``geometry`` key
        (GeoJSON-shaped or WKB-bytes — whatever the downstream
        ``prepare_record_for_upsert`` expects).  Implementations should
        also expose a ``len()``-able if cheap, for progress reporting.
        """
        raise NotImplementedError

    def feature_count(self, uri: str) -> int | None:
        """Best-effort feature count for progress reporting; ``None`` if
        unknown without a full scan.  Default skips the count."""
        return None


# ---------------------------------------------------------------------------
# Registry façade — re-exports the singleton's API at module level so the
# call site doesn't need to know about the registry class.
# ---------------------------------------------------------------------------


def register_reader(cls: Type[SourceReaderProtocol]) -> Type[SourceReaderProtocol]:
    """Decorator / function: register *cls* with :class:`ReaderRegistry`."""
    from .registry import ReaderRegistry
    ReaderRegistry.register(cls)
    return cls


def resolve_reader(uri: str) -> Type[SourceReaderProtocol]:
    """Return the highest-priority registered reader whose ``can_read(uri)``
    matches.  Raises :class:`LookupError` if no reader matches.
    """
    from .registry import ReaderRegistry
    return ReaderRegistry.resolve(uri)


def _to_vsigs(uri: str) -> str:
    """Translate ``gs://bucket/key`` → ``/vsigs/bucket/key`` for GDAL.

    Many of GDAL's drivers know ``/vsigs/``; fewer know ``gs://``
    natively.  Round-tripping here keeps every GDAL-backed reader
    consistent and lets a future S3 / Azure migration adjust ONE place.
    """
    if uri.startswith("gs://"):
        return "/vsigs/" + uri[len("gs://"):]
    return uri
