"""Reader resolution falls back to content_type when URI suffix is unknown.

Reproduces the GCP review-env failure ``LookupError: No registered
reader matches URI 'gs://.../aoi_oasis'`` when an asset was uploaded
with a bare filename.  The fallback uses the asset's stored
``content_type`` so existing rows can be ingested without re-upload.
"""

from __future__ import annotations

from typing import Any, ClassVar, Iterable, Iterator, Tuple

import contextlib
import pytest

from dynastore.tasks.ingestion.readers.base import (
    SourceReaderProtocol,
    resolve_reader,
)
from dynastore.tasks.ingestion.readers.registry import ReaderRegistry


# --- Test-only readers (no heavy GDAL/fiona deps) ---


class _ZipShapeReader(SourceReaderProtocol):
    reader_id: ClassVar[str] = "zipshape_test"
    priority: ClassVar[int] = 10
    extensions: ClassVar[Tuple[str, ...]] = (".zip", ".shp")

    @contextlib.contextmanager
    def open(  # type: ignore[override]
        self, uri: str, *, encoding: str = "utf-8",
        content_type: str | None = None, **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        yield iter([])


class _GeoJsonReader(SourceReaderProtocol):
    reader_id: ClassVar[str] = "geojson_test"
    priority: ClassVar[int] = 50
    extensions: ClassVar[Tuple[str, ...]] = (".geojson", ".json")

    @contextlib.contextmanager
    def open(  # type: ignore[override]
        self, uri: str, *, encoding: str = "utf-8",
        content_type: str | None = None, **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        yield iter([])


@pytest.fixture
def isolated_registry():
    """Swap in a clean registry, restore on teardown."""
    saved = list(ReaderRegistry._registered)
    ReaderRegistry.clear()
    ReaderRegistry.register(_ZipShapeReader)
    ReaderRegistry.register(_GeoJsonReader)
    yield
    ReaderRegistry.clear()
    for cls in saved:
        ReaderRegistry.register(cls)


def test_uri_suffix_match_unchanged(isolated_registry):
    """A URI with a known suffix resolves without consulting content_type."""
    cls = resolve_reader("gs://bucket/path/to/file.zip")
    assert cls is _ZipShapeReader


def test_bare_uri_with_zip_content_type_resolves(isolated_registry):
    """Bare URI + content_type=application/zip → ZipShape reader.

    This is the original bug: gs://.../aoi_oasis was previously
    unreadable because no reader matched.  With content_type plumbing,
    the upload's MIME hint pulls in the right reader.
    """
    cls = resolve_reader(
        "gs://bucket/collections/x/aoi_oasis",
        content_type="application/zip",
    )
    assert cls is _ZipShapeReader


def test_bare_uri_with_geojson_content_type_resolves(isolated_registry):
    cls = resolve_reader(
        "gs://bucket/collections/x/aoi_oasis",
        content_type="application/geo+json",
    )
    assert cls is _GeoJsonReader


def test_bare_uri_no_content_type_raises_with_descriptive_error(isolated_registry):
    """No suffix, no MIME hint → LookupError citing the considered readers
    AND the content_type that was tried (None)."""
    with pytest.raises(LookupError) as ei:
        resolve_reader("gs://bucket/collections/x/aoi_oasis")
    msg = str(ei.value)
    assert "aoi_oasis" in msg
    assert "content_type=None" in msg
    assert "zipshape_test" in msg
    assert "geojson_test" in msg


def test_bare_uri_octet_stream_does_not_resolve(isolated_registry):
    """``application/octet-stream`` is too generic and must not match."""
    with pytest.raises(LookupError):
        resolve_reader(
            "gs://bucket/collections/x/aoi_oasis",
            content_type="application/octet-stream",
        )


def test_uri_suffix_wins_over_conflicting_content_type(isolated_registry):
    """If the URI already has a known suffix, content_type is ignored
    (priority ordering preserved — first matcher wins)."""
    cls = resolve_reader(
        "gs://bucket/path/to/file.geojson",
        content_type="application/zip",
    )
    assert cls is _GeoJsonReader


def test_describe_classmethod_used_in_error(isolated_registry):
    """Custom ``describe()`` overrides surface in the LookupError so
    GdalOsgeoReader's KNOWN_EXT (not its empty `extensions=()` tuple)
    is what operators see."""

    class _DescribingReader(SourceReaderProtocol):
        reader_id: ClassVar[str] = "describes_test"
        priority: ClassVar[int] = 5
        extensions: ClassVar[Tuple[str, ...]] = ()

        @classmethod
        def can_read(  # type: ignore[override]
            cls, uri: str, *, content_type: str | None = None,
        ) -> bool:
            return False

        @classmethod
        def describe(cls) -> str:
            return "describes_test(known_extensions=('.foo', '.bar'))"

    ReaderRegistry.register(_DescribingReader)
    with pytest.raises(LookupError) as ei:
        resolve_reader("gs://bucket/x/y")
    assert "known_extensions=('.foo', '.bar')" in str(ei.value)


# --- _to_gdal_uri zip-hint behaviour ---


def test_to_gdal_uri_explicit_is_zip_wraps_bare_uri():
    """``GdalOsgeoReader._to_gdal_uri`` honours ``is_zip=True`` so a
    bare-URI ZIP shapefile gets the ``/vsizip/`` wrapper even without
    the ``.zip`` suffix in the URI."""
    pytest.importorskip("osgeo")  # gdal isn't always installed in unit env
    from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader

    bare = "gs://bucket/collections/x/aoi_oasis"
    # Bare URI + is_zip=True → curly-brace form so GDAL doesn't try to
    # autodetect the archive extension.
    assert GdalOsgeoReader._to_gdal_uri(bare, is_zip=True) == \
        "/vsizip/{/vsigs/bucket/collections/x/aoi_oasis}/"

    # Default (is_zip=None) still keys off URI suffix.
    assert GdalOsgeoReader._to_gdal_uri(bare) == \
        "/vsigs/bucket/collections/x/aoi_oasis"
    # URI with .zip suffix uses the simple prefix form (autodetect works).
    assert GdalOsgeoReader._to_gdal_uri(bare + ".zip") == \
        "/vsizip//vsigs/bucket/collections/x/aoi_oasis.zip"
