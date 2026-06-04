"""GdalOsgeoReader._iter_features — FID surfacing (#1820).

The reader's record-building loop is fully duck-typed over the OGR
``ds``/``layer``/``feature`` objects, so it can be exercised without the
system ``osgeo`` bindings by stubbing them in ``sys.modules`` before import.
This keeps the regression test runnable everywhere (including CI images that
lack the system libgdal Python bindings).

Regression under test: a shapefile/GeoPackage whose primary key is promoted
onto the OGR FID (``GetFIDColumn()`` reports a name) used to lose that value —
it is not part of ``GetLayerDefn()`` — so a collection declaring it as a
required field saw it as null and ingestion failed.
"""

from __future__ import annotations

import sys
import types

import pytest


@pytest.fixture()
def osgeo_reader_module(monkeypatch):
    """Import ``osgeo_reader`` with a stubbed ``osgeo`` package.

    The module hard-imports ``from osgeo import ogr, gdal`` and calls
    ``ogr.UseExceptions()`` / ``gdal.UseExceptions()`` at import time; supply
    minimal stubs so the import (and thus ``register_reader``) succeeds.
    """
    ogr_stub = types.ModuleType("osgeo.ogr")
    ogr_stub.UseExceptions = lambda: None  # type: ignore[attr-defined]
    gdal_stub = types.ModuleType("osgeo.gdal")
    gdal_stub.UseExceptions = lambda: None  # type: ignore[attr-defined]
    osgeo_pkg = types.ModuleType("osgeo")
    osgeo_pkg.ogr = ogr_stub  # type: ignore[attr-defined]
    osgeo_pkg.gdal = gdal_stub  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "osgeo", osgeo_pkg)
    monkeypatch.setitem(sys.modules, "osgeo.ogr", ogr_stub)
    monkeypatch.setitem(sys.modules, "osgeo.gdal", gdal_stub)
    # Force a fresh import so the stubbed osgeo is bound.
    monkeypatch.delitem(
        sys.modules,
        "dynastore.tasks.ingestion.readers.osgeo_reader",
        raising=False,
    )
    import importlib

    return importlib.import_module(
        "dynastore.tasks.ingestion.readers.osgeo_reader"
    )


class _FakeGeom:
    def ExportToJson(self):
        return '{"type": "Point", "coordinates": [0.0, 0.0]}'

    def ExportToWkb(self):
        return b"\x00"


class _FakeFieldDefn:
    def __init__(self, name):
        self._name = name

    def GetName(self):
        return self._name


class _FakeLayerDefn:
    def __init__(self, field_names):
        self._defns = [_FakeFieldDefn(n) for n in field_names]

    def GetFieldCount(self):
        return len(self._defns)

    def GetFieldDefn(self, i):
        return self._defns[i]


class _FakeFeat:
    def __init__(self, values, fid):
        self._values = values
        self._fid = fid

    def GetField(self, name):
        return self._values[name]

    def GetFID(self):
        return self._fid

    def GetGeometryRef(self):
        return _FakeGeom()


class _FakeLayer:
    def __init__(self, field_names, fid_column, feats):
        self._defn = _FakeLayerDefn(field_names)
        self._fid_column = fid_column
        self._feats = feats

    def ResetReading(self):
        pass

    def GetLayerDefn(self):
        return self._defn

    def GetFIDColumn(self):
        return self._fid_column

    def __iter__(self):
        return iter(self._feats)


class _FakeDS:
    def __init__(self, layers):
        self._layers = layers

    def GetLayerCount(self):
        return len(self._layers)

    def GetLayer(self, i):
        return self._layers[i]


def test_named_fid_column_surfaced_into_properties(osgeo_reader_module):
    """A promoted, named FID column lands in ``properties`` under its name."""
    layer = _FakeLayer(
        field_names=["name"],
        fid_column="FID",
        feats=[
            _FakeFeat({"name": "a"}, fid=0),  # FID == 0 must survive (not null)
            _FakeFeat({"name": "b"}, fid=1),
        ],
    )
    feats = list(osgeo_reader_module.GdalOsgeoReader._iter_features(_FakeDS([layer])))

    assert [f["properties"]["FID"] for f in feats] == [0, 1]
    assert [f["properties"]["name"] for f in feats] == ["a", "b"]


def test_anonymous_fid_is_not_injected(osgeo_reader_module):
    """An anonymous FID (empty ``GetFIDColumn()``) is left out of properties."""
    layer = _FakeLayer(
        field_names=["name"],
        fid_column="",
        feats=[_FakeFeat({"name": "a"}, fid=7)],
    )
    feats = list(osgeo_reader_module.GdalOsgeoReader._iter_features(_FakeDS([layer])))

    assert feats[0]["properties"] == {"name": "a"}


def test_existing_attribute_field_is_not_overwritten(osgeo_reader_module):
    """When the FID column name collides with a real attribute field, the
    attribute value wins — ``GetFID()`` does not clobber it."""
    layer = _FakeLayer(
        field_names=["FID"],
        fid_column="FID",
        feats=[_FakeFeat({"FID": 42}, fid=0)],
    )
    feats = list(osgeo_reader_module.GdalOsgeoReader._iter_features(_FakeDS([layer])))

    assert feats[0]["properties"]["FID"] == 42
