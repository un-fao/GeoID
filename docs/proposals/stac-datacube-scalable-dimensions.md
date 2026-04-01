# Scalable Dimension Member Dissemination and Algorithmic Generation for STAC Datacube Extension

**Status**: Superseded -- see [ogc-dimensions](https://github.com/ccancellieri/ogc-dimensions) for the authoritative specification, paper, and JSON Schema.

---

## Specification

The full proposal -- JSON Schema, worked examples, conformance levels, and scientific paper -- lives at:

- **Repository**: https://github.com/ccancellieri/ogc-dimensions
- **Spec schemas**: `spec/schema/dimension.json`, `generator.json`, `hierarchy.json`
- **Worked examples**: `spec/examples/` (dekadal, pentadal, integer-range, admin-hierarchy, indicator-tree, legacy-bridge)

## Live Reference Implementation

The generator API is deployed on the FAO Agro-Informatics Platform review environment:

- **Dimension listing**: https://data.review.fao.org/geospatial/v2/api/tools/dimensions/
- **Swagger UI**: https://data.review.fao.org/geospatial/v2/api/tools/docs

Available dimensions demonstrate all conformance levels:

| Dimension | Generator type | Conformance |
|-----------|---------------|-------------|
| `temporal-dekadal` | `dekadal` | Basic, Invertible, Searchable |
| `temporal-pentadal-monthly` | `pentadal-monthly` | Basic, Invertible, Searchable |
| `temporal-pentadal-annual` | `pentadal-annual` | Basic, Invertible, Searchable |
| `elevation-bands` | `integer-range` | Basic, Invertible, Searchable |
| `indicator-tree` | `static-tree` | Basic, Hierarchical |
| `admin-boundaries` | `leveled-tree` | Basic, Hierarchical |
| `forestry-species` | `static-tree` | Basic, Hierarchical, Searchable |

## GeoID Integration

This Dynastore extension (`dynastore.extensions.dimensions`) wraps the `ogc-dimensions` pip package into an `ExtensionProtocol`. The use-case datasets are in `use_cases.py`; the extension registration is in `dimensions_extension.py`.
