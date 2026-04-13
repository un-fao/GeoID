# Hierarchical OGC Dimensions as Navigable STAC Virtual Collections

**Audience**: STAC Children Extension and Datacube Extension maintainers.
**Status**: Draft — integration brief layered on the published
[ogc-dimensions](https://github.com/ccancellieri/ogc-dimensions) spec. The
dimensions spec itself is out of scope; this document proposes *how STAC
clients should surface hierarchical dimensions as navigable Collections*.

---

## 1. Problem for STAC

Several UN/FAO domains ship hierarchical code lists at a scale where the
"one Collection per node" pattern breaks:

| Scheme | Nodes | Depth |
|---|---|---|
| AGROVOC | ~40 000 concepts | 8+ |
| GAUL admin boundaries | ~50 000 units | 5 |
| FAOSTAT item codes | ~10 000 | 4 |
| WorldClim biogeographic regions | ~2 000 | 3 |

Materialising every node as a STAC Collection means thousands of JSON
documents clients cannot practically walk, and a static `rel="child"` graph
that can never be a view of live data. STAC Browser today has no hook for
such trees. This is the gap.

## 2. Ingredients already standardised

Three things already exist; they just do not talk.

1. **OGC Dimensions — hierarchical Building Block**
   `/dimensions/{id}/members/{code}/children`,
   `/dimensions/{id}/members/{code}/ancestors`,
   `?parent={code}` on the members listing, and `rel=parent|children|ancestors`
   on each member. Drives any source: in-memory tree, SKOS, admin registry,
   data-derived.
2. **STAC API — Children Extension**
   `rel=child` / `rel=parent` between Collections; Catalog-level walking.
3. **STAC Datacube Extension** (OGC Community Standard, Oct 2025)
   `cube:dimensions` on Collections and Items.

Today a hierarchical dimension's `/children` endpoint and a STAC Collection's
`rel=child` link describe the same relationship but belong to different graphs.
Clients must re-implement the bridge. This proposal closes that.

## 3. Integration pattern: virtual STAC Collections per hierarchy node

Every node in a hierarchical dimension surfaces as a **virtual** STAC
Collection computed on demand:

```
id            = "{source}:{node_code}"           e.g. "admin-boundaries:AFR"
title         = label (with i18n via STAC Language Extension)
rel=parent    -> the parent node's virtual Collection
                 (or the dimension's root Collection at the top)
rel=child     -> one link per direct child
rel=items     -> /items?parent={node_code}        ← same URL serves Records AND STAC
cube:dimensions[{dim_id}] -> per the Datacube Extension; values reference the
                              hierarchical dimension
extent        -> bbox + temporal interval aggregated lazily over descendant items
```

No materialisation. `/children` is resolved by the underlying provider;
extent is computed on demand; nothing is pre-written. AGROVOC-scale trees
work because only the requested slice is ever produced.

## 4. Pluggable provider model

A STAC server wiring this pattern should not hardwire the source of
hierarchy. The reference implementation (GeoID) uses a tiny protocol:

```python
class HierarchyProvider(Protocol):
    kind: ClassVar[str]
    async def roots(self, ctx, *, limit, offset, language) -> ChildrenPage: ...
    async def children(self, ctx, parent_code, *, limit, offset, language) -> ChildrenPage: ...
    async def ancestors(self, ctx, member_code, *, language) -> list[HierarchyNode]: ...
    async def extent(self, ctx, parent_code) -> HierarchyExtent: ...
    async def has_children(self, ctx, member_code) -> bool: ...
```

Concrete kinds shipped today:

| kind | Source | Typical use |
|---|---|---|
| `dimension-backed` | ogc-dimensions hierarchical provider | AGROVOC, GAUL, FAOSTAT |
| `data-derived` | SQL over a collection's rows | admin levels embedded in features |
| `static` | Embedded JSON tree in config | tiny controlled vocabularies |
| `external-skos` *(reserved)* | Remote SKOS endpoint | shared multilingual thesauri |

`HierarchyNode` is the uniform shape — `{code, label, labels, parent_code,
level, has_children, extra}` — deliberately isomorphic to the ogc-dimensions
`GeneratedMember` so `dimension-backed` is a pass-through. The virtual-
Collection renderer consumes `HierarchyProvider` only; new sources land as
new files behind the decorator-based registry.

## 5. Worked example — GAUL admin tree

Walk from world to district via live `ogc-dimensions` calls:

```
GET /stac/virtual/hierarchy/admin-boundaries/collections/admin-boundaries
  id: admin-boundaries:ROOT
  links:
    rel=child -> admin-boundaries:AFR
    rel=child -> admin-boundaries:EUR
    ...
    rel=items -> /items?parent=ROOT

GET /stac/virtual/hierarchy/admin-boundaries/collections/admin-boundaries?parent_value=AFR
  id: admin-boundaries:AFR
  links:
    rel=parent -> admin-boundaries:ROOT
    rel=child  -> admin-boundaries:KEN
    rel=child  -> admin-boundaries:TZA
    rel=items  -> /items?parent=AFR
  cube:dimensions:
    admin_boundaries:
      type: concept
      values_url: /dimensions/admin-boundaries/members?parent=AFR

GET /stac/virtual/hierarchy/admin-boundaries/collections/admin-boundaries?parent_value=KEN.42
  id: admin-boundaries:KEN.42
  links:
    rel=parent -> admin-boundaries:KEN
    rel=child  -> admin-boundaries:KEN.42.3
    rel=items  -> /items?parent=KEN.42
```

A standard STAC Browser pointed at the root catalog walks this tree with
**zero client changes** once Children-Extension conformance is declared.

## 6. Proposed STAC crosswalk

| ogc-dimensions rel | STAC Children rel | Meaning |
|---|---|---|
| `rel=parent` on member | `rel=parent` on Collection | Up one level |
| `rel=children` on member | — *(alias: the collection's own `rel=child` set)* | Enumerate subtree root |
| individual `rel=child` on listing | `rel=child` on Collection | Direct child |
| `rel=ancestors` on member | — *(derive: parent chain)* | Full chain to root |
| `parent={code}` query | `?parent_value={code}` on virtual Collection | Filter |

## 7. Concrete ask of the STAC community

1. **Datacube Extension** SHOULD recognise `provider.hierarchical: true`
   (ogc-dimensions) as a valid way to populate `cube:dimensions` values, and
   SHOULD endorse the virtual-Collection pattern in §3 as a conformant way
   to disseminate hierarchical dimensions at scale.
2. **Children Extension** SHOULD allow a server to advertise
   `rel="child"` Collections that are **virtual** (computed on request) so
   long as they honour the Collection schema and the `rel=parent` back-link.
3. A small example (AGROVOC or GAUL) SHOULD be added to the extension
   examples, so client authors can validate tree walking against a scaled
   tree.

## 8. Reference implementation

- **ogc-dimensions**: https://github.com/ccancellieri/ogc-dimensions
  (spec, JSON schemas, reference Python impl, paper, pinned git commit
  consumed by GeoID)
- **GeoID `HierarchyProvider`**:
  `src/dynastore/extensions/stac/hierarchy/` — protocol + registry + four
  provider kinds.
- **Live deployment**: FAO Agro-Informatics Platform review environment
  (hierarchical `admin-boundaries` dimension behind ogc-dimensions routes).
