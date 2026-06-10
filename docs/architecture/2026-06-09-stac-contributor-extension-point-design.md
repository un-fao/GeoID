# STAC Contributor Extension Point — Design

**Date:** 2026-06-09
**Status:** Approved (design), pending implementation plan
**Scope:** `packages/extensions/stac`, plus one neutral-model touch in `packages/core`

## Problem

STAC document generation hardcodes which extensions enrich a Catalog,
Collection, or Item. The STAC *language* extension is the clearest case:
`STAC_LANGUAGE_EXTENSION_URI` (and its `language` / `languages` top-level
fields) is injected by four separate, divergent code paths:

| Path | Location | What it does |
|------|----------|--------------|
| Root catalog | `stac_generator.create_root_catalog` (~L123) | appends URI; sets `language` |
| Catalog summary | `stac_generator.create_catalog_summary` (~L189) | appends URI; sets `language` + `languages` |
| Collection | `stac_generator.create_collection` (L426) | appends URI **only** — missing `language`/`languages` |
| Item | `stac_generator` item path (~L978) | calls `inject_stac_language_fields` |

The collection path is already inconsistent (URI without the fields).
Adding a second extension (e.g. styles) today means editing every
generator again. There is no extension point a new STAC extension can
register against.

A registry-based extension point already exists for **assets** and
**links** — `AssetContributor` / `LinkContributor` (see
`asset_factory.py`), with six live producers (maps, features, tiles,
joins, dwh, styles). It is the proven pattern in this codebase:
neutral payload dataclasses, `get_protocols()` discovery, `priority`
ordering, zero inter-module imports.

A *second*, parallel abstraction also exists — `StacExtensionProtocol`
(`stac_extension_protocol.py`) with `get_stac_extensions()` /
`add_assets_to_item()` / `add_assets_to_collection()`. It has **zero
implementers** (verified). The item render path calls it, but against an
always-empty registry, so it is a no-op. It is vestigial.

## Goals

1. Introduce a STAC-specific extension point — `StacContributor` — that a
   STAC extension registers against to declare extension URIs and merge
   top-level STAC fields onto a Catalog / Collection / Item.
2. Ship the **language** extension as the first `StacContributor`,
   collapsing the four divergent injection sites into one.
3. Route catalog / collection / item generation through the registry,
   removing every hardcoded `STAC_LANGUAGE_EXTENSION_URI` literal.
4. Retire the vestigial `StacExtensionProtocol` and its dead plumbing.

**Non-goals (explicit):** migrating the proj/raster/vector projection
logic, the `enabled_extensions` config merge, or shipping a second
extension (styles). Those become trivial follow-ups once the hook exists.

## Why `StacContributor`, not reuse `AssetContributor`

`AssetContributor` and `LinkContributor` are *consumer-agnostic*: STAC,
Records, Features, and Coverages all consume them. Their payloads are
assets and links. STAC-extension enrichment is **STAC-only** — it writes
the `stac_extensions` array and STAC top-level fields, concepts no other
protocol shares. Following the codebase's own precedent (`LinkContributor`
exists separately from `AssetContributor` precisely because its payload
shape differs), STAC enrichment gets its own sibling, named and located
to reflect that it belongs to STAC:

- Lives in `packages/extensions/stac/.../stac_contributor.py` (not in
  core `models/protocols/`, where the cross-protocol contributors live).
- Reuses the neutral `ResourceRef` from `models/protocols/asset_contrib`
  for the render reference, keeping the family consistent.

## Design

### 1. New protocol — `StacContributor`

`packages/extensions/stac/src/dynastore/extensions/stac/stac_contributor.py`

```python
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Tuple, runtime_checkable
from dynastore.models.protocols.asset_contrib import ResourceRef


@dataclass(frozen=True)
class StacContribution:
    """Neutral STAC enrichment: extension URIs to declare plus top-level
    STAC fields to merge onto the target document."""
    stac_extensions: Tuple[str, ...] = ()
    extra_fields: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class StacContributor(Protocol):
    """Producer of STAC extension enrichment for a Catalog/Collection/Item.

    Optional producer: if the module isn't loaded, get_protocols() returns
    an empty iteration and generation proceeds without the contribution.
    """
    priority: int

    def contribute_stac(self, ref: ResourceRef) -> Iterable[StacContribution]: ...
```

### 2. Render reference — add `lang` to `ResourceRef`

`ResourceRef` (core, `models/protocols/asset_contrib.py`) gains:

```python
lang: Optional[str] = None
```

It already carries render-locale-ish context (`style`). `lang` is
broadly useful to any localizing contributor and is the cleanest way to
pass the active language to `contribute_stac`. Available languages are
passed via the existing `extras` mapping
(`extras={"available_languages": [...]}`) — they are call-site data, not
a stable field. This is the single `packages/core` touch.

### 3. First implementer — `LanguageStacContributor`

In the stac extension package. Wraps the existing
`inject_stac_language_fields` so behavior is preserved:

```python
class LanguageStacContributor:
    priority = 10  # baseline, runs early

    def contribute_stac(self, ref: ResourceRef) -> Iterable[StacContribution]:
        available = ref.extras.get("available_languages") or []
        lang = ref.lang or "en"
        fields = _language_fields(available, lang)  # {"language": ..., "languages": ...}
        yield StacContribution(
            stac_extensions=(STAC_LANGUAGE_EXTENSION_URI,),
            extra_fields=fields,
        )
```

Registered by default at stac-extension module load, so
`get_protocols(StacContributor)` returns it out of the box. Language is a
**baseline** capability — always on, never gated by
`auto_render_extensions` (that config gates the projection logic, which
is out of scope here).

### 4. Consumer — `apply_stac_contributions` in `asset_factory.py`

A sibling to `add_dynamic_assets`, same file and pattern:

```python
def apply_stac_contributions(target: StacTarget | pystac.Catalog,
                             ref: ResourceRef) -> None:
    for c in sorted(get_protocols(StacContributor),
                    key=lambda c: getattr(c, "priority", 100)):
        for contribution in c.contribute_stac(ref):
            for uri in contribution.stac_extensions:
                if uri not in target.stac_extensions:
                    target.stac_extensions.append(uri)
            target.extra_fields.update(contribution.extra_fields)
```

Called from each generator, replacing the hardcoded language literal:

- `create_root_catalog` (L123/L130) → build `ResourceRef` (lang only),
  call `apply_stac_contributions`.
- `create_catalog_summary` (L189–197) → same.
- `create_collection` (L426) → seed `stac_extensions_to_add` without the
  language literal, then `apply_stac_contributions`. **Side benefit:**
  collections now emit `language`/`languages`, fixing the current gap.
- Item path (~L978) → replace the direct `inject_stac_language_fields`
  call with `apply_stac_contributions`.

`enabled_extensions` / datacube / projection conditionals at the
collection site stay exactly as-is.

### 5. Retire `StacExtensionProtocol` (behavior-neutral)

Every consumer reads the always-empty `get_protocols(StacExtensionProtocol)`
list, so removal changes no runtime behavior.

- **Delete** `stac_extension_protocol.py` (`StacExtensionProtocol`,
  `StacExtensionContext`).
- `metadata_helpers.py`:
  - Remove `filter_providers_by_short_names` + `_SHORT_NAME_URI_MARKERS`
    (gated the dead providers).
  - `prune_managed_content` / `prune_managed_content_sync`: drop the
    `providers` param and the always-empty `managed_keys`/
    `managed_extensions` computation (no external content was ever pruned).
  - `merge_stac_metadata`: drop the `providers` param and its
    `add_assets_to_item` / `get_stac_extensions` loops; replace the
    `context: StacExtensionContext` param with `lang: str` (the only field
    it used, for localizing external sidecar metadata).
- `stac_generator.py`: remove the import (L52), the
  `filter_providers_by_short_names` + `get_protocols(StacExtensionProtocol)`
  block (L1104–1107), and the `StacExtensionContext` construction (L1091);
  pass `lang` to `merge_stac_metadata`.
- `stac_items_sidecar.py` (L372/378) and
  `pg_sidecars/item_metadata.py` (L364/391): drop the
  `get_protocols(StacExtensionProtocol)` fetch and the `providers`
  argument to the prune helpers.
- `stac_config.py:281`: update the stale comment referencing the protocol.

`auto_render_extensions` (config, `stac_config.py:290`) is **kept** — it
is live at `stac_generator.py:585` for projection behavior.

## Data flow (after)

```
generator (catalog/collection/item)
  └─ build ResourceRef(lang=…, extras={"available_languages": …})
       └─ apply_stac_contributions(target, ref)
            └─ get_protocols(StacContributor)   # LanguageStacContributor (+ future styles)
                 └─ contribute_stac(ref) → StacContribution(uris, fields)
                      └─ merge onto target.stac_extensions + target.extra_fields
```

## Error handling

- A contributor that raises is logged and skipped (mirrors the
  `AssetContributor` loop's `try/except` in `asset_factory.py`), so one
  bad producer never fails document generation.
- URI de-duplication on merge keeps `apply_stac_contributions` idempotent.

## Testing

- **Unit (`StacContributor`):** `LanguageStacContributor.contribute_stac`
  yields the language URI + `language`/`languages` for a given
  `ResourceRef`; default registry returns it; `apply_stac_contributions`
  is idempotent (no duplicate URIs) and merges fields onto a bare
  Catalog/Collection/Item.
- **Regression:** existing STAC generator + endpoint snapshot tests stay
  green. The one intended diff — collections gaining `language`/
  `languages` — is asserted explicitly.
- **Retirement safety:** existing item/sidecar tests pass unchanged after
  the `providers` params are dropped (they exercised the empty-provider
  path already).

## Behavior-fidelity notes (flagged)

1. The language URI becomes consistently declared via the contributor.
   Today catalog/item add it conditionally (only when languages exist);
   collection/root add it always. Standardizing to "contributor active ⇒
   URI present" is a minor, intentional change — verified against
   snapshot tests.
2. Collections gaining `language`/`languages` is a net-new, spec-correct
   field set, not a regression.

## Follow-ups enabled (out of scope here)

- Styles (or any extension) ships as a second `StacContributor` — no
  generator edits.
- The projection/`enabled_extensions` logic can later migrate to
  contributors for full consistency.
