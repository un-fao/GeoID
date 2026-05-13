"""Per-field mutability — Protocol layer for ``PluginConfig`` (#665 slice 4).

The actual marker types (``Mutable[T]`` / ``WriteOnce[T]`` /
``Immutable[T]`` / ``Computed[T]``) live in
``dynastore.modules.db_config.platform_config_service`` next to
``PluginConfig`` itself — that module already shipped ``Immutable`` and
``WriteOnce`` and this slice rounds out the set with ``Mutable`` and
``Computed``.

This module exposes:

- ``MutabilityIntrospectionProtocol`` — the contract the
  composed-config renderer depends on (per
  ``feedback_prefer_protocols``).
- ``mutability_map(cls)`` — standalone helper for non-``PluginConfig``
  classes that follow the marker convention but don't formally
  implement the Protocol.
- ``missing_markers(cls)`` — used by ``PluginConfig``'s enforcer to
  identify fields that need annotation.

Authors annotate every Pydantic field with exactly one marker::

    from dynastore.modules.db_config.platform_config_service import (
        Mutable, WriteOnce, Immutable, Computed, PluginConfig,
    )

    class CollectionPostgresqlDriverConfig(CollectionDriverConfig):
        engine_ref:     WriteOnce[Optional[str]] = Field(default=None, ...)
        physical_table: Immutable[str]           = Field(default="collection_stac", ...)
        sidecars:       Mutable[List[str]]       = Field(default_factory=list, ...)

Each marker contributes ``readOnly`` (where applicable) +
``x-mutability`` to the field's JSON Schema via its
``__get_pydantic_json_schema__`` hook.  ``mutability_map()`` produces
the same data in a flat ``{field: kind}`` map for ``_meta.mutability``
under ``?meta=field``.
"""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    Protocol,
    Type,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)


def _extract_kind(annotation: Any) -> str | None:
    """Return ``"mutable"`` / ``"write_once"`` / ``"immutable"`` / ``"computed"``
    if the type carries a mutability marker, else ``None``.

    Resolves both ``__class_getitem__`` markers (``Immutable[T]``
    expanding to ``Annotated[T, ImmutableMarker]`` — the existing
    pattern) and the bare instance form (``Annotated[T, MarkerInstance]``).
    """
    # Import lazily to avoid a load-order cycle with platform_config_service
    # (PluginConfig imports this module's Protocol; this helper imports
    # PluginConfig's markers).
    from dynastore.modules.db_config.platform_config_service import (
        ComputedMarker,
        ImmutableMarker,
        MutableMarker,
        WriteOnceMarker,
    )

    if get_origin(annotation) is None:
        return None
    metadata = tuple(get_args(annotation))[1:]
    for meta in metadata:
        for marker_cls, kind in (
            (MutableMarker, "mutable"),
            (WriteOnceMarker, "write_once"),
            (ImmutableMarker, "immutable"),
            (ComputedMarker, "computed"),
        ):
            if meta is marker_cls or (
                isinstance(meta, type) and issubclass(meta, marker_cls)
            ):
                return kind
    return None


def mutability_map(cls: Type[Any]) -> Dict[str, str]:
    """Build the ``{field_name: kind}`` map for a Pydantic model class.

    Only Pydantic ``model_fields`` are included — ClassVars, plain class
    attributes, and ``@computed_field`` outputs are skipped (they aren't
    stored fields and don't need a marker).
    """
    hints = get_type_hints(cls, include_extras=True)
    out: Dict[str, str] = {}
    model_fields = getattr(cls, "model_fields", {})
    for name in model_fields:
        kind = _extract_kind(hints.get(name))
        if kind is not None:
            out[name] = kind
    return out


def missing_markers(cls: Type[Any]) -> Iterable[str]:
    """Yield Pydantic field names with no mutability marker on the resolved type."""
    hints = get_type_hints(cls, include_extras=True)
    model_fields = getattr(cls, "model_fields", {})
    for name in model_fields:
        if _extract_kind(hints.get(name)) is None:
            yield name


@runtime_checkable
class MutabilityIntrospectionProtocol(Protocol):
    """Surface a PluginConfig class exposes for mutability introspection.

    Implemented by ``PluginConfig`` itself (via plumbing in
    ``platform_config_service.py``).  The renderer depends on this
    Protocol, not on the concrete class — keeps the configs extension
    framework-free per ``feedback_prefer_protocols``.
    """

    @classmethod
    def mutability_map(cls) -> Dict[str, str]:
        """``{field_name: 'mutable'|'write_once'|'immutable'|'computed'}``."""
        ...
