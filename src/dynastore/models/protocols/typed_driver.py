#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Typed driver / driver-config bind — pyright-checked pairing.

Drivers inherit from ``TypedDriver[ConfigClass]`` so the (driver, config)
pair is captured by the type system; ``__init_subclass__`` registers the
pair at import time.  Driver-config classes inherit from
:class:`_PluginDriverConfig` and let ``class_key()`` auto-derive from the
bound driver class — no per-class boilerplate, and the operator-facing
JSON wire key is byte-identical to the routing entry's ``driver_id``.

Enforcement guarantees
----------------------

- pyright checks the type bind on ``TypedDriver[ConfigClass]``.
- Renaming the driver class auto-updates the JSON ``class_key()`` since
  it's a reverse lookup, not a string literal.
- Renaming the Config class is invisible on the wire.
- An orphan ``_PluginDriverConfig`` (no driver class binds it) raises a
  loud ``RuntimeError`` the first time ``class_key()`` is invoked.
- Two drivers binding the same Config raise at the second
  ``__init_subclass__`` — the bind is one-to-one by construction.

Migration shape
---------------

::

    class CollectionPostgresqlDriverConfig(_PluginDriverConfig):
        schema: str = "public"
        sidecars: List[_PgMetadataSidecarConfig] = Field(default_factory=list)
        # NO class_key() override.

    class CollectionPostgresqlDriver(TypedDriver[CollectionPostgresqlDriverConfig]):
        async def upsert_metadata(self, ...): ...

The wire then publishes ``CollectionPostgresqlDriver`` as the JSON key for
both the routing entry's ``driver_id`` and the ``configs.storage.drivers.{key}``
lookup — single name, two places.
"""

from __future__ import annotations

from functools import cache
from typing import Any, ClassVar, Dict, Generic, Type, TypeVar, get_args, get_origin

from dynastore.modules.db_config.platform_config_service import PluginConfig

ConfigT = TypeVar("ConfigT", bound="_PluginDriverConfig")


# Reverse map: config class → driver class.  Populated by
# ``TypedDriver.__init_subclass__`` at import time.  Reads happen via
# ``_PluginDriverConfig.class_key()`` (single lookup per call, no caching
# needed — the resolved string is constant per class).
_DRIVER_REGISTRY: Dict[Type["_PluginDriverConfig"], Type["TypedDriver[Any]"]] = {}

# Abstract intermediate bases declare ``is_abstract_base = True`` (read via
# ``cls.__dict__.get("is_abstract_base", False)`` so concrete subclasses do
# not inherit it).  Replaces the legacy ``_ABSTRACT_BASE_NAMES`` frozenset —
# single source of truth lives on the classes themselves, not in a hand-
# maintained string set in two files.


def _is_abstract_base(cls: type) -> bool:
    """True iff ``cls`` itself declared ``is_abstract_base = True`` in its body."""
    return bool(cls.__dict__.get("is_abstract_base", False))


class TypedDriver(Generic[ConfigT]):
    """Base for every driver class whose config class is type-bound.

    The ``ConfigT`` type parameter is the single source of truth for the
    (driver, config) pair.  ``__init_subclass__`` extracts ``ConfigT`` from
    ``__orig_bases__`` and registers the pair in :data:`_DRIVER_REGISTRY` so
    the bound config's :meth:`_PluginDriverConfig.class_key` can derive the
    operator-facing wire key from the driver class name.

    Generic intermediates that don't bind a concrete ``ConfigT`` (e.g.
    abstract role mixins) are silently skipped — only concrete leaves
    register.
    """

    @classmethod
    @cache
    def config_cls(cls) -> Type[ConfigT]:
        """Return the bound ``ConfigT`` class.

        Walks ``__orig_bases__`` looking for ``TypedDriver[X]``.  Raises
        :class:`TypeError` for an abstract intermediate that hasn't bound
        a concrete config class.
        """
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is TypedDriver:
                args = get_args(base)
                if args:
                    return args[0]
        raise TypeError(
            f"{cls.__name__}: TypedDriver subclass must declare "
            f"`TypedDriver[ConcreteConfigClass]`; abstract intermediates "
            f"don't bind a config class.",
        )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Register ONLY when ``TypedDriver[X]`` appears directly in this
        # subclass's __orig_bases__ — not when the bind is inherited.  Lets a
        # specialisation (e.g. ``ItemsElasticsearchPrivateDriver(
        # ItemsElasticsearchDriver)``) share the parent's config without
        # colliding on the registry.
        direct_bind = None
        for base in cls.__dict__.get("__orig_bases__", ()):
            if get_origin(base) is TypedDriver:
                args = get_args(base)
                if args:
                    direct_bind = args[0]
                break
        if direct_bind is None:
            return
        existing = _DRIVER_REGISTRY.get(direct_bind)
        if existing is not None and existing is not cls:
            raise RuntimeError(
                f"TypedDriver bind conflict: {direct_bind.__name__} is already "
                f"bound to {existing.__name__}; cannot re-bind to {cls.__name__}. "
                "A config class can serve at most one driver class.",
            )
        _DRIVER_REGISTRY[direct_bind] = cls


class _PluginDriverConfig(PluginConfig):
    """Base for every driver's PluginConfig.

    ``class_key()`` is auto-derived from the driver class bound to this
    config via :class:`TypedDriver`'s registry — driver authors write zero
    boilerplate per pair.

    An orphan ``_PluginDriverConfig`` subclass (no bound driver) raises a
    loud :class:`RuntimeError` the first time ``class_key()`` is called,
    so a missing ``class FooDriver(TypedDriver[FooDriverConfig])``
    declaration surfaces immediately rather than silently leaking the
    raw config class name onto the wire.
    """

    # Marker for abstract intermediate base — hidden from the deep view and
    # from the publish/registry paths.  Concrete subclasses do NOT inherit
    # this (filter reads ``cls.__dict__.get("is_abstract_base", False)``).
    is_abstract_base: ClassVar[bool] = True

    # Sentinel: subclasses MUST NOT set ``_class_key`` — derivation is
    # via the bound driver class, not a string override.
    _class_key: ClassVar[None] = None

    @classmethod
    def class_key(cls) -> str:
        """Return the wire-published key — the bound driver class name when
        a :class:`TypedDriver` binds this config, else ``__qualname__``.

        The qualname fallback is necessary because ``PluginConfig.__init_subclass__``
        chains into ``TypedModelRegistry.register`` which calls ``class_key()``
        during class creation — BEFORE the paired ``class XDriver(TypedDriver[XConfig])``
        declaration in the driver module has had a chance to populate
        :data:`_DRIVER_REGISTRY`.  The fallback is class-loading mechanics,
        not an operator-facing legacy alias: by the time an operator-facing
        publisher calls ``class_key()`` (post-import), every concrete
        ``*DriverConfig`` is bound and returns the driver class name.

        Operator-facing publishers should call :meth:`assert_bound` to surface
        any orphan configs that escaped the binding.

        The intermediate bases (``_PluginDriverConfig``, ``DriverPluginConfig``,
        ``CollectionDriverConfig``, ``AssetDriverConfig``) always return
        ``__qualname__`` — they're abstract markers, never published.
        """
        if _is_abstract_base(cls):
            return cls.__qualname__
        driver_cls = _DRIVER_REGISTRY.get(cls)
        if driver_cls is None:
            return cls.__qualname__
        return driver_cls.__name__

    @classmethod
    def assert_bound(cls) -> None:
        """Raise :class:`RuntimeError` if no :class:`TypedDriver` binds this
        config class.  Operator-facing publish paths call this before
        relying on the wire key being the driver class name.
        """
        if _is_abstract_base(cls):
            return
        if cls not in _DRIVER_REGISTRY:
            raise RuntimeError(
                f"{cls.__name__}: no TypedDriver class binds this config. "
                f"Declare `class XDriver(TypedDriver[{cls.__name__}])` so "
                "the driver class name becomes the operator-facing wire key.",
            )


def _registered_pairs() -> Dict[Type["_PluginDriverConfig"], Type["TypedDriver[Any]"]]:
    """Read-only view of the (config_cls -> driver_cls) registry.

    Test/diagnostic affordance — production code uses
    :meth:`_PluginDriverConfig.class_key` directly.  Returns a defensive
    copy so callers can't mutate the registry.
    """
    return dict(_DRIVER_REGISTRY)
