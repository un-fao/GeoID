#    Copyright 2025 FAO
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

# dynastore/extensions/stac/stac_validator.py

"""
Driver-agnostic STAC validation at write time.

Uses pystac's ``JsonSchemaSTACValidator`` for core + extension schema validation.
Falls back to stac-pydantic's ``validate_extensions()`` when available.
This module is called from the service layer before persisting to any storage driver
(PostgreSQL sidecar pipeline or Elasticsearch/SFEOS).
"""

import logging
from typing import Any, Dict, List, Optional

import pystac

logger = logging.getLogger(__name__)

# --- Capability detection ---

try:
    from pystac.validation import JsonSchemaSTACValidator

    _PYSTAC_VALIDATOR = JsonSchemaSTACValidator()
    PYSTAC_VALIDATION_AVAILABLE = True
except ImportError:
    _PYSTAC_VALIDATOR = None
    PYSTAC_VALIDATION_AVAILABLE = False

try:
    from stac_pydantic import Item as StacPydanticItem  # type: ignore[attr-defined]
    from stac_pydantic import Collection as StacPydanticCollection  # type: ignore[attr-defined]
    from stac_pydantic.extensions import validate_extensions as _sp_validate_extensions

    STAC_PYDANTIC_AVAILABLE = True
except ImportError:
    STAC_PYDANTIC_AVAILABLE = False


class STACValidationError(Exception):
    """Raised when a STAC document fails validation."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        self.errors = errors or []
        super().__init__(message)


def validate_stac_item(
    item_dict: Dict[str, Any],
    *,
    strict: bool = False,
) -> List[str]:
    """
    Validate a STAC Item dictionary against core spec and declared extensions.

    Parameters
    ----------
    item_dict : dict
        A complete STAC Item as a JSON-serialisable dict.
    strict : bool
        If ``True``, raise :class:`STACValidationError` on failure.
        If ``False`` (default), return a list of warning strings.

    Returns
    -------
    list[str]
        Empty list on success; list of validation messages otherwise.
    """
    warnings: List[str] = []

    # 1. pystac core + extension JSON-Schema validation
    if PYSTAC_VALIDATION_AVAILABLE and _PYSTAC_VALIDATOR is not None:
        try:
            stac_item = pystac.Item.from_dict(item_dict)
            stac_item.validate()
        except Exception as exc:
            msg = f"pystac validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    # 2. stac-pydantic model + extension-schema validation (when available)
    if STAC_PYDANTIC_AVAILABLE:
        try:
            sp_item = StacPydanticItem(**item_dict)
            _sp_validate_extensions(sp_item)
        except Exception as exc:
            msg = f"stac-pydantic validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    return warnings


def validate_stac_collection(
    collection_dict: Dict[str, Any],
    *,
    strict: bool = False,
) -> List[str]:
    """
    Validate a STAC Collection dictionary against core spec and declared extensions.

    Parameters
    ----------
    collection_dict : dict
        A complete STAC Collection as a JSON-serialisable dict.
    strict : bool
        If ``True``, raise :class:`STACValidationError` on failure.
        If ``False`` (default), return a list of warning strings.

    Returns
    -------
    list[str]
        Empty list on success; list of validation messages otherwise.
    """
    warnings: List[str] = []

    # 1. pystac core + extension JSON-Schema validation
    if PYSTAC_VALIDATION_AVAILABLE and _PYSTAC_VALIDATOR is not None:
        try:
            stac_coll = pystac.Collection.from_dict(collection_dict)
            stac_coll.validate()
        except Exception as exc:
            msg = f"pystac validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    # 2. stac-pydantic model + extension-schema validation (when available)
    if STAC_PYDANTIC_AVAILABLE:
        try:
            sp_coll = StacPydanticCollection(**collection_dict)
            _sp_validate_extensions(sp_coll)
        except Exception as exc:
            msg = f"stac-pydantic validation: {exc}"
            if strict:
                raise STACValidationError(msg, errors=[msg]) from exc
            warnings.append(msg)
            logger.warning(msg)

    return warnings
