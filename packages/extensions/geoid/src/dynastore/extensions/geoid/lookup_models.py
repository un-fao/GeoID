"""Re-export geoid-lookup DTOs from the search extension.

These shapes are part of the customer-facing contract. Keeping the import
indirection here makes a future move-into-geoid trivial.
"""
from dynastore.extensions.search.search_models import (  # noqa: F401
    GeoidCollection,
    GeoidResult,
    GeoidSearchBody,
)

__all__ = ["GeoidCollection", "GeoidResult", "GeoidSearchBody"]
