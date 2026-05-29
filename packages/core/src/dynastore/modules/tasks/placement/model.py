"""Placement entry + mode constants.

The platform-tier TaskPlacementConfig is added in a later change; this module
currently provides the entry value object and the three placement modes.
"""
from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field

OFF_LOAD = "off_load"   # external executor: Cloud Run job (cloud) / worker machine (onprem)
ASYNC = "async"         # in-process background
SYNC = "sync"           # in-process blocking
MODES = (OFF_LOAD, ASYNC, SYNC)


class PlacementEntry(BaseModel):
    """Where/how a single task runs."""
    consumers: List[str] = Field(default_factory=list)  # logical service names
    mode: str = OFF_LOAD
