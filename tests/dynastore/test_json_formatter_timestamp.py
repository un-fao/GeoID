"""``_JsonFormatter.formatTime`` must emit real sub-second precision.

``logging.Formatter.formatTime`` delegates to ``time.strftime``, which has no
microsecond directive — a ``%f`` in the datefmt is emitted *literally*, so log
lines carried a useless ``"timestamp": "...T10:55:26.%f"``. The override builds
the timestamp from ``datetime`` so ``%f`` expands to the actual microseconds.
"""
from __future__ import annotations

import logging
import re

from dynastore.main import _JsonFormatter


def _record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="hello", args=(), exc_info=None,
    )


def test_format_time_expands_microseconds():
    formatted = _JsonFormatter().formatTime(_record(), "%Y-%m-%dT%H:%M:%S.%f")
    # No literal "%f" left behind; ends in 6 microsecond digits.
    assert "%f" not in formatted
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$", formatted)


def test_full_payload_timestamp_is_iso_with_microseconds():
    line = _JsonFormatter().format(_record())
    assert re.search(r'"timestamp": "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}"', line)
    assert "%f" not in line
