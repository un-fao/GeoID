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
