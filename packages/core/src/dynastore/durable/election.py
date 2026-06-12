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

"""Canonical import surface for leader election.

Leader election was the first primitive to be unified (after the #1801
fail-closed/fail-open mismatch): :func:`pg_advisory_leadership` holds the
election lock on a dedicated AUTOCOMMIT connection and yields exactly once
on every path; :func:`run_leader_loop` is the resign-on-exception loop
around it. Their bodies still live where they were hardened
(``db_config.locking_tools`` / ``tools.async_utils``) because both are
deeply imported across the codebase; this module is the durable-library
entry point so new durable-work code has ONE place to import election from.
Do not hand-roll either primitive — every divergence between private
copies of this machinery has eventually shipped an incident.
"""

from dynastore.modules.db_config.locking_tools import pg_advisory_leadership
from dynastore.tools.async_utils import run_leader_loop

__all__ = ["pg_advisory_leadership", "run_leader_loop"]
