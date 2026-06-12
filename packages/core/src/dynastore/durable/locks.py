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

"""Stable advisory-lock key derivations.

PostgreSQL advisory locks are keyed by a signed bigint; every distributed
single-actor guarantee in the platform (startup DDL guards, leader election,
dispatcher capability serialization, mandatory-backstop passes) depends on
all pods deriving the *same* integer from the same logical key. Python's
builtin ``hash()`` is salted per process (PEP 456), so each derivation here
uses a deterministic cryptographic hash instead.

Two derivations exist and BOTH are load-bearing:

``stable_lock_key_signed64``
    sha256, first 8 bytes, big-endian **signed** (may be negative). Used by
    the db_config locking tools (startup locks, ``pg_advisory_leadership``)
    since their introduction.

``stable_lock_key_uint63``
    blake2b(digest_size=8) masked to a **non-negative 63-bit** int, with
    NUL-joined multi-part input. Used by the tasks dispatcher (capability
    claim serialization, mandatory backstop) since its introduction.

They are deliberately NOT merged into one function: an advisory lock's
identity is the derived integer. Changing the derivation under an existing
key string would make old and new pods compute different lock ids during a
rolling deploy — both would "win" their own lock, electing two leaders for
the same role (the exact incident class behind the all-PENDING outbox
drain, #1801). Reconciliation therefore means: one home, two explicitly
named algorithms, golden-value tests pinning their outputs. New code should
prefer ``stable_lock_key_uint63`` (multi-part, collision-documented,
non-negative); existing keys must keep the algorithm they shipped with.
"""

import hashlib


def stable_lock_key_signed64(key: str) -> int:
    """Stable signed 64-bit int from a string key (sha256-based).

    Byte-equivalent successor of the db_config locking-tools derivation:
    first 8 bytes of ``sha256(key)``, big-endian, signed — the full signed
    bigint range, so negative values are normal.
    """
    hashed = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(hashed[:8], byteorder="big", signed=True)


def stable_lock_key_uint63(*parts: str) -> int:
    """Stable non-negative 63-bit int from one or more key parts (blake2b).

    Byte-equivalent successor of the tasks-dispatcher derivation. Parts are
    NUL-joined before hashing so ``("a", "bc")`` and ``("ab", "c")`` derive
    different keys. The result is masked to 63 bits so it always fits
    PostgreSQL's signed bigint ``pg_try_advisory_xact_lock(bigint)``
    signature without ever being negative.
    """
    h = hashlib.blake2b(
        b"\x00".join(p.encode("utf-8") for p in parts), digest_size=8,
    )
    return int.from_bytes(h.digest(), "big") & 0x7FFFFFFFFFFFFFFF
