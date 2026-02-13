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

import uuid
import time
import os


# UUIDV7_GENERATOR_DDL = """
# CREATE OR REPLACE FUNCTION uuidv7_generate()
# RETURNS uuid AS $$
# DECLARE
#   v_time numeric;
#   v_unix_t bigint;
#   v_rand_a bigint;
#   v_rand_b bigint;
# BEGIN
#   -- 1. Get current UNIX timestamp in milliseconds
#   v_time := extract(epoch from clock_timestamp());
#   v_unix_t := floor(v_time * 1000)::bigint;

#   -- 2. Generate 12 bits of randomness for the 'rand_a' section
#   v_rand_a := floor(random() * 4096)::bigint;

#   -- 3. Generate 62 bits of randomness for the 'rand_b' section
#   v_rand_b := (floor(random() * 4611686018427387904))::bigint;

#   -- 4. Construct the UUID: 
#   --    - 48 bits: timestamp
#   --    - 4 bits: version (7)
#   --    - 12 bits: rand_a
#   --    - 2 bits: variant (10)
#   --    - 62 bits: rand_b
#   RETURN lpad(to_hex(v_unix_t), 12, '0') || 
#          to_hex((7 << 12) | v_rand_a) ||
#          to_hex((2 << 14) | (v_rand_b >> 48)) || 
#          lpad(to_hex(v_rand_b & 281474976710655), 12, '0');
# END;
# $$ LANGUAGE plpgsql VOLATILE;
# """

def generate_uuidv7() -> uuid.UUID:
    """
    Generates a UUID v7 following RFC 9562.
    - 48 bits: Unix timestamp (ms)
    - 4 bits: Version (7)
    - 12 bits: Sequence/Rand_a
    - 2 bits: Variant (10)
    - 62 bits: Rand_b
    """
    # 1. Get current time in milliseconds (48 bits)
    ms = int(time.time() * 1000)
    
    # 2. Generate random bytes
    # We need 10 bytes (80 bits) of randomness to fill the rest
    rand_bytes = os.urandom(10)
    
    # 3. Construct the byte array
    # 6 bytes for timestamp
    uuid_bytes = bytearray(ms.to_bytes(6, 'big'))
    
    # Add the next 10 bytes of randomness
    uuid_bytes.extend(rand_bytes)
    
    # 4. Set Version (4 bits) - bits 48-51 must be 0111 (7)
    uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x70
    
    # 5. Set Variant (2 bits) - bits 64-65 must be 10
    uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80
    
    return uuid.UUID(bytes=bytes(uuid_bytes))

def generate_geoid() -> str:
    """Generates a new unique identifier (UUIDv7) for a geospatial feature."""
    return str(generate_uuidv7())

def generate_transaction_id() -> str:
    """Generates a unique transaction identifier (UUIDv7)."""
    return str(generate_uuidv7())
