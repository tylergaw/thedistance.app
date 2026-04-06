import random
import time

BASE32_SORTABLE = "234567abcdefghijklmnopqrstuvwxyz"

_last_tid_int = 0


def generate_tid():
    """Generate an AT Protocol TID (Timestamp Identifier).

    Returns a 13-character base32-sortable string encoding a 64-bit integer:
    - Top bit: always 0
    - Bits 1-53: microseconds since Unix epoch
    - Bits 54-63: random 10-bit clock ID

    Guarantees monotonically increasing output even when called multiple times
    within the same microsecond, by bumping past the last generated value.
    """
    global _last_tid_int

    timestamp_us = int(time.time() * 1_000_000)
    clock_id = random.randint(0, 1023)
    tid_int = (timestamp_us << 10) | clock_id
    tid_int &= 0x7FFFFFFFFFFFFFFF  # clear top bit

    if tid_int <= _last_tid_int:
        tid_int = _last_tid_int + 1

    _last_tid_int = tid_int

    chars = []
    for _ in range(13):
        chars.append(BASE32_SORTABLE[tid_int & 0x1F])
        tid_int >>= 5

    return "".join(reversed(chars))
