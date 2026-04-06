import re

from app.tid import generate_tid

TID_RE = re.compile(r"^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$")


def validate_tid(v):
    """Borrowed from the Python AT Protocol SDK."""
    if not TID_RE.match(v) or (ord(v[0]) & 0x40):
        raise ValueError(f"Invalid TID: {v}")
    return v


class TestGenerateTid:
    def test_length_is_13(self):
        tid = generate_tid()
        assert len(tid) == 13

    def test_passes_sdk_validator(self):
        for _ in range(100):
            tid = generate_tid()
            validate_tid(tid)

    def test_monotonically_increasing(self):
        tids = [generate_tid() for _ in range(100)]
        assert tids == sorted(tids)

    def test_unique(self):
        tids = {generate_tid() for _ in range(1000)}
        assert len(tids) == 1000
