"""API key generation and hashing.

Keys carry 256 bits of entropy, so a single unsalted SHA-256 is sufficient: there is
nothing to brute force, and a fast hash keeps authentication cheap on the hot path.
Only the hash is stored.
"""

from __future__ import annotations

import hashlib
import secrets

KEY_PREFIX = "jp_"


def generate_api_key() -> tuple[str, str]:
    raw = KEY_PREFIX + secrets.token_urlsafe(32)
    return raw, hash_api_key(raw)


def hash_api_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()
