"""Canonicalisation and cache-key construction.

This module is the single source of truth for every hash in the
system (SPEC §4.3). No other code path is permitted to construct
cache keys or canonicalise data for hashing — it all flows through
``canonical_json`` and ``compute_cache_key``.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

_CACHE_KEY_SCHEMA_VERSION = 1


def canonical_json(obj: Any) -> str:
    """Canonicalise a value to its one-and-only JSON string, for hashing.

    Per SPEC §4.1 the canonical form is::

        json.dumps(obj, sort_keys=True, separators=(',', ':'),
                   ensure_ascii=False)

    Any deviation is a correctness bug: it changes cache keys and
    silently fragments the cache.

    Args:
        obj: A JSON-serialisable value composed of dict, list, str,
            int, float, bool, and None only. No bytes, no datetime,
            no custom classes — the canonical form must be exactly
            reproducible by any stdlib consumer.

    Returns:
        The canonical JSON string. Stable across Python versions, OS
        and locales. ``ensure_ascii=False`` is deliberate so non-ASCII
        strings appear as their UTF-8 characters rather than
        ``\\uXXXX`` escapes; this keeps the canonicalisation aligned
        with the on-disk encoding.

    Raises:
        TypeError: if ``obj`` contains a value ``json.dumps`` cannot
            encode. Custom encoders are NOT registered — unencodable
            values are a caller bug and must surface loudly.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_cache_key(
    input_hash: str,
    producer_name: str,
    producer_version: str,
    producer_config: dict[str, Any],
) -> str:
    """Compute the cache key for an artifact — the ONE function that
    constructs cache keys anywhere in the system (SPEC §4.3).

    Formula (SPEC §4.2)::

        producer_config_hash = sha256(canonical_json(producer_config))
        cache_key = sha256(canonical_json({
            "schema_version": 1,
            "input_hash":            input_hash,
            "producer_name":         producer_name,
            "producer_version":      producer_version,
            "producer_config_hash":  producer_config_hash,
        }))

    Invariants:

      - ``input_hash`` is the SHA-256 of the input's *byte content*,
        never the cache key of an upstream artifact (SPEC §4.2).
      - None of: timestamp, path, user identity, API key, retry count,
        hostname, run-id ever enters this function (SPEC §4.4).
      - ``schema_version`` is part of the hashed payload so a future
        change in key structure invalidates old keys loudly, not
        silently.
      - Returned hash is always 64 lowercase hex characters
        (SPEC §14.4).

    Args:
        input_hash: 64-char lowercase SHA-256 hex of the input's
            content. Validated on entry.
        producer_name: Stable identifier (e.g., ``"pandoc"``).
        producer_version: Exact installed version (e.g., ``"3.1.9"``).
            SPEC §14.5 startup check verifies installed == configured;
            this function trusts the caller.
        producer_config: Parameters controlling producer behaviour.
            Must be canonical-JSON-encodable.

    Returns:
        64-character lowercase SHA-256 hex.

    Raises:
        ValueError: if ``input_hash`` is not exactly 64 lowercase hex
            characters.
        TypeError: if ``producer_config`` is not canonical-JSON-
            encodable (surfaced from ``canonical_json``).
    """
    if not _HEX64_RE.match(input_hash):
        raise ValueError(
            f"input_hash must be exactly 64 lowercase hex characters, "
            f"got {input_hash!r}"
        )

    producer_config_hash = hashlib.sha256(
        canonical_json(producer_config).encode("utf-8")
    ).hexdigest()

    payload = {
        "schema_version": _CACHE_KEY_SCHEMA_VERSION,
        "input_hash": input_hash,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "producer_config_hash": producer_config_hash,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
