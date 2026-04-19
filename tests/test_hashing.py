"""Contract tests for ``pkm.hashing``.

These tests are the keel of the system: every cache key, every
producer config hash, and every canonicalised payload that ever gets
written to ``meta.json`` or to the catalogue flows through the two
functions exercised here. They are deliberately written BEFORE the
implementation exists so the contract is fixed first and the
implementation conforms to it, not the other way around.

Correspondence to SPEC:

- Test 1  → SPEC §4.1 (canonicalisation invariants).
- Test 1b → SPEC §4.1 (``ensure_ascii=False`` detail — the subtle
  bit most likely to silently drift via misreading).
- Test 2  → SPEC §4.2, §4.3, §14.4 (single hashing function,
  input isolation, full 64-char lowercase hex output).
"""

from __future__ import annotations

import json
import re

import pytest
from pkm.hashing import canonical_json, compute_cache_key

HEX64 = re.compile(r"^[0-9a-f]{64}$")


# --- Test 1 -----------------------------------------------------------------

def test_canonical_json_is_order_insensitive_and_deterministic() -> None:
    """``canonical_json`` is stable under dict key reordering, across
    repeat calls, and rejects non-JSON values with ``TypeError`` rather
    than silently coercing them.
    """
    # Dict key order does not affect output.
    assert (
        canonical_json({"b": 1, "a": 2})
        == canonical_json({"a": 2, "b": 1})
        == '{"a":2,"b":1}'
    )

    # Nested dicts are also sorted.
    nested_a = {"z": {"b": 1, "a": 2}, "a": []}
    nested_b = {"a": [], "z": {"a": 2, "b": 1}}
    assert canonical_json(nested_a) == canonical_json(nested_b)

    # Lists preserve their own order — only dict keys are sorted.
    assert canonical_json([3, 1, 2]) == "[3,1,2]"
    assert canonical_json([3, 1, 2]) != canonical_json([1, 2, 3])

    # No whitespace between separators — tightest possible form.
    assert canonical_json({"a": 1, "b": [1, 2]}) == '{"a":1,"b":[1,2]}'

    # Idempotent across many calls.
    obj = {"x": 1, "y": [1, 2, {"a": True, "b": None}]}
    first = canonical_json(obj)
    for _ in range(10):
        assert canonical_json(obj) == first

    # Non-JSON-serialisable inputs raise TypeError — no silent fallback,
    # no custom encoder registration. These are caller bugs.
    with pytest.raises(TypeError):
        canonical_json({"k": {1, 2, 3}})  # set is not JSON
    with pytest.raises(TypeError):
        canonical_json({"k": b"bytes"})   # bytes is not JSON
    with pytest.raises(TypeError):
        canonical_json(object())          # arbitrary object


# --- Test 1b ----------------------------------------------------------------

def test_canonical_json_preserves_utf8_for_non_ascii() -> None:
    """Non-ASCII strings are emitted as their UTF-8 characters, not as
    ``\\uXXXX`` escapes (SPEC §4.1, ``ensure_ascii=False``).

    This is the subtle bit of the canonicalisation most likely to
    silently drift from the spec via a misreading. A drift here would
    silently fragment the cache across any machine that interpreted the
    ensure_ascii flag differently.
    """
    obj = {"k": "café", "arrow": "→", "emoji": "🦀"}
    serialised = canonical_json(obj)

    # Literal UTF-8 characters appear in the output.
    assert "café" in serialised
    assert "→" in serialised
    assert "🦀" in serialised

    # ASCII-escaped equivalents must NOT appear. The crab emoji
    # U+1F980 sits outside the BMP, so its escaped form would use a
    # UTF-16 surrogate pair; both halves must be absent.
    assert "\\u00e9" not in serialised
    assert "\\u2192" not in serialised
    assert "\\ud83e" not in serialised
    assert "\\udd80" not in serialised

    # Exact expected form for the simple case.
    assert canonical_json({"k": "café"}) == '{"k":"café"}'

    # Round-trip via json.loads returns the original mapping with
    # identical code points.
    round_tripped = json.loads(serialised)
    assert round_tripped == obj


# --- Test 2 -----------------------------------------------------------------

def test_compute_cache_key_is_deterministic_and_input_isolated() -> None:
    """``compute_cache_key`` is the single hashing function (SPEC §4.3).
    It is deterministic, isolates each input, returns 64-char lowercase
    hex (SPEC §14.4), and validates its inputs.
    """
    valid_hash = "a" * 64
    base = dict(
        input_hash=valid_hash,
        producer_name="pandoc",
        producer_version="3.1.9",
        producer_config={"ocr": True, "lang": "eng"},
    )

    key = compute_cache_key(**base)

    # Shape: 64 lowercase hex characters, always.
    assert len(key) == 64
    assert HEX64.match(key), f"cache key is not lowercase hex: {key!r}"

    # Deterministic across repeat calls.
    for _ in range(5):
        assert compute_cache_key(**base) == key

    # Perturbing any one argument produces a different key.
    assert compute_cache_key(**{**base, "input_hash": "b" * 64}) != key
    assert compute_cache_key(**{**base, "producer_name": "docling"}) != key
    assert compute_cache_key(**{**base, "producer_version": "3.1.10"}) != key
    assert (
        compute_cache_key(
            **{**base, "producer_config": {"ocr": False, "lang": "eng"}}
        )
        != key
    )

    # producer_config key order must collapse via canonicalisation:
    # semantically identical config → identical key.
    reordered = {"lang": "eng", "ocr": True}
    assert compute_cache_key(**{**base, "producer_config": reordered}) == key

    # Empty config is a valid input and produces a valid-shape key.
    empty_key = compute_cache_key(
        input_hash=valid_hash,
        producer_name="pandoc",
        producer_version="3.1.9",
        producer_config={},
    )
    assert HEX64.match(empty_key)
    assert empty_key != key  # different config → different key

    # Golden key — pins the exact algorithm. If this changes, the
    # cache key format has changed and every existing cache entry
    # is invalidated. Regenerate only by deliberate decision, with
    # a SPEC version bump (the schema_version field inside the key
    # exists for exactly this purpose, SPEC §4.2).
    assert compute_cache_key(
        input_hash="0" * 64,
        producer_name="pandoc",
        producer_version="3.1.9",
        producer_config={},
    ) == "92d68dd86f12140e5bb00e5b97bba4b37d1c9307ba17f272dcf76b560d41fd70"

    # input_hash must be exactly 64 lowercase hex chars.
    for bad in ("a" * 63, "a" * 65, "A" * 64, "g" * 64, "", "not-a-hash"):
        with pytest.raises(ValueError):
            compute_cache_key(**{**base, "input_hash": bad})

    # Non-JSON-serialisable producer_config raises TypeError (surfaced
    # from canonical_json). It is a caller bug, not something the
    # hashing module silently accommodates.
    with pytest.raises(TypeError):
        compute_cache_key(**{**base, "producer_config": {"bad": {1, 2, 3}}})
