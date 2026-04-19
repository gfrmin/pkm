"""Producer-layer value types (SPEC §7.1).

This module currently contains only ``ProducerResult``, which
``pkm.cache.write_artifact`` consumes. The ``Producer`` protocol
itself is added in the next step (Step 5 — Producer types) together
with a small conformance test. Splitting the dataclass here from the
protocol there keeps each step's scope minimal while letting the
cache module be written against a real type rather than a stand-in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ProducerResult:
    """The outcome of a single ``Producer.produce`` call (SPEC §7.1,
    §13.3, §14.7).

    Invariants by status:

      status == "success":
        - ``content`` is ``bytes`` (never ``str``, never ``None``).
        - ``content_type`` is a non-empty string (MIME type or
          producer-specific identifier such as
          ``"application/x-docling-json"``).
        - ``content_encoding`` is the declared encoding for text
          artifacts (e.g. ``"utf-8"``) or ``None`` for binary.
          There is no auto-detection at read time (§14.7); the
          declaration is authoritative.
        - ``error_message`` is ``None``.

      status == "failed":
        - ``content``, ``content_type``, ``content_encoding`` are
          all ``None``.
        - ``error_message`` is a non-empty string.

    ``producer_metadata`` is always a dict (possibly empty). It is
    written verbatim into ``meta.json`` and must be
    canonical-JSON-encodable. It is NOT part of the cache key.
    """

    status: Literal["success", "failed"]
    content: bytes | None
    content_type: str | None
    content_encoding: str | None
    error_message: str | None
    producer_metadata: dict[str, Any]
