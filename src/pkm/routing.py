"""Routing (SPEC §7.3) — which producers run on a given source.

A single pure function. Not a rule engine, not configuration. Given
the source's extension, tags, and the set of producers already
attempted (with their outcomes), returns the ordered list of
producers that should still run. When every applicable producer
has either succeeded or been retried to exhaustion, the return is
``[]`` — which is how ``pkm extract`` achieves idempotency against
already-extracted sources.

Policy per SPEC §7.3 at v0.1.6:

  1. Pandoc on every source whose extension Pandoc handles.
  2. Docling on PDFs (format-based eager), on any Docling-handled
     source tagged as layout-sensitive (``invoice``, ``report``,
     ``contract``), and as a fallback when Pandoc failed on a
     Docling-handled source.
  3. Unstructured on email formats ``.eml`` / ``.msg`` (format-based
     eager). Unstructured also runs when every other applicable
     producer is blocked — either it does not handle the format or
     it already failed. This collapses two SPEC cases (nobody else
     handles, and both-others-failed) into one predicate.

The router reads ``handled_formats`` class attributes on the three
concrete producers; it does NOT construct them. The version check
and model loading that constructors perform are paid only once per
CLI invocation, by the caller in ``pkm.extract``.
"""

from __future__ import annotations

from collections.abc import Collection

from pkm.producers.docling import DoclingProducer
from pkm.producers.pandoc import PandocProducer
from pkm.producers.unstructured import UnstructuredProducer

_LAYOUT_SENSITIVE_TAGS: frozenset[str] = frozenset(
    {"invoice", "report", "contract"}
)
"""User tags that escalate a Docling-handled source through Docling
even when it's not a PDF. Tags are optional modulators; format
signals remain the default (SPEC §7.3 rationale)."""

_EAGER_DOCLING_EXTENSIONS: frozenset[str] = frozenset({".pdf"})
"""Extensions that trigger Docling unconditionally. PDF is the
canonical layout-heavy format and tag coverage would be unreliable;
the format-based trigger ensures every PDF gets Docling's
structured extraction regardless of tagging diligence."""

_EAGER_UNSTRUCTURED_EXTENSIONS: frozenset[str] = frozenset(
    {".eml", ".msg"}
)
"""Email formats that trigger Unstructured unconditionally. Pandoc
and Docling don't handle these; Unstructured's email-specific
partitioning is the point."""


def route(
    *,
    extension: str,
    tags: Collection[str] = (),
    succeeded: Collection[str] = (),
    failed: Collection[str] = (),
    retry_failed: bool = False,
) -> list[str]:
    """Return the ordered list of producer names to run.

    Args:
        extension: The source's file extension including the dot,
            e.g. ``".pdf"``. Matched case-insensitively.
        tags: User-applied tags on the source (from ``sources.yaml``
            and the ``source_tags`` table).
        succeeded: Producer names that already have a successful
            artifact for this source. Always excluded from the
            returned list.
        failed: Producer names that already have a failed artifact
            for this source. Excluded unless ``retry_failed``.
        retry_failed: If True, include previously-failed producers
            in the candidate set (wired from ``pkm extract
            --retry-failed``).

    Returns:
        Ordered list of producer names (``"pandoc"``, ``"docling"``,
        ``"unstructured"``). Empty when every applicable producer
        has succeeded or no producer applies.
    """
    ext = extension.lower()
    tag_set = set(tags)
    succeeded_set = set(succeeded)
    failed_set = set(failed)

    plan: list[str] = []

    # 1. Pandoc: baseline on every source it handles.
    if ext in PandocProducer.handled_formats:
        plan.append("pandoc")

    # 2. Docling: eager on PDFs + layout-tagged, fallback on
    #    Pandoc-failed.
    if ext in DoclingProducer.handled_formats:
        eager = (
            ext in _EAGER_DOCLING_EXTENSIONS
            or bool(tag_set & _LAYOUT_SENSITIVE_TAGS)
        )
        fallback = "pandoc" in failed_set
        if eager or fallback:
            plan.append("docling")

    # 3. Unstructured: eager on email; otherwise runs when every
    #    earlier producer is blocked (doesn't handle the format OR
    #    already failed). This collapses the "nobody else handles"
    #    and "both-others-failed" cases of §7.3 into one predicate.
    if ext in UnstructuredProducer.handled_formats:
        eager = ext in _EAGER_UNSTRUCTURED_EXTENSIONS
        pandoc_blocked = (
            ext not in PandocProducer.handled_formats
            or "pandoc" in failed_set
        )
        docling_blocked = (
            ext not in DoclingProducer.handled_formats
            or "docling" in failed_set
        )
        if eager or (pandoc_blocked and docling_blocked):
            plan.append("unstructured")

    result: list[str] = []
    for name in plan:
        if name in succeeded_set:
            continue
        if name in failed_set and not retry_failed:
            continue
        result.append(name)
    return result
