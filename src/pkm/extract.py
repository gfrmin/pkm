"""Extraction pipeline (SPEC §7) — the step where the infrastructure
actually earns its keep.

Composition of everything prior: iterate over registered sources,
consult routing (§7.3) for which producers to run per source, run
them, write results through the cache layer. Idempotency falls out
of the cache (write_artifact short-circuits on existing rows) and
routing (returns ``[]`` on a fully-extracted source).

Producer construction cost is amortised across the run. Docling and
Unstructured in particular load ML models on first use; constructing
them once per run rather than once per source is the difference
between a 10-second run and a multi-minute one. A first pass over
the source set determines which producers any source might need,
and only those are constructed (a corpus with no `.eml` files does
not pay for Unstructured). An explicit ``--producer NAME`` filter
narrows further.

Flags and their semantics:

  ``--verify``        Re-run every already-successful artifact,
                      byte-compare against the cached content. On
                      mismatch: ERROR log with both hashes,
                      print to stdout, non-zero exit. No writes —
                      ``--verify`` is read-only. Failed entries are
                      skipped (their error text may contain
                      transient information that will not match);
                      ``--retry-failed`` is the mechanism for
                      re-running failures.

  ``--retry-failed``  Include previously-failed producers back in
                      the routing candidate set. Passes through to
                      ``pkm.routing.route``.

  ``--source HASH``   Restrict processing to sources whose
                      ``source_id`` starts with the given hex
                      prefix. Prefix must be at least 16 chars to
                      reduce accidental match; ambiguity (two
                      sources sharing the prefix) errors out
                      rather than silently picking one.

  ``--producer NAME`` Restrict processing to the named producer.
                      Names outside {"pandoc", "docling",
                      "unstructured"} error out at construction.

Interruption. ``SIGINT`` (Ctrl-C) sets a flag checked between
sources; the current source completes, a summary is printed, and
the process exits non-zero. Mid-source interruption is the
producer's concern (each has its own timeout).

No parallelism. SPEC §14.6 forbids it; sequential extraction is the
Phase 1 shape. If real-corpus runs in 7h show this as painfully
slow, revisit *with data*.
"""

from __future__ import annotations

import hashlib
import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from pkm.cache import (
    content_path_rel,
    delete_artifact,
    sweep_orphans,
    write_artifact,
)
from pkm.catalogue import open_catalogue
from pkm.config import Config, ExtractorConfig
from pkm.hashing import compute_cache_key
from pkm.producer import Producer
from pkm.producers.docling import DoclingProducer
from pkm.producers.pandoc import PandocProducer
from pkm.producers.unstructured import UnstructuredProducer
from pkm.routing import route

logger = logging.getLogger(__name__)

_PRODUCER_NAMES: frozenset[str] = frozenset(
    {"pandoc", "docling", "unstructured"}
)

_MIN_SOURCE_PREFIX_LEN = 16


class ExtractError(Exception):
    """Raised for extract-invocation problems distinct from per-source
    producer failures (which are recorded as ``status='failed'``
    artifacts).

    Examples: ``--source`` prefix shorter than 16 chars, prefix with
    no matching source, prefix matching multiple sources, unknown
    ``--producer`` name, missing extractor config entry for a
    producer that routing would call.
    """


@dataclass(frozen=True)
class ExtractResult:
    """Outcome of an ``extract`` call."""

    total_sources: int
    processed: int
    succeeded: int
    failed: int
    cache_hits: int
    mismatches: int
    interrupted: bool
    elapsed_seconds: float
    mismatch_cache_keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _SourceRecord:
    source_id: str
    current_path: Path
    tags: frozenset[str]


class _StopFlag:
    """Thread-safe-enough stop signal. Set by the SIGINT handler,
    polled between sources. The actual check is a plain attribute
    read; Python bytecode guarantees atomicity of the load we need.
    """

    def __init__(self) -> None:
        self._stop = False

    def request_stop(self) -> None:
        self._stop = True

    def stop_requested(self) -> bool:
        return self._stop


def extract(
    root: Path,
    config: Config,
    *,
    verify: bool = False,
    retry_failed: bool = False,
    source_prefix: str | None = None,
    producer_name: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> ExtractResult:
    """Run the extraction pipeline over registered sources.

    Args:
        root: Knowledge root (SPEC §3).
        config: Loaded ``config.yaml`` contents, including the
            ``extractors`` section with per-producer versions and
            configs.
        verify: If True, re-run every already-successful artifact
            and byte-compare with the cached content; writes
            nothing; non-zero mismatches surface in the returned
            ``ExtractResult``.
        retry_failed: If True, re-run previously-failed producers.
        source_prefix: Restrict to source_ids starting with this
            hex prefix (min 16 chars).
        producer_name: Restrict to this one producer.
        progress: Optional callable invoked once per source with a
            one-line human-readable summary. Intended for CLI
            stdout; tests pass a capturing list.

    Returns:
        ``ExtractResult`` with per-run counters.

    Raises:
        ExtractError: invocation-level problem (bad prefix, unknown
            producer, missing extractor config).
    """
    t_start = time.monotonic()

    if producer_name is not None and producer_name not in _PRODUCER_NAMES:
        raise ExtractError(
            f"unknown --producer {producer_name!r}; valid values are "
            f"{sorted(_PRODUCER_NAMES)}"
        )

    stop_flag = _StopFlag()
    old_handler = signal.signal(
        signal.SIGINT,
        lambda _signum, _frame: stop_flag.request_stop(),
    )

    try:
        return _run(
            root=root,
            config=config,
            verify=verify,
            retry_failed=retry_failed,
            source_prefix=source_prefix,
            producer_name=producer_name,
            progress=progress,
            stop_flag=stop_flag,
            t_start=t_start,
        )
    finally:
        signal.signal(signal.SIGINT, old_handler)


def _run(
    *,
    root: Path,
    config: Config,
    verify: bool,
    retry_failed: bool,
    source_prefix: str | None,
    producer_name: str | None,
    progress: Callable[[str], None] | None,
    stop_flag: _StopFlag,
    t_start: float,
) -> ExtractResult:
    with open_catalogue(root) as conn:
        swept = sweep_orphans(root, conn)
        if swept:
            logger.warning(
                "extract_swept_orphans",
                extra={
                    "event": "extract_swept_orphans",
                    "count": len(swept),
                },
            )

        sources = _load_sources(conn, source_prefix)
        if not sources:
            elapsed = time.monotonic() - t_start
            logger.info(
                "extract_complete",
                extra={
                    "event": "extract_complete",
                    "total_sources": 0,
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "cache_hits": 0,
                    "mismatches": 0,
                    "elapsed_seconds": elapsed,
                },
            )
            return ExtractResult(
                total_sources=0,
                processed=0,
                succeeded=0,
                failed=0,
                cache_hits=0,
                mismatches=0,
                interrupted=False,
                elapsed_seconds=elapsed,
            )

        possibly_needed = _needed_producer_names(sources, producer_name)
        _require_config_for(possibly_needed, config)

        # Construction is lazy: producers are materialised the first
        # time a source triggers them, not up-front. This avoids paying
        # Docling's model-load cost on a corpus where routing only
        # eagerly calls Pandoc (and Docling would only construct if a
        # Pandoc failure escalates the document). Up-front we've
        # already validated that config.extractors has an entry for
        # every producer that could conceivably be called, so a
        # missing-config error surfaces before any extraction work
        # begins; only the actual model-load cost is deferred.
        producers: dict[str, Producer] = {}

        logger.info(
            "extract_started",
            extra={
                "event": "extract_started",
                "total_sources": len(sources),
                "possibly_needed_producers": sorted(possibly_needed),
                "verify": verify,
                "retry_failed": retry_failed,
            },
        )

        counters = _Counters()
        interrupted = False

        for i, source in enumerate(sources, start=1):
            if stop_flag.stop_requested():
                interrupted = True
                logger.warning(
                    "extract_interrupted",
                    extra={
                        "event": "extract_interrupted",
                        "processed": counters.processed,
                        "remaining": len(sources) - counters.processed,
                    },
                )
                break

            parts = _process_source(
                source=source,
                conn=conn,
                root=root,
                producers=producers,
                producer_factory=lambda name: _ensure_constructed(
                    name, producers, config
                ),
                config=config,
                verify=verify,
                retry_failed=retry_failed,
                producer_filter=producer_name,
                counters=counters,
            )
            counters.processed += 1

            if progress is not None:
                progress(
                    f"[{i}/{len(sources)}] {source.source_id[:12]}: "
                    + (" | ".join(parts) if parts else "(nothing to do)")
                )

    elapsed = time.monotonic() - t_start
    logger.info(
        "extract_complete",
        extra={
            "event": "extract_complete",
            "total_sources": len(sources),
            "processed": counters.processed,
            "succeeded": counters.succeeded,
            "failed": counters.failed,
            "cache_hits": counters.cache_hits,
            "mismatches": counters.mismatches,
            "elapsed_seconds": elapsed,
            "interrupted": interrupted,
        },
    )

    return ExtractResult(
        total_sources=len(sources),
        processed=counters.processed,
        succeeded=counters.succeeded,
        failed=counters.failed,
        cache_hits=counters.cache_hits,
        mismatches=counters.mismatches,
        interrupted=interrupted,
        elapsed_seconds=elapsed,
        mismatch_cache_keys=list(counters.mismatch_cache_keys),
    )


# --- Mutable counters ----------------------------------------------------


@dataclass
class _Counters:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    cache_hits: int = 0
    mismatches: int = 0
    mismatch_cache_keys: list[str] = field(default_factory=list)


# --- Source loading ------------------------------------------------------


def _load_sources(
    conn: duckdb.DuckDBPyConnection, source_prefix: str | None
) -> list[_SourceRecord]:
    if source_prefix is None:
        rows = conn.execute(
            "SELECT source_id, current_path FROM sources "
            "ORDER BY source_id"
        ).fetchall()
    else:
        _validate_prefix(source_prefix)
        prefix = source_prefix.lower()
        rows = conn.execute(
            "SELECT source_id, current_path FROM sources "
            "WHERE source_id LIKE ? ORDER BY source_id",
            [prefix + "%"],
        ).fetchall()
        if not rows:
            raise ExtractError(
                f"--source prefix {source_prefix!r} matched no sources"
            )
        if len(rows) > 1:
            raise ExtractError(
                f"--source prefix {source_prefix!r} matched "
                f"{len(rows)} sources; use a longer prefix to "
                f"disambiguate"
            )

    out: list[_SourceRecord] = []
    for sid, current_path in rows:
        tag_rows = conn.execute(
            "SELECT tag FROM source_tags WHERE source_id = ?", [sid]
        ).fetchall()
        tags = frozenset(r[0] for r in tag_rows)
        out.append(
            _SourceRecord(
                source_id=sid,
                current_path=Path(current_path),
                tags=tags,
            )
        )
    return out


def _validate_prefix(prefix: str) -> None:
    if len(prefix) < _MIN_SOURCE_PREFIX_LEN:
        raise ExtractError(
            f"--source prefix must be at least "
            f"{_MIN_SOURCE_PREFIX_LEN} hex characters, got "
            f"{len(prefix)} ({prefix!r})"
        )
    lower = prefix.lower()
    if not all(c in "0123456789abcdef" for c in lower):
        raise ExtractError(
            f"--source prefix must be lowercase hex, got {prefix!r}"
        )


# --- Producer construction -----------------------------------------------


def _needed_producer_names(
    sources: list[_SourceRecord], producer_filter: str | None
) -> set[str]:
    """Enumerated by producer rather than dict-dispatched so mypy can
    see the concrete class each ``handled_formats`` lookup resolves
    to."""
    extensions = {s.current_path.suffix.lower() for s in sources}

    if producer_filter is not None:
        if producer_filter == "pandoc":
            handled = PandocProducer.handled_formats
        elif producer_filter == "docling":
            handled = DoclingProducer.handled_formats
        else:  # "unstructured" — validated upstream
            handled = UnstructuredProducer.handled_formats
        return {producer_filter} if extensions & handled else set()

    needed: set[str] = set()
    if extensions & PandocProducer.handled_formats:
        needed.add("pandoc")
    if extensions & DoclingProducer.handled_formats:
        needed.add("docling")
    if extensions & UnstructuredProducer.handled_formats:
        needed.add("unstructured")
    return needed


def _require_config_for(names: set[str], config: Config) -> None:
    """Validate up-front that every possibly-called producer has an
    ``extractors.<name>`` entry in config.yaml. Fails fast so a typo
    or missing declaration surfaces before any extraction work
    starts, rather than halfway through the corpus.
    """
    missing = names - set(config.extractors)
    if missing:
        raise ExtractError(
            f"config.yaml has no `extractors.{sorted(missing)[0]}` "
            f"section, but some source in the corpus has an extension "
            f"that producer could be called on. Declare "
            f"extractors.<name>.version and .config for every producer "
            f"that might run, or use --producer to narrow the scope."
        )


def _ensure_constructed(
    name: str,
    producers: dict[str, Producer],
    config: Config,
) -> Producer:
    """Materialise ``name``'s producer on first use, caching in
    ``producers`` for subsequent sources in the same run. The up-front
    ``_require_config_for`` call has already verified the config entry
    exists, so the ``KeyError`` path is defensive only."""
    if name in producers:
        return producers[name]

    spec: ExtractorConfig = config.extractors[name]
    if name == "pandoc":
        producers[name] = PandocProducer(expected_version=spec.version)
    elif name == "docling":
        producers[name] = DoclingProducer(
            expected_version=spec.version, config=spec.config
        )
    elif name == "unstructured":
        producers[name] = UnstructuredProducer(
            expected_version=spec.version, config=spec.config
        )
    else:
        raise ExtractError(f"unknown producer {name!r}")
    return producers[name]


# --- Per-source dispatch -------------------------------------------------


def _process_source(
    *,
    source: _SourceRecord,
    conn: duckdb.DuckDBPyConnection,
    root: Path,
    producers: dict[str, Producer],
    producer_factory: Callable[[str], Producer],
    config: Config,
    verify: bool,
    retry_failed: bool,
    producer_filter: str | None,
    counters: _Counters,
) -> list[str]:
    """Run whatever producers apply to this source, mutating
    ``counters`` in place. Returns a list of per-producer display
    strings suitable for a progress line.
    """
    succeeded, failed = _existing_attempts(conn, source.source_id)

    if verify:
        # Re-run every successful producer; byte-compare the output.
        to_run = list(succeeded)
    else:
        to_run = route(
            extension=source.current_path.suffix.lower(),
            tags=list(source.tags),
            succeeded=succeeded,
            failed=failed,
            retry_failed=retry_failed,
        )

    if producer_filter is not None:
        to_run = [p for p in to_run if p == producer_filter]

    parts: list[str] = []
    for name in to_run:
        producer = producer_factory(name)

        spec = config.extractors[name]
        cache_key = compute_cache_key(
            input_hash=source.source_id,
            producer_name=name,
            producer_version=producer.version,
            producer_config=spec.config,
        )

        if verify:
            parts.append(
                _verify_one(
                    source=source,
                    producer=producer,
                    spec=spec,
                    cache_key=cache_key,
                    root=root,
                    counters=counters,
                )
            )
        else:
            # If this producer is in the failed set, the caller has
            # asked for a retry (routing would not have included it
            # otherwise). Delete the cached failure so write_artifact
            # writes fresh bytes rather than short-circuiting on the
            # existing row (SPEC §14.3).
            if name in failed:
                delete_artifact(root, conn, cache_key)

            parts.append(
                _run_one(
                    source=source,
                    name=name,
                    producer=producer,
                    spec=spec,
                    cache_key=cache_key,
                    root=root,
                    conn=conn,
                    counters=counters,
                )
            )
    return parts


def _run_one(
    *,
    source: _SourceRecord,
    name: str,
    producer: Producer,
    spec: ExtractorConfig,
    cache_key: str,
    root: Path,
    conn: duckdb.DuckDBPyConnection,
    counters: _Counters,
) -> str:
    # Routing has already filtered out producers with an existing
    # artifact for this source, so a cache hit here is unusual but
    # possible under re-routing edge cases (e.g., config producing
    # the same cache_key by coincidence). Treat explicitly.
    if _artifact_row_exists(conn, cache_key):
        counters.cache_hits += 1
        return f"{name} (cache hit)"

    t0 = time.monotonic()
    result = producer.produce(
        source.current_path, source.source_id, spec.config
    )
    elapsed = time.monotonic() - t0

    write_artifact(
        root,
        conn,
        cache_key=cache_key,
        input_hash=source.source_id,
        producer_name=name,
        producer_version=producer.version,
        producer_config=spec.config,
        result=result,
    )

    if result.status == "success":
        counters.succeeded += 1
        logger.info(
            "extraction_succeeded",
            extra={
                "event": "extraction_succeeded",
                "source_id": source.source_id,
                "cache_key": cache_key,
                "producer": name,
                "elapsed_seconds": elapsed,
            },
        )
        return f"{name} (extracted, {elapsed:.1f}s)"

    counters.failed += 1
    logger.warning(
        "extraction_failed",
        extra={
            "event": "extraction_failed",
            "source_id": source.source_id,
            "cache_key": cache_key,
            "producer": name,
            "elapsed_seconds": elapsed,
            "error_message": result.error_message,
        },
    )
    return f"{name} (failed, {elapsed:.1f}s)"


def _verify_one(
    *,
    source: _SourceRecord,
    producer: Producer,
    spec: ExtractorConfig,
    cache_key: str,
    root: Path,
    counters: _Counters,
) -> str:
    """Re-run the producer and byte-compare with the cached content.
    Writes nothing. Mismatch surfaces as ERROR log + counter bump.
    """
    result = producer.produce(
        source.current_path, source.source_id, spec.config
    )
    if result.status != "success":
        # Re-running produced a failure; that's itself a mismatch with
        # the cached success. Count and record.
        counters.mismatches += 1
        counters.mismatch_cache_keys.append(cache_key)
        logger.error(
            "verify_mismatch",
            extra={
                "event": "verify_mismatch",
                "cache_key": cache_key,
                "reason": "re-run produced status=failed",
                "error_message": result.error_message,
            },
        )
        return f"{producer.name} (VERIFY MISMATCH: re-run failed)"

    cached_path = root / "cache" / content_path_rel(cache_key) / "content"
    cached_bytes = cached_path.read_bytes()
    if result.content is None or result.content != cached_bytes:
        counters.mismatches += 1
        counters.mismatch_cache_keys.append(cache_key)
        cached_hash = hashlib.sha256(cached_bytes).hexdigest()
        rerun_hash = hashlib.sha256(result.content or b"").hexdigest()
        logger.error(
            "verify_mismatch",
            extra={
                "event": "verify_mismatch",
                "cache_key": cache_key,
                "cached_sha256": cached_hash,
                "rerun_sha256": rerun_hash,
            },
        )
        return (
            f"{producer.name} (VERIFY MISMATCH: "
            f"cached {cached_hash[:12]}… rerun {rerun_hash[:12]}…)"
        )

    return f"{producer.name} (verify ok)"


# --- Catalogue helpers ---------------------------------------------------


def _existing_attempts(
    conn: duckdb.DuckDBPyConnection, source_id: str
) -> tuple[list[str], list[str]]:
    rows = conn.execute(
        "SELECT producer_name, status FROM artifacts "
        "WHERE input_hash = ?",
        [source_id],
    ).fetchall()
    succeeded: list[str] = []
    failed: list[str] = []
    for name, status in rows:
        if status == "success":
            succeeded.append(name)
        elif status == "failed":
            failed.append(name)
    return succeeded, failed


def _artifact_row_exists(
    conn: duckdb.DuckDBPyConnection, cache_key: str
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM artifacts WHERE cache_key = ?", [cache_key]
    ).fetchone()
    return row is not None
