"""Integration tests for ``pkm.extract`` — the composition step
(SPEC §7).

These tests exercise the full chain: migrate → ingest → extract,
writing real artifacts to the cache through real producer calls.
Fixtures lean on Pandoc (fast, subprocess) and Unstructured
(~2 s first call, ~0.1 s subsequent) to keep the suite tolerable;
Docling is deliberately not exercised here — its contract is
tested in test_producer_docling.py, and forcing its model load in
every extract run would push the suite over 30 s for questionable
added signal.

Config construction uses the actually-installed producer versions
(discovered at test-collection time via each producer's
``installed_*_version``) so the version check inside the producer
constructors passes; any version drift between the installed
package and a hardcoded test string would be a test-environment
bug, not a pkm bug.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from pkm.catalogue import open_catalogue, run_migrations
from pkm.config import Config, ExtractorConfig
from pkm.extract import ExtractError, ExtractResult, extract
from pkm.ingest import ingest_sources
from pkm.producers.docling import installed_docling_version
from pkm.producers.pandoc import installed_pandoc_version
from pkm.producers.unstructured import installed_unstructured_version

# --- Helpers -------------------------------------------------------------


@dataclass(frozen=True)
class _Bench:
    """Shortcut bundle: a migrated root with sources already
    ingested, plus a matching Config object. Each test gets its own
    ``tmp_path``-scoped instance."""

    root: Path
    config: Config
    sources_dir: Path


def _build_config(root: Path) -> Config:
    return Config(
        root_dir=root,
        source=root / "config.yaml",
        extractors={
            "pandoc": ExtractorConfig(
                version=installed_pandoc_version(),
                config={},
            ),
            "docling": ExtractorConfig(
                version=installed_docling_version(),
                config={"ocr": False, "table_structure": True},
            ),
            "unstructured": ExtractorConfig(
                version=installed_unstructured_version(),
                config={"strategy": "auto"},
            ),
        },
    )


def _write(path: Path, content: bytes | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)
    return path


def _bench(tmp_path: Path, sources: list[tuple[str, bytes | str]]) -> _Bench:
    """Set up a migrated root and ingest the given (relative_path,
    content) pairs. Returns a _Bench ready for ``extract`` calls."""
    root = tmp_path / "knowledge"
    (root / "cache").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "sources").mkdir()
    run_migrations(root)

    sources_dir = tmp_path / "docs"
    yaml_entries: list[str] = []
    for rel, content in sources:
        p = _write(sources_dir / rel, content)
        yaml_entries.append(f"  - path: {p}\n")
    (root / "sources" / "sources.yaml").write_text(
        "version: 1\nsources:\n" + "".join(yaml_entries),
        encoding="utf-8",
    )

    ingest_sources(root)
    return _Bench(
        root=root, config=_build_config(root), sources_dir=sources_dir
    )


def _artifact_rows(root: Path) -> list[tuple[str, str, str]]:
    with open_catalogue(root) as conn:
        return [
            (input_hash, producer_name, status)
            for input_hash, producer_name, status in conn.execute(
                "SELECT input_hash, producer_name, status FROM artifacts "
                "ORDER BY input_hash, producer_name"
            ).fetchall()
        ]


# --- End-to-end happy path ----------------------------------------------


def test_extract_over_md_and_eml_produces_artifacts(
    tmp_path: Path,
) -> None:
    bench = _bench(
        tmp_path,
        [
            ("note.md", "# Hello\n\nBody text.\n"),
            (
                "mail.eml",
                "From: a@b\nTo: c@d\nSubject: T\n\nBody line.\n",
            ),
        ],
    )

    progress_lines: list[str] = []
    result = extract(
        bench.root,
        bench.config,
        progress=progress_lines.append,
    )

    assert isinstance(result, ExtractResult)
    assert result.total_sources == 2
    assert result.processed == 2
    assert result.succeeded == 2
    assert result.failed == 0
    assert result.cache_hits == 0
    assert result.mismatches == 0
    assert not result.interrupted

    rows = _artifact_rows(bench.root)
    producers = {r[1] for r in rows}
    assert producers == {"pandoc", "unstructured"}, rows
    assert all(r[2] == "success" for r in rows)

    assert len(progress_lines) == 2
    assert all(
        "extracted" in line or "cache hit" in line for line in progress_lines
    )


def test_second_extract_is_a_no_op(tmp_path: Path) -> None:
    bench = _bench(
        tmp_path, [("note.md", "# Hello\n\nBody text.\n")]
    )

    extract(bench.root, bench.config)
    rows_before = _artifact_rows(bench.root)

    result = extract(bench.root, bench.config)
    rows_after = _artifact_rows(bench.root)

    assert rows_after == rows_before
    # Routing returned [] so nothing was attempted;
    # cache_hits is zero because we never even asked.
    assert result.succeeded == 0
    assert result.failed == 0
    assert result.cache_hits == 0


# --- Verify: read-only check of cache integrity --------------------------


def test_verify_on_clean_cache_reports_zero_mismatches(
    tmp_path: Path,
) -> None:
    bench = _bench(
        tmp_path, [("note.md", "# Hello\n\nBody.\n")]
    )
    extract(bench.root, bench.config)

    result = extract(bench.root, bench.config, verify=True)
    assert result.mismatches == 0
    assert result.mismatch_cache_keys == []
    # Verify must not write — the artifacts count is unchanged.
    assert len(_artifact_rows(bench.root)) == 1


def test_verify_detects_corrupted_cache_content(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    bench = _bench(
        tmp_path, [("note.md", "# Hello\n\nBody.\n")]
    )
    extract(bench.root, bench.config)

    # Corrupt the cached content file for the one artifact we have.
    with open_catalogue(bench.root) as conn:
        (content_path,) = conn.execute(
            "SELECT content_path FROM artifacts"
        ).fetchone()
    content_file = bench.root / "cache" / content_path / "content"
    content_file.write_bytes(b"corrupted bytes not matching pandoc output")

    with caplog.at_level("ERROR", logger="pkm.extract"):
        result = extract(bench.root, bench.config, verify=True)

    assert result.mismatches == 1
    assert len(result.mismatch_cache_keys) == 1
    assert any(
        r.levelname == "ERROR" and "verify_mismatch" in r.message
        for r in caplog.records
    )


# --- retry_failed re-runs failures --------------------------------------


def test_retry_failed_reruns_and_flips_failed_artifact_to_success(
    tmp_path: Path,
) -> None:
    """Force pandoc's artifact into a failed state (simulating a
    prior run that failed), then confirm --retry-failed re-runs
    pandoc and the artifact becomes status=success.

    Without --retry-failed, the routing policy sends the work to
    Docling as a fallback (pandoc failed → Docling runs on .md),
    and pandoc's row stays failed. That behaviour is also
    asserted here as a side-property of the retry-flag check —
    both sides of the flag should be distinguishable."""
    bench = _bench(
        tmp_path, [("note.md", "# Hello\n\nBody.\n")]
    )
    extract(bench.root, bench.config)

    with open_catalogue(bench.root) as conn:
        conn.execute(
            "UPDATE artifacts SET status = 'failed', "
            "error_message = 'simulated', size_bytes = NULL "
            "WHERE producer_name = 'pandoc'"
        )

    # Without retry_failed: routing proposes Docling as fallback
    # (pandoc failed on .md). Pandoc's row stays failed.
    extract(bench.root, bench.config)
    with open_catalogue(bench.root) as conn:
        statuses = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT producer_name, status FROM artifacts"
            ).fetchall()
        }
    assert statuses["pandoc"] == "failed"

    # With retry_failed: extract deletes the failed row and runs
    # pandoc fresh. The new attempt succeeds; the row flips to
    # status=success.
    result = extract(bench.root, bench.config, retry_failed=True)
    with open_catalogue(bench.root) as conn:
        statuses = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT producer_name, status FROM artifacts"
            ).fetchall()
        }
    assert statuses["pandoc"] == "success"
    assert result.succeeded >= 1


# --- --source prefix filtering ------------------------------------------


def test_source_prefix_restricts_to_one_source(
    tmp_path: Path,
) -> None:
    bench = _bench(
        tmp_path,
        [
            ("a.md", "# A\n\nFirst document.\n"),
            ("b.md", "# B\n\nSecond document.\n"),
        ],
    )

    # Pick the first source's id and use a 16-char prefix.
    with open_catalogue(bench.root) as conn:
        sids = [
            r[0]
            for r in conn.execute(
                "SELECT source_id FROM sources ORDER BY source_id"
            ).fetchall()
        ]

    target = sids[0]
    result = extract(
        bench.root, bench.config, source_prefix=target[:16]
    )
    assert result.total_sources == 1
    assert result.processed == 1

    # The other source was not processed — no artifact for it.
    rows = _artifact_rows(bench.root)
    assert {r[0] for r in rows} == {target}


def test_source_prefix_shorter_than_16_chars_raises(
    tmp_path: Path,
) -> None:
    bench = _bench(
        tmp_path, [("a.md", "# A\n\n.\n")]
    )
    with pytest.raises(ExtractError, match="at least 16"):
        extract(bench.root, bench.config, source_prefix="abc")


def test_source_prefix_non_hex_raises(tmp_path: Path) -> None:
    bench = _bench(
        tmp_path, [("a.md", "# A\n\n.\n")]
    )
    with pytest.raises(ExtractError, match="hex"):
        extract(
            bench.root,
            bench.config,
            source_prefix="g" * 16,
        )


def test_source_prefix_with_no_match_raises(tmp_path: Path) -> None:
    bench = _bench(
        tmp_path, [("a.md", "# A\n\n.\n")]
    )
    with pytest.raises(ExtractError, match="matched no sources"):
        extract(
            bench.root,
            bench.config,
            source_prefix="f" * 16,
        )


# --- --producer filtering -----------------------------------------------


def test_unknown_producer_name_raises(tmp_path: Path) -> None:
    bench = _bench(
        tmp_path, [("a.md", "# A\n\n.\n")]
    )
    with pytest.raises(ExtractError, match="unknown --producer"):
        extract(
            bench.root,
            bench.config,
            producer_name="pandora",
        )


def test_producer_filter_narrows_to_one_producer(tmp_path: Path) -> None:
    """With --producer unstructured, only the .eml source gets
    processed; the .md doesn't (Unstructured's routing doesn't
    propose for .md when Pandoc would handle it).
    """
    bench = _bench(
        tmp_path,
        [
            ("note.md", "# A\n\n.\n"),
            (
                "mail.eml",
                "From: a@b\nTo: c@d\nSubject: T\n\nBody.\n",
            ),
        ],
    )

    result = extract(
        bench.root, bench.config, producer_name="unstructured"
    )
    assert result.succeeded == 1  # only the .eml
    rows = _artifact_rows(bench.root)
    assert len(rows) == 1
    assert rows[0][1] == "unstructured"


# --- Config validation --------------------------------------------------


def test_missing_producer_config_raises_before_any_extraction(
    tmp_path: Path,
) -> None:
    """A corpus with a .md triggers Pandoc; Pandoc's fallback could
    call Docling. Config missing `extractors.docling` should fail
    fast, before any source is processed."""
    bench = _bench(
        tmp_path, [("a.md", "# A\n\n.\n")]
    )
    broken_config = Config(
        root_dir=bench.root,
        source=bench.config.source,
        extractors={
            "pandoc": bench.config.extractors["pandoc"],
            # docling and unstructured missing
        },
    )
    with pytest.raises(ExtractError, match=r"extractors\.docling"):
        extract(bench.root, broken_config)

    # No artifacts produced.
    assert _artifact_rows(bench.root) == []


# --- Empty-corpus edge case ---------------------------------------------


def test_extract_on_empty_source_set_is_a_clean_no_op(
    tmp_path: Path,
) -> None:
    """No sources registered — extract completes without error and
    reports zero work done."""
    root = tmp_path / "knowledge"
    (root / "cache").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "sources").mkdir()
    run_migrations(root)
    config = _build_config(root)

    result = extract(root, config)
    assert result.total_sources == 0
    assert result.processed == 0
    assert result.succeeded == 0


# --- SIGINT flag semantics (direct, no real signals) --------------------


def test_extract_breaks_out_of_loop_when_stop_is_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SIGINT handler sets a flag polled between sources. This
    test simulates the flag being set after one source by
    monkeypatching the progress callback to set it.
    """
    bench = _bench(
        tmp_path,
        [
            ("a.md", "# A\n\n.\n"),
            ("b.md", "# B\n\n.\n"),
            ("c.md", "# C\n\n.\n"),
        ],
    )

    # Install a real SIGINT via the running process' own signal
    # machinery: we can't easily send SIGINT from inside pytest, so
    # we instead rely on the progress callback (called after each
    # source) to trigger the same _StopFlag the handler sets.
    import signal

    def progress_and_interrupt(_line: str) -> None:
        # Raise SIGINT ourselves; the handler installed by extract()
        # will flip the stop flag.
        signal.raise_signal(signal.SIGINT)

    result = extract(
        bench.root, bench.config, progress=progress_and_interrupt
    )

    assert result.interrupted
    assert result.processed == 1  # only one source before the signal
    assert result.total_sources == 3
