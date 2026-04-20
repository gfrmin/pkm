"""Tests for ``pkm.producers.docling`` — the Docling extractor
(SPEC §7.1, §7.2, §13.5).

Fixture strategy differs from the Pandoc tests: Docling's model
load is slow and the output is not byte-stable across runs in all
cases, so the conversion fixtures are committed PDFs under
``tests/fixtures/docling/`` rather than generated at test time. The
committed corpus (simple, table, two-column) is ~34 KB total and
exercises one thing each — baseline extraction, TableFormer, and
DocLayNet multi-column linearisation.

Producer construction is expensive (loads ML models on first
``produce``), so the ``producer`` fixture is module-scoped and
shared across conversion tests. Version-mismatch and
config-rejection tests don't touch the converter and stay fast.

OCR path (scanned PDFs) is deliberately not exercised here; it
would require an image-only PDF fixture and adds tens of seconds
per run. Revisit if the real corpus has many such documents.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pkm.producer import (
    Producer,
    ProducerConfigError,
    ProducerResult,
    ProducerVersionMismatchError,
)
from pkm.producers.docling import (
    DoclingProducer,
    installed_docling_version,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "docling"


@pytest.fixture(scope="module")
def docling_version() -> str:
    return installed_docling_version()


@pytest.fixture(scope="module")
def producer(docling_version: str) -> DoclingProducer:
    return DoclingProducer(
        expected_version=docling_version,
        config={"ocr": False, "table_structure": True},
    )


@pytest.fixture
def corrupted_pdf(tmp_path: Path) -> Path:
    path = tmp_path / "corrupted.pdf"
    path.write_bytes(b"not a real pdf, just bytes with a .pdf extension\n")
    return path


@pytest.fixture
def unsupported_file(tmp_path: Path) -> Path:
    path = tmp_path / "something.xyz"
    path.write_bytes(b"an extension docling does not handle\n")
    return path


# --- Construction: version and config ------------------------------------


def test_construction_succeeds_with_matching_version_and_valid_config(
    docling_version: str,
) -> None:
    p = DoclingProducer(
        expected_version=docling_version,
        config={"ocr": False, "table_structure": True},
    )
    assert p.name == "docling"
    assert p.version == docling_version


def test_construction_raises_on_version_mismatch(
    docling_version: str,
) -> None:
    wrong = "0.0.0"
    assert wrong != docling_version
    with pytest.raises(ProducerVersionMismatchError) as excinfo:
        DoclingProducer(
            expected_version=wrong,
            config={"ocr": False, "table_structure": True},
        )
    err = excinfo.value
    assert err.producer_name == "docling"
    assert err.expected == wrong
    assert err.installed == docling_version
    msg = str(err)
    assert wrong in msg and docling_version in msg


def test_construction_rejects_missing_required_key(
    docling_version: str,
) -> None:
    with pytest.raises(ProducerConfigError, match="missing required keys"):
        DoclingProducer(
            expected_version=docling_version,
            config={"ocr": True},
        )


def test_construction_rejects_unknown_key(docling_version: str) -> None:
    with pytest.raises(ProducerConfigError, match="unknown keys"):
        DoclingProducer(
            expected_version=docling_version,
            config={"ocr": True, "table_structure": True, "bogus": 1},
        )


def test_construction_rejects_non_bool_ocr(docling_version: str) -> None:
    """``ocr: "yes"`` must not be silently coerced — SPEC §14.5's
    exact-value discipline requires a loud rejection here."""
    with pytest.raises(ProducerConfigError, match="must be a bool"):
        DoclingProducer(
            expected_version=docling_version,
            config={"ocr": "yes", "table_structure": True},
        )


def test_construction_rejects_non_bool_int_for_table_structure(
    docling_version: str,
) -> None:
    """``1`` and ``0`` look like booleans in YAML but aren't Python
    bools. Coercing them silently would change cache keys without
    the user noticing; reject."""
    with pytest.raises(ProducerConfigError, match="must be a bool"):
        DoclingProducer(
            expected_version=docling_version,
            config={"ocr": False, "table_structure": 1},
        )


def test_docling_producer_satisfies_producer_protocol(
    producer: DoclingProducer,
) -> None:
    assert isinstance(producer, Producer)


# --- Conversion: success paths ------------------------------------------


def test_converts_simple_pdf_to_docling_json(
    producer: DoclingProducer,
) -> None:
    import json

    pdf = _FIXTURES / "simple.pdf"
    result = producer.produce(pdf, "a" * 64, {})
    assert isinstance(result, ProducerResult)
    assert result.status == "success"
    assert result.content_type == "application/x-docling-json"
    assert result.content_encoding == "utf-8"
    assert result.error_message is None
    assert isinstance(result.content, bytes)

    doc = json.loads(result.content)
    assert doc["schema_name"] == "DoclingDocument"
    assert "texts" in doc
    assert doc["texts"], "expected at least one text block in the document"
    all_text = " ".join(t.get("text", "") for t in doc["texts"])
    assert "Introduction" in all_text

    # Schema version is surfaced to producer_metadata but is NOT in
    # the cache key (SPEC §13.5 rationale applies here too — bumping
    # the schema in a way that matters is a producer version bump,
    # not a cache-key change).
    assert "docling_schema_version" in result.producer_metadata
    assert result.producer_metadata["docling_schema_version"] == doc["version"]


def test_converts_pdf_with_table(producer: DoclingProducer) -> None:
    """A PDF with a clear table should yield at least one detected
    table in the Docling JSON (exercising TableFormer)."""
    import json

    pdf = _FIXTURES / "table.pdf"
    result = producer.produce(pdf, "a" * 64, {})
    assert result.status == "success"
    assert result.content is not None

    doc = json.loads(result.content)
    assert len(doc.get("tables", [])) >= 1, (
        f"expected at least one table, got {len(doc.get('tables', []))}"
    )


def test_converts_two_column_pdf(producer: DoclingProducer) -> None:
    """Multi-column PDF: content preserved end-to-end. Not asserting
    exact reading order (DocLayNet is not byte-stable across minor
    versions); just that both columns' text made it through."""
    import json

    pdf = _FIXTURES / "two-column.pdf"
    result = producer.produce(pdf, "a" * 64, {})
    assert result.status == "success"
    assert result.content is not None

    doc = json.loads(result.content)
    all_text = " ".join(t.get("text", "") for t in doc.get("texts", []))
    assert "First heading" in all_text
    assert "Second heading" in all_text
    assert "Closing" in all_text


# --- Conversion: failure paths ------------------------------------------


def test_unsupported_extension_is_recorded_as_failed(
    producer: DoclingProducer, unsupported_file: Path,
) -> None:
    result = producer.produce(unsupported_file, "a" * 64, {})
    assert result.status == "failed"
    assert result.content is None
    assert result.content_type is None
    assert result.error_message is not None
    assert ".xyz" in result.error_message
    assert "supported" in result.error_message.lower()


def test_corrupted_pdf_records_failure(
    producer: DoclingProducer, corrupted_pdf: Path,
) -> None:
    """Garbage bytes with a .pdf extension: extension check passes,
    Docling's conversion itself fails. The failure must be captured
    as status='failed' with a non-empty error_message (SPEC §7.1
    invariant 2: produce() never raises)."""
    result = producer.produce(corrupted_pdf, "a" * 64, {})
    assert result.status == "failed"
    assert result.content is None
    assert result.error_message is not None
    assert result.error_message  # non-empty
