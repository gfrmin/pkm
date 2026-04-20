"""Tests for ``pkm.producers.unstructured`` — the Unstructured
extractor (SPEC §7.1, §7.2).

Fixture strategy: two committed files covering Unstructured's
primary niches — a plain-text ``.eml`` (its unique value against
Pandoc + Docling) and a pandoc-generated ``.pptx``. Total
committed fixture size is ~31 KB. Failure-path fixtures are
generated in ``tmp_path``.

Producer fixture is module-scoped (same pattern as Docling). The
first ``produce`` call initialises NLTK data on demand; sharing
the instance keeps the test file's wall-clock modest.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pkm.producer import (
    Producer,
    ProducerConfigError,
    ProducerResult,
    ProducerVersionMismatchError,
)
from pkm.producers.unstructured import (
    UnstructuredProducer,
    installed_unstructured_version,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "unstructured"


@pytest.fixture(scope="module")
def unstructured_version() -> str:
    return installed_unstructured_version()


@pytest.fixture(scope="module")
def producer(unstructured_version: str) -> UnstructuredProducer:
    return UnstructuredProducer(
        expected_version=unstructured_version,
        config={"strategy": "auto"},
    )


@pytest.fixture
def unsupported_file(tmp_path: Path) -> Path:
    path = tmp_path / "thing.xyz"
    path.write_bytes(b"extension outside the supported set\n")
    return path


# --- Construction: version and config -----------------------------------


def test_construction_succeeds_with_matching_version(
    unstructured_version: str,
) -> None:
    p = UnstructuredProducer(
        expected_version=unstructured_version,
        config={"strategy": "auto"},
    )
    assert p.name == "unstructured"
    assert p.version == unstructured_version


def test_construction_raises_on_version_mismatch(
    unstructured_version: str,
) -> None:
    wrong = "0.0.0"
    assert wrong != unstructured_version
    with pytest.raises(ProducerVersionMismatchError) as excinfo:
        UnstructuredProducer(
            expected_version=wrong,
            config={"strategy": "auto"},
        )
    assert excinfo.value.producer_name == "unstructured"
    assert excinfo.value.expected == wrong
    assert excinfo.value.installed == unstructured_version


def test_construction_rejects_missing_strategy(
    unstructured_version: str,
) -> None:
    with pytest.raises(ProducerConfigError, match="missing required keys"):
        UnstructuredProducer(
            expected_version=unstructured_version, config={}
        )


def test_construction_rejects_unknown_key(
    unstructured_version: str,
) -> None:
    with pytest.raises(ProducerConfigError, match="unknown keys"):
        UnstructuredProducer(
            expected_version=unstructured_version,
            config={"strategy": "auto", "bogus": 1},
        )


def test_construction_rejects_non_str_strategy(
    unstructured_version: str,
) -> None:
    with pytest.raises(ProducerConfigError, match="must be a str"):
        UnstructuredProducer(
            expected_version=unstructured_version,
            config={"strategy": 1},
        )


def test_construction_rejects_unknown_strategy_value(
    unstructured_version: str,
) -> None:
    with pytest.raises(ProducerConfigError, match="is unknown"):
        UnstructuredProducer(
            expected_version=unstructured_version,
            config={"strategy": "ultra"},
        )


@pytest.mark.parametrize(
    "strategy", ["auto", "fast", "hi_res", "ocr_only"]
)
def test_construction_accepts_all_documented_strategies(
    unstructured_version: str, strategy: str,
) -> None:
    """All four documented strategy values are accepted at
    construction. The producer does not opinionate on which is
    sensible — the user's config.yaml and the routing layer do."""
    UnstructuredProducer(
        expected_version=unstructured_version,
        config={"strategy": strategy},
    )


def test_unstructured_producer_satisfies_producer_protocol(
    producer: UnstructuredProducer,
) -> None:
    assert isinstance(producer, Producer)


# --- Conversion: success paths ------------------------------------------


def test_converts_eml_to_unstructured_json(
    producer: UnstructuredProducer,
) -> None:
    eml = _FIXTURES / "sample.eml"
    result = producer.produce(eml, "a" * 64, {})
    assert isinstance(result, ProducerResult)
    assert result.status == "success"
    assert result.content_type == "application/x-unstructured-json"
    assert result.content_encoding == "utf-8"
    assert result.error_message is None
    assert isinstance(result.content, bytes)

    elements = json.loads(result.content)
    assert isinstance(elements, list)
    assert elements, "expected at least one element from the .eml"

    # Semantic content: some element must carry a recognisable
    # fragment of the email body. Not asserting exact element types
    # because unstructured's classification is not byte-stable.
    all_text = " ".join(e.get("text", "") for e in elements)
    assert "quarterly planning sync" in all_text.lower()

    # producer_metadata surfaces strategy + element_count.
    assert result.producer_metadata["strategy"] == "auto"
    assert result.producer_metadata["element_count"] == len(elements)


def test_converts_pptx(producer: UnstructuredProducer) -> None:
    pptx = _FIXTURES / "sample.pptx"
    result = producer.produce(pptx, "a" * 64, {})
    assert result.status == "success"
    assert result.content is not None
    elements = json.loads(result.content)
    assert len(elements) >= 3  # at minimum one per slide


def test_cached_bytes_are_path_independent(
    producer: UnstructuredProducer, tmp_path: Path,
) -> None:
    """SPEC §7.1 invariant: determinism over input *content*, not
    input_path. Two paths carrying byte-identical content MUST
    produce byte-identical output. Unstructured's default JSON
    serialisation leaks filename/file_directory/last_modified AND
    bakes the filename into each element's id_to_hash, so the
    producer strips the metadata and recomputes the IDs before
    serialising. This test pins the fix."""
    src = _FIXTURES / "sample.eml"
    alt = tmp_path / "different-name.eml"
    alt.write_bytes(src.read_bytes())

    r_src = producer.produce(src, "a" * 64, {})
    r_alt = producer.produce(alt, "a" * 64, {})
    assert r_src.status == "success"
    assert r_alt.status == "success"
    assert r_src.content == r_alt.content, (
        "same content at different paths produced different output "
        "— path-dependent metadata has leaked into the cached JSON"
    )


def test_repeated_produce_is_byte_stable(
    producer: UnstructuredProducer,
) -> None:
    """Running produce twice on the same file must return
    byte-identical content. This is a weaker claim than path
    independence but would catch a regression where some nondet
    source (clock, uuid) crept in."""
    eml = _FIXTURES / "sample.eml"
    r1 = producer.produce(eml, "a" * 64, {})
    r2 = producer.produce(eml, "a" * 64, {})
    assert r1.content == r2.content


# --- Failure paths ------------------------------------------------------


def test_unsupported_extension_is_recorded_as_failed(
    producer: UnstructuredProducer, unsupported_file: Path,
) -> None:
    result = producer.produce(unsupported_file, "a" * 64, {})
    assert result.status == "failed"
    assert result.content is None
    assert result.content_type is None
    assert result.error_message is not None
    assert ".xyz" in result.error_message
    assert "supported" in result.error_message.lower()


def test_produce_wraps_internal_exceptions_without_raising(
    producer: UnstructuredProducer,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Pin the "never raises" invariant directly: monkeypatch the
    internal partition call to raise a specific exception and
    confirm produce returns failed rather than propagating."""
    src = _FIXTURES / "sample.eml"

    def _boom(_self: object, _input_path: Path) -> list[object]:
        raise RuntimeError("simulated internal failure")

    monkeypatch.setattr(
        UnstructuredProducer, "_partition", _boom, raising=True
    )
    result = producer.produce(src, "a" * 64, {})
    assert result.status == "failed"
    assert result.error_message is not None
    assert "RuntimeError" in result.error_message
    assert "simulated internal failure" in result.error_message
