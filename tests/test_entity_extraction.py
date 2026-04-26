"""Tests for ``pkm.transforms.entity_extraction``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from pkm.policy import CostEstimate
from pkm.transform import ModelResponse
from pkm.transform_declaration import TransformDeclaration
from pkm.transforms.entity_extraction import (
    EntityExtractionProducer,
    estimate_cost,
)

_ENTITY_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["entities", "format_version"],
    "properties": {
        "format_version": {"const": 1},
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["text", "type", "span"],
                "properties": {
                    "text": {"type": "string"},
                    "type": {
                        "type": "string",
                        "enum": [
                            "person", "organization", "location",
                            "date", "money", "other",
                        ],
                    },
                    "span": {
                        "type": "object",
                        "required": ["start", "end"],
                        "properties": {
                            "start": {"type": "integer", "minimum": 0},
                            "end": {"type": "integer", "minimum": 0},
                        },
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                    },
                },
            },
        },
    },
}

_PROMPT_TEMPLATE = "Extract entities from:\n---\n{text}\n---"


def _make_declaration(**overrides: Any) -> TransformDeclaration:
    defaults: dict[str, Any] = {
        "name": "entity_extraction",
        "version": "0.1.0",
        "producer_class": "pkm.transforms.entity_extraction.EntityExtractionProducer",
        "model_identity": {
            "provider": "anthropic",
            "model": "claude-haiku-4-5",
            "inference_params": {"temperature": 0.0, "max_tokens": 4096},
        },
        "prompt_name": "entity_extraction_v1",
        "prompt_text": _PROMPT_TEMPLATE,
        "prompt_hash": "a" * 64,
        "output_schema_name": "entity_extraction_v1",
        "output_schema": _ENTITY_SCHEMA,
        "policies": ["cost_gate", "sensitive_doc_gate"],
        "input_producer": "pandoc",
        "input_required_status": "success",
        "declaration_hash": "b" * 64,
    }
    defaults.update(overrides)
    return TransformDeclaration(**defaults)


@dataclass
class _MockUsage:
    input_tokens: int = 150
    output_tokens: int = 75


@dataclass
class _MockTextBlock:
    text: str = ""
    type: str = "text"


@dataclass
class _MockResponse:
    content: list[_MockTextBlock]
    usage: _MockUsage


def _make_mock_client(output: dict[str, Any]) -> MagicMock:
    client = MagicMock()
    client.messages.create.return_value = _MockResponse(
        content=[_MockTextBlock(text=json.dumps(output))],
        usage=_MockUsage(input_tokens=150, output_tokens=75),
    )
    return client


def _valid_output_for(text: str) -> dict[str, Any]:
    """Build a valid entity extraction output with correct spans."""
    return {
        "format_version": 1,
        "entities": [
            {
                "text": "Alice",
                "type": "person",
                "span": {"start": text.index("Alice"), "end": text.index("Alice") + 5},
                "confidence": 0.95,
            },
        ],
    }


# --- render_prompt -------------------------------------------------------


def test_render_prompt_substitutes_text() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    prompt = producer.render_prompt(
        b"Hello Alice", {"input_hash": "x" * 64},
    )
    assert "Hello Alice" in prompt
    assert "{text}" not in prompt


# --- call_model ----------------------------------------------------------


def test_call_model_returns_model_response() -> None:
    output = {"format_version": 1, "entities": []}
    client = _make_mock_client(output)
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=client,
    )
    response = producer.call_model("test prompt")

    assert isinstance(response, ModelResponse)
    assert response.input_tokens == 150
    assert response.output_tokens == 75
    assert response.cost_usd > 0
    client.messages.create.assert_called_once()


# --- parse_output --------------------------------------------------------


def test_parse_output_handles_plain_json() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    raw = '{"format_version": 1, "entities": []}'
    result = producer.parse_output(raw)
    assert result == {"format_version": 1, "entities": []}


def test_parse_output_strips_code_fences() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    raw = '```json\n{"format_version": 1, "entities": []}\n```'
    result = producer.parse_output(raw)
    assert result == {"format_version": 1, "entities": []}


# --- post_validate -------------------------------------------------------


def test_post_validate_accepts_valid_spans() -> None:
    text = "Alice works at Acme Corp."
    output = _valid_output_for(text)
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    producer.post_validate(output, text.encode())


def test_post_validate_catches_span_out_of_range() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    parsed = {
        "format_version": 1,
        "entities": [
            {"text": "X", "type": "person", "span": {"start": 0, "end": 999}},
        ],
    }
    with pytest.raises(ValueError, match="out of range"):
        producer.post_validate(parsed, b"short")


def test_post_validate_catches_span_text_mismatch() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    parsed = {
        "format_version": 1,
        "entities": [
            {"text": "Bob", "type": "person", "span": {"start": 0, "end": 5}},
        ],
    }
    with pytest.raises(ValueError, match="span text mismatch"):
        producer.post_validate(parsed, b"Alice works here")


def test_post_validate_catches_start_ge_end() -> None:
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=MagicMock(),
    )
    parsed = {
        "format_version": 1,
        "entities": [
            {"text": "X", "type": "person", "span": {"start": 5, "end": 5}},
        ],
    }
    with pytest.raises(ValueError, match=r"start.*>=.*end"):
        producer.post_validate(parsed, b"Hello World")


# --- produce end-to-end -------------------------------------------------


def test_produce_end_to_end_success(tmp_path: Path) -> None:
    text = "Alice works at Acme Corp."
    src = tmp_path / "doc.txt"
    src.write_text(text, encoding="utf-8")

    output = _valid_output_for(text)
    client = _make_mock_client(output)
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=client,
    )

    result = producer.produce(src, "a" * 64, {})
    assert result.status == "success"
    assert result.content is not None
    parsed = json.loads(result.content)
    assert parsed["entities"][0]["text"] == "Alice"
    assert result.producer_metadata["prompt_name"] == "entity_extraction_v1"


def test_produce_schema_validation_failure(tmp_path: Path) -> None:
    src = tmp_path / "doc.txt"
    src.write_text("hello", encoding="utf-8")

    bad_output = {"no_format_version": True}
    client = _make_mock_client(bad_output)
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=client,
    )

    result = producer.produce(src, "a" * 64, {})
    assert result.status == "failed"
    assert "schema_validation_failed" in (result.error_message or "")


def test_produce_post_validate_failure(tmp_path: Path) -> None:
    text = "Alice works here"
    src = tmp_path / "doc.txt"
    src.write_text(text, encoding="utf-8")

    output = {
        "format_version": 1,
        "entities": [
            {
                "text": "Bob",
                "type": "person",
                "span": {"start": 0, "end": 3},
            },
        ],
    }
    client = _make_mock_client(output)
    decl = _make_declaration()
    producer = EntityExtractionProducer(
        declaration=decl, client=client,
    )

    result = producer.produce(src, "a" * 64, {})
    assert result.status == "failed"
    assert "span text mismatch" in (result.error_message or "")


# --- cost estimator ------------------------------------------------------


def test_estimate_cost_returns_positive() -> None:
    decl = _make_declaration()
    estimate = estimate_cost(decl, [1000, 2000, 3000])
    assert isinstance(estimate, CostEstimate)
    assert estimate.total_usd > 0
    assert estimate.per_source_usd > 0
    assert estimate.source_count == 3


def test_estimate_cost_scales_with_input_size() -> None:
    decl = _make_declaration()
    small = estimate_cost(decl, [100])
    large = estimate_cost(decl, [100_000])
    assert large.total_usd > small.total_usd
