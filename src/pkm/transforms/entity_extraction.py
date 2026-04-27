"""Entity extraction transform producer (SPEC v0.2.0 §19-§20).

Concrete ``TransformProducer`` that sends extracted text through
Anthropic Haiku 4.5 for named-entity recognition.  Output conforms
to the ``entity_extraction_v1`` JSON schema (§19.3).

Uses Anthropic's Structured Outputs (``output_config``) to constrain
model output to a derived schema.  The canonical schema
(``schemas/entity_extraction_v1.json``) retains all constraints
for client-side ``jsonschema`` validation; the transmitted schema
has unsupported properties stripped by ``_strip_unsupported_for_api``.
"""

from __future__ import annotations

import copy
import json
import logging
import time
from typing import Any

import anthropic

from pkm.policy import CostEstimate
from pkm.transform import ModelResponse, TransformProducer
from pkm.transform_declaration import TransformDeclaration

logger = logging.getLogger(__name__)

_HAIKU_INPUT_PRICE_PER_MTOK = 0.80
_HAIKU_OUTPUT_PRICE_PER_MTOK = 4.00

_CHARS_PER_TOKEN = 4

_UNSUPPORTED_KEYS: frozenset[str] = frozenset({
    "$schema",
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
    "multipleOf",
    "minLength", "maxLength",
    "maxItems", "uniqueItems",
    "oneOf", "not",
    "if", "then", "else",
    "prefixItems",
})


def _strip_unsupported_for_api(schema: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *schema* safe to send to Anthropic's API.

    Removes JSON Schema keywords the API doesn't support, and adds
    ``additionalProperties: false`` to every ``object`` type (required
    by the API).  The canonical schema retains the full constraints
    for client-side validation.
    """
    out: dict[str, Any] = {}
    for key, value in schema.items():
        if key in _UNSUPPORTED_KEYS:
            continue
        if key == "minItems" and isinstance(value, int) and value > 1:
            continue
        if isinstance(value, dict):
            out[key] = _strip_unsupported_for_api(value)
        elif isinstance(value, list):
            out[key] = [
                _strip_unsupported_for_api(item)
                if isinstance(item, dict) else item
                for item in value
            ]
        else:
            out[key] = value

    if out.get("type") == "object" and "additionalProperties" not in out:
        out["additionalProperties"] = False

    return out


def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    return (
        input_tokens * _HAIKU_INPUT_PRICE_PER_MTOK / 1_000_000
        + output_tokens * _HAIKU_OUTPUT_PRICE_PER_MTOK / 1_000_000
    )


def estimate_cost(
    declaration: TransformDeclaration,
    input_sizes: list[int],
) -> CostEstimate:
    """Estimate the total cost of running entity extraction.

    Token counts are approximated from character counts.  Output
    allowance uses ``max_tokens`` from the model identity.  The
    estimate is deliberately conservative (overestimates).
    """
    prompt_tokens = len(declaration.prompt_text) // _CHARS_PER_TOKEN
    max_output = declaration.model_identity.get(
        "inference_params", {},
    ).get("max_tokens", 4096)

    total = 0.0
    for size in input_sizes:
        input_toks = prompt_tokens + size // _CHARS_PER_TOKEN
        total += _compute_cost(input_toks, max_output)

    per_source = total / len(input_sizes) if input_sizes else 0.0
    return CostEstimate(
        total_usd=total,
        per_source_usd=per_source,
        source_count=len(input_sizes),
    )


class EntityExtractionProducer(TransformProducer):
    """Named-entity extraction via Anthropic Haiku 4.5.

    Uses Structured Outputs (``output_config``) to constrain the
    model's response to the transmitted schema.  Client-side
    ``jsonschema`` validation against the canonical schema enforces
    constraints the API doesn't support (e.g. numeric ranges).
    """

    name = "entity_extraction"
    version = "0.2.0"

    def __init__(
        self,
        *,
        declaration: TransformDeclaration,
        client: anthropic.Anthropic | None = None,
    ) -> None:
        self.model_identity: dict[str, Any] = declaration.model_identity
        self.prompt_name = declaration.prompt_name
        self.output_schema: dict[str, Any] = declaration.output_schema
        self._prompt_template = declaration.prompt_text
        self._model = declaration.model_identity["model"]
        self._inference_params = declaration.model_identity.get(
            "inference_params", {},
        )
        self._client = client or anthropic.Anthropic()
        self._api_schema = _strip_unsupported_for_api(
            copy.deepcopy(declaration.output_schema),
        )

    def render_prompt(
        self, input_content: bytes, input_metadata: dict[str, Any],
    ) -> str:
        text = input_content.decode("utf-8", errors="replace")
        return self._prompt_template.replace("{text}", text)

    def call_model(self, prompt: str) -> ModelResponse:
        t0 = time.monotonic()
        response = self._client.messages.create(
            model=self._model,
            max_tokens=self._inference_params.get("max_tokens", 4096),
            temperature=self._inference_params.get("temperature", 0.0),
            messages=[{"role": "user", "content": prompt}],
            output_config={
                "format": {
                    "type": "json_schema",
                    "schema": self._api_schema,
                },
            },
        )
        latency_ms = int((time.monotonic() - t0) * 1000)

        raw_text: str = response.content[0].text  # type: ignore[union-attr]
        input_tokens: int = response.usage.input_tokens
        output_tokens: int = response.usage.output_tokens
        cost_usd = _compute_cost(input_tokens, output_tokens)

        return ModelResponse(
            raw_text=raw_text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
        )

    def parse_output(self, raw_output: str) -> dict[str, Any]:
        return json.loads(raw_output)  # type: ignore[no-any-return]

    def post_validate(
        self, parsed: dict[str, Any], input_content: bytes,
    ) -> None:
        """Span-index and span-text validation (PHASE2.md §4.5)."""
        text = input_content.decode("utf-8", errors="replace")
        text_len = len(text)

        for i, entity in enumerate(parsed.get("entities", [])):
            span = entity.get("span", {})
            start = span.get("start", 0)
            end = span.get("end", 0)

            if start < 0 or end < 0:
                raise ValueError(
                    f"entity[{i}]: negative span index"
                )
            if start > text_len or end > text_len:
                raise ValueError(
                    f"entity[{i}]: span out of range "
                    f"(text length {text_len})"
                )
            if start >= end:
                raise ValueError(
                    f"entity[{i}]: span start ({start}) >= end ({end})"
                )
            actual = text[start:end]
            expected = entity.get("text", "")
            if actual != expected:
                raise ValueError(
                    f"entity[{i}]: span text mismatch: "
                    f"expected {expected!r}, got {actual!r}"
                )
