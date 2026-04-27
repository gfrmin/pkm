# Stage B retrospective -- Entity extraction end-to-end

**Date:** 2026-04-27
**Scope:** First real LLM transform (entity extraction via Anthropic
Haiku 4.5), 50-source integration test with mock client, policy
evaluation (cost_gate, sensitive_doc_gate), transform orchestration,
structured-output migration to `producer_version: 0.2.0`.

## What was built

1. **EntityExtractionProducer** (`src/pkm/transforms/entity_extraction.py`)
   -- concrete `TransformProducer` that sends extracted text through
   Anthropic Haiku 4.5 for named-entity recognition. Uses Structured
   Outputs (`output_config`) at v0.2.0; the canonical JSON Schema
   retains all constraints for client-side `jsonschema` validation.

2. **Schema stripping** (`_strip_unsupported_for_api`) -- recursive
   function that produces an API-safe schema by removing unsupported
   keywords and injecting `additionalProperties: false` on all object
   types.

3. **Transform orchestration** (`src/pkm/transform_run.py`) -- full
   pipeline: declaration loading, policy evaluation, producer dispatch,
   artifact writing with lineage, telemetry logging, cache-hit
   idempotency, 2x cost abort guard.

4. **Policy loader** (`src/pkm/policy_loader.py`) -- loads policy
   callables from `<root>/policies/<name>.py` via importlib.

5. **Two policy implementations** (test fixtures) -- `cost_gate`
   (budget threshold) and `sensitive_doc_gate` (tag-based approval
   gating).

6. **288 tests** passing, lint clean, type-clean. 27 tests in
   `test_entity_extraction.py`, 9 in `test_transform_run.py`.

## Structured-output migration (0.1.0 -> 0.2.0)

Migrated from prompt-level JSON instructions + code-fence stripping to
Anthropic's GA Structured Outputs feature. Key changes:

- `call_model` sends `output_config={"format": {"type": "json_schema",
  "schema": <stripped>}}` to the API.
- `parse_output` simplified to bare `json.loads` (no fence stripping).
- `_strip_unsupported_for_api` added to derive the API schema from the
  canonical schema at init time.
- Producer version bumped from `0.1.0` to `0.2.0` (new cache keys;
  old artifacts remain accessible but distinct).

### Post-migration verification

#### 1. Post-validator pass rate, 0.1.0 vs 0.2.0

Both versions use the same deterministic mock client (`_find_entities`
regex extractor returning `input_tokens=100, output_tokens=50`). The
mock produces identical output regardless of whether the prompt
includes JSON-instruction preamble (0.1.0) or relies on `output_config`
(0.2.0) -- it ignores `output_config` entirely.

**Result:** 50/50 schema-valid and post-validate-pass under both
versions. Zero divergences. The failure sets are identical (empty).

This is a clean result but a weak signal: the mock bypasses the actual
structured-output constraint, so the pass rate reflects the mock's
correctness, not the API's. Real divergence testing requires a live API
call (deferred to Stage C adversarial pass with `@pytest.mark.llm`
tests).

#### 2. Schema-stripping surprises

`_strip_unsupported_for_api` strips 16 keys plus conditional handling
of `minItems`:

- **Numeric:** `minimum`, `maximum`, `exclusiveMinimum`,
  `exclusiveMaximum`, `multipleOf`
- **String:** `minLength`, `maxLength`
- **Array:** `maxItems`, `uniqueItems`; `minItems` stripped only when
  value > 1 (0 and 1 preserved)
- **Composition:** `oneOf`, `not`, `if`, `then`, `else`
- **Other:** `$schema`, `prefixItems`
- **Injected:** `additionalProperties: false` on every `object` type
  that doesn't already set it

**Missing from strip list (documented unsupported but not yet
stripped):**

- `pattern` -- JSON Schema string regex constraint. Not present in the
  entity extraction schema, so harmless now. Future transforms with
  regex-validated fields will need this added.
- `format` -- JSON Schema format annotation (e.g. `"date-time"`,
  `"email"`). Also absent from the entity extraction schema.

No unexpected keys were stripped beyond the documented set. The
function strips exactly what the Anthropic docs specify as
unsupported, with two documented-unsupported keywords (`pattern`,
`format`) absent because the entity extraction schema doesn't use
them. **Finding for Stage C:** add `pattern` and `format` to
`_UNSUPPORTED_KEYS` proactively, or at minimum when the next
transform's schema uses them.

#### 3. Cost estimator drift

The estimator uses `max_tokens` (4096) for output token allowance and
`len(text) // 4` for input token approximation. The mock client
returns fixed values (`input_tokens=100`, `output_tokens=50`) that
bear no relation to real API usage.

**Ratios (actual_mock_cost / estimated_cost):**

- Median: 0.0171
- Min: 0.0171
- Max: 0.0171
- Any > 2.0: No

The ~59x overestimate is entirely expected: the estimator budgets
4096 output tokens while the mock returns 50. This is by-design
conservatism per SPEC, not drift.

**Assessment:** The mock data is not informative for calibration.
The estimator's accuracy can only be evaluated against real API token
counts from a live run. The 2x abort guard (SPEC §22.4) cannot
trigger under the mock because the mock's actual cost is always far
below the estimate. **No action required now.** Defer calibration
to the first live `@pytest.mark.llm` run in Stage C or D. Per
PHASE2.md §4.2, the budget is "fix before Stage E if Stage D shows
>1.5x systematic drift."

## What's next (Stage C)

- Adversarial pass: craft inputs that stress the structured-output
  constraint (malformed spans, schema-edge cases, large documents).
- Live API test with `@pytest.mark.llm` marker.
- Add `pattern` and `format` to `_UNSUPPORTED_KEYS` if needed by the
  next transform's schema.
- Evaluate cost-estimator accuracy against real token counts.
