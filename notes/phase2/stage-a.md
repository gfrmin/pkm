# Stage A retrospective -- Phase 2 transform substrate

**Date:** 2026-04-26
**Scope:** SPEC v0.2.0 amendment, cache-key extension, storage layout,
catalogue migration, TransformProducer ABC, telemetry, policy engine,
HITL approval data model, `pkm transform` CLI surface, invariant
tests 9-13, exit criterion test.

## What was built

1. **SPEC v0.2.0** -- contract document for Phase 2 LLM transforms.
   Rewrote section 17.1 to fix a backward-compatibility bug in cache-key
   computation: `schema_version` is now a payload-format discriminator
   (v1 = 5-field for extractors, v2 = 7-field for transforms).

2. **Cache-key extension** (`hashing.py`) -- `compute_cache_key` gains
   `schema_version`, `model_identity`, and `prompt_hash` keyword args.
   v1 path unchanged (golden key preserved). v2 adds
   `model_identity_hash` and `prompt_hash` to the canonical-JSON payload.

3. **Migration 0003** -- `artifact_lineage`, `pending_approvals`,
   `approval_sources`, `approval_samples`, `approval_reasons` tables.
   Hash-verified and idempotent.

4. **Storage layout** (`cache.py`, `rebuild.py`) -- `write_artifact`
   extended with `lineage` and `cache_key_schema_version`. Write order:
   content -> lineage.json -> meta.json -> catalogue row. Rebuild
   reconstructs `artifact_lineage` from `lineage.json` files. Corruption
   check: transform-specific fields without `cache_key_schema_version`
   in `meta.json` is a fatal error.

5. **TransformProducer** (`transform.py`) -- ABC with
   `render_prompt`/`call_model`/`parse_output` abstractions. `produce`
   orchestrates the pipeline, validates output against JSON Schema, and
   normalises all failures to `ProducerResult(status="failed")`.

6. **Transform declarations** (`transform_declaration.py`) -- YAML
   loader for `<root>/transforms/<name>.yaml`. Resolves prompt and
   schema files, computes hashes.

7. **Telemetry** (`telemetry.py`) -- JSONL append to
   `<root>/logs/transforms/<YYYY-MM-DD>.jsonl`.

8. **Policy engine** (`policy.py`) -- `Allow | Block | RequireApproval`
   ADT with sequential evaluation. First Block short-circuits; any
   RequireApproval promotes.

9. **Approval CRUD** (`approval.py`) -- create/approve/reject/get/
   list_pending lifecycle against catalogue tables.

10. **CLI** -- `pkm transform {list,run,approve,reject,status,show}`.
    `run` is stubbed for Stage A; other commands wire through to modules.

11. **236 tests** passing, lint clean, type-clean.

## What surprised

- **DuckDB FK limitation**: within a single transaction, the effects of
  a DELETE on a child table are not visible to a subsequent DELETE on the
  parent table for FK constraint checking. Required splitting lineage
  deletes outside the main transaction in both `delete_artifact` and
  `rebuild_artifacts`. This is a documented DuckDB limitation, not a bug.

- **SPEC section 17.1 bug** was caught during planning, before any code.
  The original "empty-string hash preserves keys" claim was wrong --
  adding fields to the canonical-JSON payload changes the hash regardless
  of values. The fix (schema_version as discriminator) was cleaner than
  any backward-compat hack.

## SPEC divergences

None. The Stage A implementation matches SPEC v0.2.0 as written. Three
design decisions (explicit schema_version param, meta.json format_version
stays at 1, UUID4 approval IDs) are implementation-level only and do not
require SPEC changes.

## What's next (Stage B)

- Real `transform run` orchestration: load declaration, query eligible
  sources, evaluate policies, dispatch producer, write artifacts.
- First real LLM call via SDK (entity_extraction with Claude Haiku).
- Budget tracking and cost_gate policy implementation.
- Sample output generation for approval flow.
