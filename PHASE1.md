# Phase 1 — retrospective

Phase 1 built the foundation: a content-addressed extraction cache, a DuckDB
catalogue that rebuilds from the cache, and three producers (`pandoc`,
`docling`, `unstructured`). The data flow is `ingest → route → extract →
(cache + catalogue)`. Source of truth is on disk; the catalogue is a
queryable index. See `SPEC.md` for the contract.

## Key invariants

1. **One hashing function.** All cache keys are built by `compute_cache_key()`
   in `src/pkm/hashing.py`. No ad-hoc hashing anywhere else. SPEC §4.3.
2. **Cache-key determinism is about input identity, not output identity.**
   The key hashes `(input_hash, producer_name, producer_version,
   producer_config_hash)`. Two byte-identical inputs at different paths must
   share a key; a producer's output may still differ bit-for-bit on rerun
   (Docling's bbox coordinates do). SPEC §4.2, §7.1 — rewritten in v0.1.8
   (`e5224c0`) after Step 7h surfaced the issue; `--verify` was removed
   (`eae013f`).
3. **Atomicity across cache + catalogue.** Write order is content →
   `meta.json` → catalogue row in one DuckDB transaction. Orphan cache
   directories (content without a row, after a crash) are swept on every
   `pkm extract` or `pkm rebuild-catalogue` startup. The inverse — a row
   without files — is a hard error requiring explicit `rebuild-catalogue`.
   SPEC §6.2.
4. **Migrations are hash-verified and immutable once applied.** Editing a
   landed migration in-place aborts the next run. SPEC §5.1, §14.8.

## Decisions worth remembering

- The determinism contract was **deliberately relaxed** in v0.1.8. Future-self
  will be tempted to re-tighten it to byte-equality; don't. Semantic
  equivalence is what producers can honestly offer.
- `completion` (`"complete" | "partial_timeout" | "partial_other"`) lives in
  `producer_metadata`, not a catalogue column. Low prevalence; promoting it
  would be premature schema churn. Revisit when a second structured key
  earns its keep.
- **Subprocess isolation per producer is deferred** despite Step 7h finding
  a 12 MB PDF that OOM-killed Docling at ~24 GB RSS. One recurring failure
  mode doesn't clear the evidence bar; a category of them might. SPEC §7.1
  documents this as an uncatchable failure mode (v0.1.9, `bb3cb83`).
- Routing is a pure function in `src/pkm/routing.py`. Three producers don't
  justify a rule engine or plugin registry. SPEC §7.2 — no abstraction until
  a fourth concrete implementation exists.

## The four runs

Under `~/yo/pkm/runs/phase1/`:

- `100doc-2026-04-20/` — 100-source smoke test.
- `500doc-2026-04-22/` — 499-source mid-scale shakedown. Two `partial_timeout`
  artifacts backfilled post-hoc (`b0d807e`).
- `1000doc-2026-04-22/` — 990-source stratified sample (Step 7h). Surfaced
  `.org` silent-skip (fixed, `4d7c6a5`), encrypted-PDF handling
  (`9ceb280`), the Docling OOM, and the determinism correction.
- `full-2026-04-22/` — 2,107-source full-corpus extraction. `~/yo/pkm/live`
  symlinks here.

**Gap:** the 1000-doc stratified sample seed is not checked in. The run is
not bit-reproducible without it.

## Known-failed source categories

- Encrypted PDFs (categorised via `pikepdf` pre-flight, `9ceb280`).
- Memory-pathological PDFs (one 12 MB case observed; Docling-specific).
- Format coverage gaps — `.xml` with no claimant. Surfaced via SPEC §14.3
  (v0.1.10, `fc5157c`); not promoted to first-class representation yet.
- Legacy binary `.doc` — deliberately unsupported by Pandoc; Unstructured
  may pick it up.

## Deliberately deferred

- Subprocess isolation per producer call (SPEC §7.1).
- Per-document memory bounds (SPEC §7.1).
- Pre-flight size estimation (SPEC §7.1).
- `prune` command for ghost sources (SPEC §13.2).
- A `csv` producer preserving columnar structure.
- First-class representation of unattempted sources (SPEC §14.3 names the
  query; doesn't promote to a column).

## What Phase 2 should inherit

- The structured `producer_metadata` pattern (`completion` today;
  `confidence`, `uncertainty` are candidates for ML outputs).
- The SPEC v0.1.9 uncatchable-failure-modes paragraph as the template for
  documenting what the system cannot catch.
- Sampling discipline: seeded, reproducible, proportional stratification,
  no overlap between runs. The absent 1000-doc seed is the one unpaid debt
  here — Phase 2 samples check in their seeds.
