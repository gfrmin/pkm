# SPEC.md — technical specification

Version: 0.1.3 (draft)
Status: Foundation for Phase 1 (extraction layer with content-addressed
caching). This spec is the contract. Changes require a separate commit
with justification.

This spec is intentionally strict. See §14 for the principles of
first-principles debuggability that govern every decision below.

## 1. Scope of this specification

This document specifies the foundation layer: content-addressed cache,
catalogue, canonicalisation rules, and the extraction pipeline that
runs over raw documents. It does not specify LLM transforms, query
planning, or any higher-layer concerns — those live in later spec
versions once the foundation is stable.

## 2. Core concepts

### 2.1 Source

A source is a file in the user's filesystem that the user considers
input material. Sources are immutable from the system's perspective:
the system never modifies source files. Sources are identified by
the SHA-256 hash of their byte content.

### 2.2 Producer

A producer is a named, versioned piece of code that consumes inputs
and produces outputs. Extractors are producers; LLM transforms (in
later phases) will also be producers. Every producer has:

- `name`: a stable identifier (e.g., `pandoc`, `docling`).
- `version`: a semantic version that changes whenever the producer's
  behaviour changes.
- `config`: a dict of parameters controlling the producer's behaviour.

### 2.3 Artifact

An artifact is the output of a producer applied to an input. Every
artifact has a cache key computed from its inputs and producer
identity. Artifacts are immutable once written.

### 2.4 Cache

The cache is the content-addressed store of all artifacts. Artifacts
are written to paths derived from their cache keys. The cache is an
append-only, content-addressed filesystem layout. Nothing in the cache
is ever modified in place.

### 2.5 Catalogue

The catalogue is a DuckDB database recording metadata about sources,
producers, and artifacts: what exists, where, produced by whom, when,
with what status. The catalogue is mutable and can be rebuilt from
the cache. The catalogue is the index; the cache is the storage.

## 3. Directory layout

All state lives under a single root directory, configured in
`config.yaml` as `root_dir`. Default: `~/knowledge/`.

```
<root_dir>/
├── sources/              # manifests describing source locations
│   └── sources.yaml      # registry of source paths (not the files
│                         # themselves — sources live where they live)
├── cache/                # content-addressed artifact storage
│   └── <aa>/             # first 2 hex chars of key
│       └── <bb...>/      # remaining 62 hex chars as directory
│           ├── content   # the artifact itself
│           └── meta.json # producer identity, timestamps
├── catalogue.duckdb      # metadata database
├── config.yaml           # configuration (single file)
└── logs/                 # structured logs
    └── <YYYY-MM-DD>.jsonl
```

Rationale for the cache layout: `<aa>/<bb...>/` prevents any single
directory from accumulating millions of entries (standard practice
from git, IPFS, Nix). Using `<bb...>/` as a directory with a `content`
file inside (rather than a file named `<bb...>`) allows producer
metadata to sit alongside the artifact without a separate index.

## 4. Cache key format

### 4.1 Canonicalisation

Any structured data entering a hash function MUST be canonicalised
using:

```python
json.dumps(obj, sort_keys=True, separators=(',', ':'),
           ensure_ascii=False)
```

Any deviation from this canonicalisation is a bug.

### 4.2 Cache key computation

The cache key for an artifact is:

```
cache_key = sha256(canonical_json({
    "schema_version": 1,
    "input_hash": <sha256 of the input content>,
    "producer_name": <string>,
    "producer_version": <string>,
    "producer_config_hash": <sha256 of canonicalised config dict>,
})).hexdigest()
```

Crucially, `input_hash` is the hash of the input's **content**, not
the cache key of the input (if the input is itself a cached artifact).
This ensures that two producers which happen to produce byte-identical
outputs are recognised as equivalent inputs to downstream transforms.

### 4.3 The single hashing function

All cache keys MUST be computed by a single utility function:

```python
def compute_cache_key(
    input_hash: str,
    producer_name: str,
    producer_version: str,
    producer_config: dict,
) -> str:
    ...
```

No other code path may construct cache keys. No ad-hoc hashing.

### 4.4 What must NOT be in the cache key

The following MUST be excluded from cache key construction:

- Timestamps, wall-clock times
- Request IDs, run IDs, session IDs
- User identity, hostname, IP address
- Paths to files (only content hashes)
- API keys, credentials
- Retry counts, latency measurements

These destroy hit rates without improving correctness.

## 5. Catalogue schema

Schema version: 1

### 5.1 Tables

```sql
CREATE TABLE schema_meta (
    schema_version INTEGER PRIMARY KEY,
    migration_id   VARCHAR NOT NULL,  -- migration filename, e.g. '0001_initial_schema.py'
    migration_hash VARCHAR NOT NULL,  -- SHA-256 hex of the migration file at apply time
    applied_at     TIMESTAMP NOT NULL
);
-- One row per applied migration. The "current" schema version is
-- MAX(schema_version); the full history of schema applications is
-- the full row set (see §14.8 on migration hash verification).

CREATE TABLE sources (
    source_id     VARCHAR PRIMARY KEY,  -- SHA-256 of content
    current_path  VARCHAR NOT NULL,      -- most recently observed path
    first_seen    TIMESTAMP NOT NULL,
    last_seen     TIMESTAMP NOT NULL,
    size_bytes    BIGINT NOT NULL,
    mime_type     VARCHAR,               -- as detected, nullable
    tags          VARCHAR[]              -- user-applied tags
);

CREATE TABLE source_paths (
    source_id     VARCHAR NOT NULL,
    path          VARCHAR NOT NULL,
    seen_at       TIMESTAMP NOT NULL,
    PRIMARY KEY (source_id, path),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);
-- One source may have been seen at multiple paths over time
-- (moves, renames, copies). The history is kept.

CREATE TABLE artifacts (
    cache_key              VARCHAR PRIMARY KEY,
    input_hash             VARCHAR NOT NULL,
    producer_name          VARCHAR NOT NULL,
    producer_version       VARCHAR NOT NULL,
    producer_config_hash   VARCHAR NOT NULL,
    status                 VARCHAR NOT NULL,  -- 'success' | 'failed'
    produced_at            TIMESTAMP NOT NULL,
    size_bytes             BIGINT,            -- null if failed
    error_message          VARCHAR,           -- non-null iff failed
    content_type           VARCHAR,           -- MIME or producer-specific
    content_encoding       VARCHAR,           -- e.g. 'utf-8', null for binary
    content_path           VARCHAR NOT NULL   -- relative to cache dir
);

CREATE INDEX idx_artifacts_input ON artifacts(input_hash);
CREATE INDEX idx_artifacts_producer
    ON artifacts(producer_name, producer_version);
CREATE INDEX idx_artifacts_status ON artifacts(status);
```

### 5.2 Invariants

- Every row in `artifacts` with `status='success'` has a
  corresponding file at `<root>/cache/<content_path>/content`.
- Every row in `artifacts` with `status='failed'` has a non-null
  `error_message` and a null `size_bytes`.
- `input_hash` in `artifacts` is either a `source_id` in `sources`
  or a `cache_key` in `artifacts` (we don't enforce this as a FK
  because the input might be a source we haven't ingested yet in
  some edge case — but it SHOULD resolve).
- `producer_config_hash` is `sha256(canonical_json(config_dict))`.

### 5.3 Rebuilding from the cache

There MUST be a `rebuild_catalogue` command that walks the cache
directory and reconstructs the `artifacts` table from the `meta.json`
files. Rebuild does NOT reconstruct the `sources` or `source_paths`
tables — those contain observational data (paths, timestamps, sizes,
MIME types, user tags) that cannot be recovered from the cache alone.
To repopulate `sources` after a rebuild, the user re-runs `pkm ingest`.

This keeps the responsibilities of rebuild and ingest crisply
separated: rebuild is for artifact derivations recorded in the cache,
ingest is for observations about source files on the filesystem.

## 6. Operations

### 6.1 Idempotency

Every operation MUST be idempotent. The test: running the operation
twice, the second run produces zero new writes to cache or catalogue.

Operations that may legitimately be non-idempotent (e.g., explicit
re-extraction with `--force`) must be flagged and require the flag
to be non-idempotent.

### 6.2 Atomicity

Writes to the cache and the corresponding catalogue row MUST be
logically atomic: on the next successful run of the system, no cache
directory exists without a matching catalogue row, and no catalogue
row exists without its cache files. This is the visible invariant that
downstream code relies on.

The filesystem and the DuckDB catalogue cannot share a single
transaction, so the invariant is maintained by ordering plus an
explicit orphan sweep. The write order is:

1. Write `content` (byte file) to its final location.
2. Write `meta.json` beside it.
3. Open a DuckDB transaction, insert the `artifacts` row, commit.

If the process is interrupted between any of these steps, the on-disk
state may contain an orphan cache directory (content and/or meta.json
present without a catalogue row). Orphans are removed by a consistency
sweep that runs at the start of every `pkm extract` and every
`pkm rebuild-catalogue` invocation. "Start of invocation" is the only
meaningful notion of startup here — there is no daemon (SPEC §14.6).
Other commands (`pkm ingest`, `pkm migrate`) do not touch the cache
and therefore do not run the sweep.

The sweep is conservative: a cache directory is considered orphaned
iff (a) it contains a `content` file or a `meta.json` file, and (b)
no row in `artifacts` has `cache_key` equal to the directory name.
Orphan directories are removed; the event is logged.

### 6.3 Transactions

All multi-row catalogue operations use DuckDB transactions explicitly.
No implicit autocommit for multi-row logic.

## 7. Producers (Phase 1 extractors)

### 7.1 Common interface

Every producer implements:

```python
class Producer(Protocol):
    name: str
    version: str

    def produce(
        self,
        input_path: Path,
        input_hash: str,
        config: dict,
    ) -> ProducerResult:
        ...


@dataclass(frozen=True)
class ProducerResult:
    status: Literal["success", "failed"]
    content: bytes | None           # None iff failed
    content_type: str | None        # MIME or producer-specific; required on success
    content_encoding: str | None    # e.g. 'utf-8'; None for binary artifacts
    error_message: str | None       # None iff success
    producer_metadata: dict         # written to meta.json; may be empty
```

Producers MUST:

- Return `bytes` for `content` (never `str`). If the output is text,
  the producer encodes it and records the encoding in
  `content_encoding`.
- Never raise exceptions that escape `produce()`. Any failure is
  caught and returned as `status='failed'` with a message.
- Be deterministic given the same input *content* (as identified by
  `input_hash`) and the same `config`. `input_path` is an I/O handle
  used to read the bytes at call time; it is NOT part of the
  determinism contract. Two machines with the same byte content at
  different paths MUST produce the same output. If a producer is
  non-deterministic by nature (e.g., later LLM producers), the
  randomness source MUST appear in `config`.

### 7.2 Initial extractors

Phase 1 ships with exactly three extractors:

- `pandoc` — fast, baseline, handles common formats
- `docling` — sophisticated, handles layout and tables
- `unstructured` — broad format coverage including email

No plugin system. These are three concrete imports. When a fourth
is needed, we'll consider abstraction.

### 7.3 Routing

A simple routing policy decides which extractor to run for a given
source. Phase 1 policy:

1. Pandoc on everything.
2. Docling on: anything Pandoc failed on, plus anything tagged as
   layout-sensitive (`invoice`, `report`, `contract` — by user tag).
3. Unstructured on: anything Docling can't handle (email formats,
   odd formats), plus anything both others failed on.

The policy is implemented as a single Python function. No rule
engine, no configuration. When it becomes complex, we'll revisit.

## 8. Source registration

### 8.1 The sources.yaml manifest

```yaml
version: 1
sources:
  - path: ~/Documents/legal/velotix/complaint-2025-03.pdf
    tags: [legal, velotix]
  - path: ~/Sync/career/CV_v9.docx
    tags: [cv, career]
  - path: ~/Documents/medical/
    tags: [medical]
    recursive: true
```

Paths may be files or directories. If a directory with
`recursive: true`, all files within are sources.

### 8.2 Ingestion

The `ingest` command:

1. Reads `sources.yaml`.
2. For each path, computes the content hash.
3. If the `source_id` is new, creates a row in `sources`.
4. If the path is new for an existing `source_id`, records it in
   `source_paths`.
5. Updates `last_seen` on existing sources.

Ingestion does NOT run extractors. It only registers sources in
the catalogue.

## 9. Configuration

A single file, `config.yaml`:

```yaml
version: 1
root_dir: ~/knowledge
log_level: INFO
extractors:
  pandoc:
    version: "3.1.9"    # used in cache keys; must match installed
    config: {}
  docling:
    version: "2.14.0"
    config:
      ocr: true
      table_structure: true
  unstructured:
    version: "0.16.0"
    config:
      strategy: auto
```

Version strings in config are used in cache keys. Mismatch between
config version and actually installed version is a startup error.

## 10. Logging

Structured JSON logs, one file per day, in `<root>/logs/`. Every
log line includes:

- `timestamp`: ISO 8601 with timezone
- `level`: DEBUG | INFO | WARNING | ERROR
- `component`: which module
- `event`: short event name (e.g., `cache_hit`, `extraction_started`)
- `source_id` or `cache_key` if applicable
- `message`: human-readable

No `print()` in library code. CLI entry points may print to stdout
for user output, but structured events still go to the log.

## 11. Backup and recovery

The cache, catalogue, and configuration are covered by BorgBackup.
The cache is reproducible from sources + code, but re-running all
extractors is expensive, so the cache is backed up rather than
regenerated on loss.

The catalogue is rebuildable from the cache via `rebuild_catalogue`.
This command MUST exist and MUST be tested.

Source files themselves are outside this system's scope for backup
(they live where they live, under the user's existing backup policy).

## 12. What is explicitly out of scope for this spec version

- LLM transforms (Phase 2)
- Query planning (Phase 2)
- Embeddings and vector search (Phase 2)
- Web UI, dashboards (not planned)
- Parallelism (not planned for Phase 1)
- Plugin architecture for extractors (not planned until 4+ extractors)
- Multi-user support (single-user system)
- Remote cache, distributed execution (local only)

## 13. Resolved design decisions

These were open questions during drafting; they are now fixed
decisions. Each carries a brief rationale because understanding
why we chose is as important as what we chose.

### 13.1 `meta.json` is authoritative; catalogue is rebuildable

Every cache entry stores its full metadata in `meta.json` alongside
the `content` file. The catalogue is a derived index that can be
rebuilt by walking the cache directory. This means:

- `meta.json` contains everything needed to reconstruct the
  `artifacts` row: `cache_key`, `input_hash`, `producer_name`,
  `producer_version`, `producer_config`, `producer_config_hash`,
  `status`, `produced_at`, `size_bytes`, `error_message`,
  `producer_metadata`.
- The catalogue is never the sole source of truth for artifact
  data. Losing the catalogue is inconvenient but not catastrophic.
- `rebuild_catalogue` walks `<root>/cache/`, reads every
  `meta.json`, and reconstructs the `artifacts` table from scratch.

Rationale: the cache is the foundational record; the catalogue
exists only to make it queryable. Keeping `meta.json` authoritative
means the system degrades gracefully under catalogue corruption
and that a user can forensically inspect any artifact without
touching the database.

### 13.2 Deleted sources remain as ghosts

If a source file disappears from its recorded path:

- The `sources` row is retained.
- `last_seen` is not updated (it records the last time we
  observed the file existed).
- No artifacts are cascade-deleted.
- `ingest` logs a WARNING when a recorded path no longer resolves.

Rationale: artifacts are valid derivations of content that existed
at a specific time. Deleting them because the source file was
moved or deleted destroys the derivation history. The catalogue
is a log, not a live index of the filesystem.

A future `prune` command MAY offer opt-in removal of ghost sources
and their artifacts, but only behind an explicit flag and with a
dry-run preview. Phase 1 does not implement this.

### 13.3 Artifacts are arbitrary bytes

The cache stores bytes. Producers may emit plaintext, JSON,
structured binary formats (Docling's native format), images,
embeddings (as binary arrays), audio, or anything else.

- The `content` file is written verbatim as bytes.
- `meta.json` records a `content_type` field (MIME type or a
  producer-specific identifier) to help consumers interpret it.
- No transformation (encoding normalisation, compression,
  re-serialisation) is applied between producer output and
  cache write.

Rationale: constraining Phase 1 to text would require rewriting
cache primitives when Phase 2 adds embeddings. Bytes is the
most general abstraction; interpretation belongs one layer up.

### 13.4 Paths that never resolved

Distinct from §13.2 (which covers sources that *were* seen and then
vanished): a `sources.yaml` entry whose path has never resolved to a
readable file produces a WARNING log event and is skipped. No
`source_id` is created, because there is no content to hash; the
`sources` and `source_paths` tables are unaffected.

A path resolving to something unreadable — file not found, permission
denied, broken symlink, device or socket file, a directory when
`recursive: true` is not set — is treated under this section rather
than as an ingest failure. The behaviour is uniform: WARNING + skip.
Ingest MUST NOT halt because of an unreadable entry; subsequent
entries are processed normally. If the path later becomes readable,
the next `ingest` run treats it as a first-time source and creates a
`source_id` at that point.

Rationale: sources are aspirational when declared in `sources.yaml`
and become real only when their bytes are read. A declaration that
never corresponded to readable bytes is noise, not a failure. Halting
ingest on the first bad path would block progress on the rest of the
manifest and invite ad-hoc retry mechanisms; a WARNING line in the
log is already sufficient debug evidence per §14.2.

## 14. Strictness and first-principles debuggability

This project is intentionally strict. The following rules exist to
ensure that any state of the system can be understood and debugged
from first principles without recourse to tribal knowledge.

### 14.1 Every state is inspectable with standard tools

- The cache is a directory tree. Any artifact can be examined with
  `cat`, `file`, `jq`, `xxd`.
- The catalogue is a DuckDB file. Any state can be queried with
  the `duckdb` CLI.
- Logs are JSON Lines. Any event can be filtered with `jq` or
  `grep`.
- Configuration is a single YAML file. No environment variables,
  no runtime overrides, no magic.

At no point does the system rely on state that is not directly
visible in these four locations.

### 14.2 Every operation is traceable

Every cache write produces a log event that records the cache key,
input hash, producer identity, and the config hash. Given a cache
key, the user can always answer "why does this artifact exist?"
from logs alone.

### 14.3 Failures are recorded, not lost

A failed producer run writes a `meta.json` with `status: failed`
and an `error_message`. Failed artifacts occupy cache space so
that re-running the producer on the same input is a cache hit
(returning the failure) rather than a repeated attempt. An
explicit `--retry-failed` flag is required to re-attempt.

Rationale: implicit retry of failures is a debugging nightmare.
Explicit retry means the user always knows why work is happening.

### 14.4 Hash prefixes are unambiguous

All hashes are full SHA-256 hex (64 characters). No truncation
in identifiers, cache keys, or catalogue columns. Truncation
saves a few bytes and creates collision bugs.

Display code (log output, CLI output) MAY show truncated hashes
for readability, but always with a clear prefix convention
(e.g., `abc123…` with an ellipsis). Never silently truncate
in stored data.

### 14.5 Version strings are exact

Every version string in configuration is an exact match against
the installed tool's reported version. Startup verifies this and
fails loudly on mismatch:

- `pandoc --version` reports version; must match `config.yaml`.
- `python -c "import docling; print(docling.__version__)"` must
  match config.
- Similarly for every other producer.

Version matches are not "semver-compatible" or "at least this
version" — they are exact. Cache keys depend on them, so drift
without explicit acknowledgement is a correctness bug.

### 14.6 No hidden state

The system maintains no hidden caches, no in-memory state that
survives a process, no background daemons. Every invocation starts
from the on-disk state and ends having written its changes to
the on-disk state. This guarantees that understanding the system
requires only understanding its on-disk layout.

### 14.7 No implicit conversions

Byte outputs are stored as bytes. Text outputs are stored with an
explicit declared encoding in `meta.json`. No auto-detection of
encoding at read time. No silent UTF-8 assumption. If a producer
emits Latin-1, that fact is recorded and consumers must handle it
explicitly.

### 14.8 Schemas are versioned, migrations are explicit

Every catalogue table has a `schema_version`. Every JSON format
has a `format_version`. Migrations between versions are explicit
Python functions in a `migrations/` directory, applied in order,
each logged. No automatic migration on startup — the user runs
`migrate` explicitly and sees what changes.

**Migration hash verification.** For every migration it has applied,
the `schema_meta` table stores the migration filename and the
SHA-256 hash of the migration file at the moment it was applied
(see §5.1). The migration runner recomputes the on-disk hash of
every previously-applied migration and compares it to the stored
hash before applying any new migrations. A mismatch — the file has
been edited after application, replaced with different content, or
removed — aborts with a clear error identifying the affected
migration and rejecting any further work. This is the same class of
paranoia as §14.5 version matching: the schema's derivation history
must remain reproducible from the source tree, so applied migrations
are immutable by policy. If a schema change is needed, a new
numbered migration is added in sequence.

## 15. Change log

- 0.1.0 (draft): Initial specification covering Phase 1 foundation.
- 0.1.1 (draft): Resolved §13 design decisions; added §14 on
  strictness and first-principles debuggability.
- 0.1.3 (draft): Migration hash verification and schema_meta
  extension. §5.1 extends `schema_meta` with `migration_id` and
  `migration_hash` columns so each row records which migration
  produced the schema version and what that migration file hashed to
  at apply time. §14.8 adds a new paragraph mandating that the
  migration runner re-hashes every previously-applied migration
  on every run and aborts loudly on mismatch — the same class of
  paranoia as §14.5 version matching. Rationale: without the stored
  filename + hash, "schema_meta records the current state" conflates
  with "schema_meta records the path taken to get here", and there
  is no way to detect an applied migration being edited in-place.
  The conflation is resolved by making `schema_meta` a true log of
  applications rather than a one-row version marker.
- 0.1.2 (draft): Four edits resolving ambiguities surfaced during
  Phase 1 implementation planning:
    - §5.3 narrowed to rebuild `artifacts` only; `sources` is
      repopulated by re-running `pkm ingest`. Rationale: `sources`
      rows carry observational data that cannot be reconstructed
      from cache alone, so the only honest rebuild is artifact-only.
    - §6.2 rewritten to state the visible invariant explicitly
      ("no catalogue row without files, no files without a catalogue
      row on next run") and to name the commands that run the orphan
      sweep (`pkm extract`, `pkm rebuild-catalogue`). Rationale: the
      previous "single transaction" phrasing conflated FS and DB
      atomicity, and "on next run" had no clear referent without a
      daemon.
    - §7.1 determinism contract clarified: deterministic given the
      same input *content* (by `input_hash`) and `config`;
      `input_path` is an I/O handle, not part of the contract.
      Rationale: paths differ by machine, so the old wording could
      be read to permit path-dependent behaviour, which would break
      cross-machine cache parity.
    - §13.4 added to cover paths that never resolved (including
      unreadable, permission-denied, broken-symlink, device-file
      cases). Rationale: previously conflated with §13.2 ghost
      behaviour; separating them prevents ad-hoc retry logic and
      clarifies that ingest never halts on bad manifest entries.
