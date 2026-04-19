# CLAUDE.md — project rules for coding agents

This document governs how coding agents (Claude Code and similar) work on
this project. Read it at the start of every session. If anything here
conflicts with a later instruction in the conversation, raise the conflict
rather than silently resolving it.

## Philosophy

This project is building foundations. The cost of getting cache semantics
wrong is rebuilding the entire cache from scratch. The cost of writing
boring, correct, spec-compliant code is a few extra minutes.

"Pragmatic" is not a virtue here. If your instinct is to take a shortcut
because "we can always fix it later", the answer is almost always no.
Later is expensive; now is cheap.

## Before writing any code

1. Read `SPEC.md`. The spec is the contract. If what you are about to
   write isn't covered by the spec, stop and ask the user. Do not invent.
2. If what the user just asked for conflicts with `SPEC.md`, point out
   the conflict. Do not silently resolve it in favour of the most recent
   instruction.
3. If `SPEC.md` needs to change to accommodate a new requirement, update
   `SPEC.md` first (in a separate commit with a clear justification),
   then write the code that matches the updated spec.

## Things to refuse

Refuse these even if the user seems to want them. If the user insists,
explain the cost and get explicit confirmation before proceeding.

- **Hardcoded or human-readable filenames in the cache directory.**
  The cache is content-addressed. Every path is derived from a hash.
- **Ad-hoc hashing outside `compute_cache_key()`.**
  There is one hashing function. Everything goes through it.
- **Partial writes or non-atomic operations across cache and catalogue.**
  A cache entry without a catalogue row (or vice versa) is a bug.
- **Non-idempotent operations by default.**
  Running any command twice must produce the same result as running it
  once. `--force` flags are fine; non-idempotent defaults are not.
- **Plugin architectures, registries, or abstract base classes before
  three concrete implementations exist.**
  Premature abstraction is worse than duplication. Write the third
  implementation, then abstract.
- **Parallelism, concurrency, or async code without a measured
  performance problem.**
  Sequential execution is the default. If you propose `concurrent.futures`,
  `asyncio`, or threading, you must first demonstrate a measured
  bottleneck. "It might be slow" is not a measurement.
- **Configuration systems beyond the single `config.yaml`.**
  No hierarchical configs, no environment-variable overrides, no CLI
  flag for every parameter. One file, clear schema.
- **Web UIs, dashboards, monitoring endpoints, telemetry.**
  CLI only. A dashboard is a Phase 3+ concern and is not in scope.
- **Scope expansion via refactoring.**
  If you notice something tangential that could be improved, mention it
  to the user as a separate item. Do not silently refactor while working
  on something else.

## Things to always do

- **Write the test before the implementation.** Tests define the contract.
- **Demonstrate cache idempotency explicitly.** Every test that touches
  the cache runs the operation twice and asserts the second run is a
  no-op (cache hit, no new writes).
- **Use the `logging` module with structured output.** Never `print()`
  in library code. `print()` is acceptable only in CLI entry points for
  user-facing output.
- **Canonicalise JSON for hashing** with
  `json.dumps(obj, sort_keys=True, separators=(',', ':'),
  ensure_ascii=False)`. Any deviation is a bug.
- **Include `schema_version` in every new table** and
  `format_version` in every new JSON output format.
- **Use transactions for any operation that writes to both the cache
  and the catalogue.** If one fails, neither commits.
- **Fail loudly.** No silent exception swallowing. If you need to
  tolerate a failure, log it at WARNING or ERROR and record it in
  the catalogue with a reason.
- **Store hashes in full, always.** SHA-256 hex is 64 characters.
  Truncate only in display code (logs, CLI output), and always with
  a visible ellipsis. Never in stored data.
- **Honour `SPEC.md` §14 strictness principles.** The system must be
  debuggable from first principles using only `cat`, `jq`, `duckdb`,
  and `grep`. Any design choice that conflicts with this is wrong.

## If you are uncertain

Ask. Do not guess. The user strongly prefers "I don't know how to
proceed because X and Y are in tension, here are the trade-offs" over
silent interpretation.

Specifically, ask before:

- Adding any dependency not already in `pyproject.toml`
- Creating any new top-level directory
- Introducing any new file format
- Changing any function signature in the cache or catalogue layer
- Any change that would invalidate existing cache entries

## Scope discipline for sessions

Each session should have one focused goal. If the user asks for
"Phase 1", decompose it into specific, testable units and confirm
the decomposition before starting. Do not attempt to implement
multiple unrelated things in one session.

If you find yourself writing code in three different modules for
one feature, stop and ask whether the feature is correctly scoped.

## Review checklist (apply before every commit)

- [ ] Does this match `SPEC.md`? If not, was `SPEC.md` updated first
      with justification?
- [ ] Does every new code path have a test?
- [ ] Do cache-touching tests demonstrate idempotency?
- [ ] Are there any hardcoded paths, non-canonicalised hashes, or
      ad-hoc shortcuts that should go through the standard utilities?
- [ ] Were any "convenience" features added that weren't requested?
- [ ] Is the commit scoped to one concern?

## Language and style

- British English in prose (documentation, comments, log messages).
- Type hints on every function signature in library code.
- Docstrings on public functions explaining invariants, not just
  parameters.
- No cleverness for cleverness's sake. Boring code ages well.

## When the user says "just make it work"

They don't mean it. They mean "please make the current failure go
away in a principled way." Find the root cause, fix it properly,
explain what you changed. Do not add a workaround without calling
it out explicitly as a workaround with a TODO.
