"""Command-line interface for pkm.

Phase 1 surface:

    pkm                          # prints help to stderr, exits 1
    pkm --help                   # argparse help, exits 0
    pkm --version                # "pkm <package-version>", exits 0

    pkm migrate [--dry-run]
    pkm rebuild-catalogue [--dry-run]
    pkm ingest                   # placeholder (Step 7), exits non-zero
    pkm extract                  # placeholder (Step 7), exits non-zero

Design notes:

  - ``argparse`` from the stdlib — no click, no typer, no
    argcomplete, no generated shell completions.
  - Subcommand dispatch is an explicit ``{name: handler}`` dict.
    No ``set_defaults(func=...)``. Grepping for the literal string
    ``"migrate"`` finds the handler; grepping for ``_cmd_migrate``
    finds its dispatch entry.
  - Exit codes:
        0  — the command succeeded (including dry-run reports).
        1  — the command was reached but failed (not implemented,
             or an unhandled exception that's not a config error).
        2  — configuration error (missing config, malformed config,
             argparse argument errors).
  - Logging is configured via ``basicConfig`` here. Step 7 will
    replace this with JSONL file output in ``logging_setup.py``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from importlib.metadata import version as _package_version
from pathlib import Path

from pkm.catalogue import run_migrations
from pkm.config import Config, ConfigError, load_config
from pkm.ingest import ingest_sources
from pkm.rebuild import rebuild_artifacts

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH: Path = Path("~/knowledge/config.yaml")

_NOT_IMPLEMENTED_MESSAGE = (
    "`pkm {name}` is declared in the CLI surface but is not wired up "
    "in Phase 1. It is scheduled for Step 7 (ingestion and extraction "
    "pipeline). Until then it exits non-zero rather than silently "
    "succeeding."
)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns the process exit code; does not call
    ``sys.exit``. The installed ``pkm`` console script and
    ``python -m pkm`` both wrap the return value.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args)

    if args.subcommand is None:
        parser.print_help(sys.stderr)
        return 1

    handler = _SUBCOMMANDS[args.subcommand]
    try:
        return handler(args)
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        logger.exception("unhandled error in pkm %s", args.subcommand)
        print(f"error: {e}", file=sys.stderr)
        return 1


# --- Argument parser -----------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pkm",
        description=(
            "Personal knowledge management — content-addressed "
            "extraction cache and catalogue."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"pkm {_package_version('pkm')}",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        metavar="PATH",
        help=(
            f"path to config.yaml (default: {DEFAULT_CONFIG_PATH}); "
            "no environment-variable override per SPEC §14.6"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="log level for this invocation (default: INFO)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="shortcut for --log-level DEBUG",
    )

    subparsers = parser.add_subparsers(
        dest="subcommand",
        metavar="<subcommand>",
    )

    p_migrate = subparsers.add_parser(
        "migrate",
        help="Apply pending schema migrations.",
        description=(
            "Apply any pending catalogue migrations in numeric "
            "order. Idempotent: running twice produces zero new "
            "schema_meta rows on the second run. The on-disk hash "
            "of every previously applied migration is verified "
            "before anything new is applied (SPEC §14.8)."
        ),
    )
    p_migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="list pending migrations without applying any",
    )

    p_rebuild = subparsers.add_parser(
        "rebuild-catalogue",
        help="Rebuild the artifacts table from cache meta.json files.",
        description=(
            "Walk <root>/cache/ and rewrite the artifacts table "
            "from the meta.json files (SPEC §5.3). Does not touch "
            "sources or source_paths — re-run `pkm ingest` "
            "afterwards to repopulate those."
        ),
    )
    p_rebuild.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be inserted without writing",
    )

    subparsers.add_parser(
        "ingest",
        help="Register sources from sources.yaml into the catalogue.",
        description=(
            "Read <root>/sources/sources.yaml, hash each referenced "
            "file, and populate the sources and source_paths tables "
            "(SPEC §8). Idempotent: a second run produces zero new "
            "rows. Unreadable entries (missing file, broken symlink, "
            "directory without recursive: true) are WARNING-logged "
            "and skipped (SPEC §13.4); ingest never halts on a bad "
            "manifest entry."
        ),
    )

    subparsers.add_parser(
        "extract",
        help="Run extractors over registered sources (not implemented).",
        description=(
            "Not yet implemented — scheduled for Phase 1 Step 7. "
            "When wired, applies the routing policy (§7.3) and runs "
            "producers over sources, caching outcomes."
        ),
    )

    return parser


def _configure_logging(args: argparse.Namespace) -> None:
    level = (
        logging.DEBUG
        if args.verbose
        else getattr(logging, args.log_level)
    )
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


def _load_config(args: argparse.Namespace) -> Config:
    return load_config(args.config.expanduser())


# --- Subcommand handlers -------------------------------------------------


def _cmd_migrate(args: argparse.Namespace) -> int:
    config = _load_config(args)
    versions = run_migrations(config.root_dir, dry_run=args.dry_run)
    if args.dry_run:
        if versions:
            print(
                f"dry-run: would apply {len(versions)} migration(s): "
                f"{versions}"
            )
        else:
            print("dry-run: no pending migrations")
    else:
        if versions:
            print(f"applied {len(versions)} migration(s): {versions}")
        else:
            print("no pending migrations")
    return 0


def _cmd_rebuild_catalogue(args: argparse.Namespace) -> int:
    config = _load_config(args)
    result = rebuild_artifacts(config.root_dir, dry_run=args.dry_run)
    if args.dry_run:
        print(
            f"dry-run: scanned {result.scanned}, "
            f"would insert {result.scanned - len(result.skipped)}, "
            f"would skip {len(result.skipped)}"
        )
    else:
        print(
            f"rebuilt artifacts: scanned {result.scanned}, "
            f"inserted {result.inserted}, "
            f"skipped {len(result.skipped)}, "
            f"swept {len(result.swept)}"
        )
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    config = _load_config(args)
    result = ingest_sources(config.root_dir)
    print(
        f"ingested: scanned {result.scanned}, "
        f"{result.new_sources} new sources, "
        f"{result.new_paths} new paths, "
        f"{len(result.skipped)} skipped"
    )
    return 0


def _cmd_extract(args: argparse.Namespace) -> int:
    print(_NOT_IMPLEMENTED_MESSAGE.format(name="extract"), file=sys.stderr)
    return 1


# Explicit subcommand table. Grep-friendly: searching for "migrate"
# finds both this entry and the function that handles it.
_SUBCOMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "migrate": _cmd_migrate,
    "rebuild-catalogue": _cmd_rebuild_catalogue,
    "ingest": _cmd_ingest,
    "extract": _cmd_extract,
}
