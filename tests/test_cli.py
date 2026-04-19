"""Tests for ``pkm.cli`` — the Phase 1 CLI surface.

Tests call ``main(argv)`` directly rather than spawning a
subprocess. argparse still raises ``SystemExit`` on ``--help``,
``--version``, and argument errors; ``pytest.raises(SystemExit)``
catches those. Other return paths yield an int exit code.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pkm.cli import main


def _make_config(root: Path, cfg_path: Path) -> Path:
    cfg_path.write_text(f"root_dir: {root}\n", encoding="utf-8")
    return cfg_path


# --- Help, version, and no-subcommand -----------------------------------


def test_top_level_help_lists_every_subcommand(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    for name in ("migrate", "rebuild-catalogue", "ingest", "extract"):
        assert name in out


def test_top_level_help_has_distinct_descriptions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Each subcommand's one-line description reads as something
    specific, not ``Run the foo command``. This is a readability
    regression test."""
    with pytest.raises(SystemExit):
        main(["--help"])
    out = capsys.readouterr().out
    assert "Apply pending schema migrations" in out
    assert "Rebuild the artifacts table" in out
    assert "Register sources" in out
    assert "Run extractors" in out


def test_migrate_help_describes_idempotency(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["migrate", "--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "Idempotent" in out or "idempotent" in out


def test_rebuild_catalogue_help_names_the_scope(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["rebuild-catalogue", "--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "artifacts" in out
    assert "sources" in out.lower()  # mentions what's NOT rebuilt


def test_version_prints_pkm_and_version_string(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--version"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert out.startswith("pkm ")


def test_no_subcommand_prints_help_and_exits_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "usage" in err.lower()


def test_unknown_subcommand_exits_two(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["bogus-command"])
    assert excinfo.value.code == 2


# --- migrate -------------------------------------------------------------


def test_migrate_on_fresh_root_applies_0001(
    tmp_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_config(tmp_root, tmp_path / "config.yaml")
    rc = main(["--config", str(cfg), "migrate"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "applied" in out
    assert "[1]" in out
    assert (tmp_root / "catalogue.duckdb").exists()


def test_second_migrate_is_a_no_op(
    tmp_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_config(tmp_root, tmp_path / "config.yaml")
    main(["--config", str(cfg), "migrate"])
    capsys.readouterr()
    rc = main(["--config", str(cfg), "migrate"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no pending" in out.lower()


def test_migrate_dry_run_reports_and_writes_nothing(
    tmp_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_config(tmp_root, tmp_path / "config.yaml")
    rc = main(["--config", str(cfg), "migrate", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dry-run" in out.lower()
    assert "would apply" in out.lower()
    # The catalogue file is created by open_catalogue even in dry-run,
    # but no migration was applied so there should be no schema_meta
    # row. Verify by re-running without dry-run and checking the
    # applied version list.
    rc2 = main(["--config", str(cfg), "migrate"])
    assert rc2 == 0
    out2 = capsys.readouterr().out
    assert "[1]" in out2  # would have been "no pending" if dry-run had written


# --- rebuild-catalogue ---------------------------------------------------


def test_rebuild_catalogue_on_empty_cache(
    tmp_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_config(tmp_root, tmp_path / "config.yaml")
    main(["--config", str(cfg), "migrate"])
    capsys.readouterr()
    rc = main(["--config", str(cfg), "rebuild-catalogue"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "rebuilt artifacts" in out.lower()
    assert "inserted 0" in out


def test_rebuild_catalogue_dry_run(
    tmp_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_config(tmp_root, tmp_path / "config.yaml")
    main(["--config", str(cfg), "migrate"])
    capsys.readouterr()
    rc = main(["--config", str(cfg), "rebuild-catalogue", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dry-run" in out.lower()


# --- Placeholders --------------------------------------------------------


def test_ingest_placeholder_exits_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["ingest"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "not wired up" in err.lower() or "not implemented" in err.lower()
    assert "Step 7" in err


def test_extract_placeholder_exits_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["extract"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "not wired up" in err.lower() or "not implemented" in err.lower()
    assert "Step 7" in err


# --- Config error surfaces clearly --------------------------------------


def test_missing_config_exits_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["--config", str(tmp_path / "no_such.yaml"), "migrate"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "config error" in err.lower()
