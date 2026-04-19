"""Shared pytest fixtures for the pkm test suite.

One fixture so far: ``tmp_root`` — a per-test directory shaped like
the knowledge root specified in SPEC §3. All subsequent tests assume
this shape and should not re-implement it.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    """A fresh, SPEC §3-shaped knowledge root for a single test.

    Guarantees on the returned directory:

      - ``<root>/cache/`` exists and is empty.
      - ``<root>/logs/`` exists and is empty.
      - ``<root>/sources/`` exists and is empty.
      - ``<root>/catalogue.duckdb`` does NOT exist (migrations create
        it; tests exercising migrations rely on its absence).
      - ``<root>/config.yaml`` does NOT exist; tests that need one
        write it themselves so each test's config is explicit.

    Teardown is pytest's built-in ``tmp_path`` cleanup.
    """
    (tmp_path / "cache").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "sources").mkdir()
    return tmp_path
