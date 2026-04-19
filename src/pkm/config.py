"""Configuration loading from ``config.yaml`` (SPEC §9, §14.6).

This module exposes exactly two things:

  - ``Config``: a frozen dataclass of the settings in use for one
    CLI invocation.
  - ``load_config(path)``: reads the YAML file, validates the
    shape, and returns a ``Config``.

Only ``root_dir`` is parsed at this step — the CLI needs it to
locate the catalogue and the cache. Later steps add producer
versions and their per-producer config dicts (required by SPEC
§14.5 exact-version matching) and the log-level key. Keeping
``Config`` narrow means the CLI skeleton depends on one field and
not on fields that do not yet exist.

No environment-variable overrides are supported. SPEC §14.6
prohibits hidden state; the single YAML file is the sole source
of configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


class ConfigError(Exception):
    """Raised when ``config.yaml`` is missing, unreadable, or the
    contents do not satisfy the minimum expected shape.
    """


@dataclass(frozen=True)
class Config:
    """The subset of settings the Phase 1 CLI needs to function.

    More fields will be added as Phase 1 progresses (producer
    versions + configs at Step 7, log-level override, etc.).
    """

    root_dir: Path
    """Absolute, user-expanded knowledge root (SPEC §3)."""

    source: Path
    """The config file this was loaded from. Useful for error
    messages and for log events; not part of the data itself.
    """


def load_config(path: Path) -> Config:
    """Load and validate ``config.yaml`` at ``path``.

    Args:
        path: Path to the config file. Not expanded — callers
            should expand ``~`` themselves if needed.

    Returns:
        A ``Config`` with ``root_dir`` resolved to an absolute
        path (``~`` expansion + ``.resolve()``).

    Raises:
        ConfigError: file does not exist, is not valid YAML, is
            not a mapping at the top level, or does not contain
            a string ``root_dir`` field.
    """
    if not path.exists():
        raise ConfigError(
            f"config file not found at {path}. create it with at "
            f"minimum `root_dir: <path>` or pass --config to override."
        )

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"config at {path} is not valid YAML: {e}") from e

    if raw is None:
        raise ConfigError(f"config at {path} is empty")
    if not isinstance(raw, dict):
        raise ConfigError(
            f"config at {path} must be a YAML mapping, got "
            f"{type(raw).__name__}"
        )

    root_dir_raw = raw.get("root_dir")
    if not isinstance(root_dir_raw, str):
        raise ConfigError(
            f"config at {path} must contain a string `root_dir` field; "
            f"got {type(root_dir_raw).__name__}"
        )

    root_dir = Path(root_dir_raw).expanduser().resolve()

    return Config(root_dir=root_dir, source=path)
