"""
CleanSweep config.py — v2.7.0

Config format:
  {
    "version":           "2.7",   ← required, string, must be "2.x"
    "scan": {
      "recursive":      true,     ← required, bool
      "symlink_policy": "ignore", ← required, enum: ignore|follow|error
      "max_depth":      null,     ← optional, int >= 0 or null
      "exclude":        []        ← optional, list of fnmatch glob strings
    },
    "ignore_extensions": [".log"],
    "ignore_folders":    ["node_modules"],
    "default_scan_path": null,
    "default_dry_run":   false,
    "min_file_size":     0,
    "hash_chunk_size":   1048576,
    "log_level":         "INFO",
    "workers":           null
  }

Version policy (v2.7+):
  - 'version' field is required in every config file.
  - Supported: any "major.minor" string where major == 2 (e.g. "2.0", "2.7").
  - Version missing  → ConfigError with explicit upgrade instructions.
  - Version mismatch → ConfigError naming the invalid version and supported range.
  - upgrade_config() injects a version field for pre-2.7 (version-less) configs.

INVARIANT: _SCHEMA.keys() exposes every recognised key (top-level + scan
sub-keys) so callers can enumerate the full key space.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final


# ---------------------------------------------------------------------------
# Public version constants
# ---------------------------------------------------------------------------

#: Schema version written into newly generated / default config files.
CONFIG_VERSION: Final[str] = "2.7"

#: Only config files whose major version equals this integer are accepted.
_SUPPORTED_MAJOR: Final[int] = 2


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ConfigError(Exception):
    """
    Raised when a config file fails schema or version validation.

    Always carries a human-readable message naming the exact violation.
    Never swallowed — callers must handle and map to exit code 2.
    """


# ---------------------------------------------------------------------------
# Runtime constants
# ---------------------------------------------------------------------------

_CPU_COUNT = os.cpu_count() or 1
MAX_WORKERS_HARD_CAP = _CPU_COUNT * 4

_VALID_SYMLINK_POLICIES: Final[frozenset[str]] = frozenset({"ignore", "follow", "error"})

_VALID_LOG_LEVELS: Final[frozenset[str]] = frozenset(
    {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
)

#: Valid policy modes — matches policy.VALID_POLICY_MODES (duplicated here to
#: avoid importing policy.py from config.py and creating a dependency cycle).
_VALID_POLICY_MODES: Final[frozenset[str]] = frozenset({"strict", "safe", "warn"})


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

# Top-level JSON keys (excluding the scan block).
_TOP_SCHEMA: dict[str, type | tuple] = {
    "version":           str,
    "scan":              dict,
    "ignore_extensions": list,
    "ignore_folders":    list,
    "default_scan_path": (str, type(None)),
    "default_dry_run":   bool,
    "min_file_size":     int,
    "hash_chunk_size":   int,
    "log_level":         str,
    "workers":           (int, type(None)),
    "policy_mode":       str,
}

# Keys within the "scan" sub-object.
_SCAN_SCHEMA: dict[str, type | tuple] = {
    "recursive":      bool,
    "symlink_policy": str,
    "max_depth":      (int, type(None)),
    "exclude":        list,
}

# Combined — exposes every recognised key so _SCHEMA.keys() is complete.
# Callers and audit tools rely on this for key enumeration.
_SCHEMA: dict[str, type | tuple] = {**_TOP_SCHEMA, **_SCAN_SCHEMA}

_TOP_KNOWN:  Final[frozenset[str]] = frozenset(_TOP_SCHEMA.keys())
_SCAN_KNOWN: Final[frozenset[str]] = frozenset(_SCAN_SCHEMA.keys())

# Both must be present in every valid config file.
_REQUIRED_TOP_KEYS: Final[frozenset[str]] = frozenset({"version", "scan"})


# ---------------------------------------------------------------------------
# Default config data
# ---------------------------------------------------------------------------

#: Canonical default config dict. parse_config(DEFAULT_CONFIG_DATA) must
#: always succeed.  Module-level constant — never mutated.
DEFAULT_CONFIG_DATA: Final[dict] = {
    "version":           CONFIG_VERSION,
    "scan": {
        "recursive":      True,
        "symlink_policy": "ignore",
        "max_depth":      None,
        "exclude":        [],
    },
    "ignore_extensions": [],
    "ignore_folders":    [],
    "default_scan_path": None,
    "default_dry_run":   True,
    "min_file_size":     0,
    "hash_chunk_size":   1048576,
    "log_level":         "INFO",
    "workers":           None,
    "policy_mode":       "safe",
}


# ---------------------------------------------------------------------------
# AppConfig — immutable, validated application configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AppConfig:
    # ── Version (stored for introspection; validated before construction) ─
    config_version:    str             = CONFIG_VERSION
    # ── Scan block (required in config JSON) ─────────────────────────────
    recursive:         bool            = True
    symlink_policy:    str             = "ignore"
    # ── Scan block optional ───────────────────────────────────────────────
    max_depth:         int | None      = None
    exclude:           tuple[str, ...] = field(default_factory=tuple)
    # ── Top-level fields ──────────────────────────────────────────────────
    ignore_extensions: tuple[str, ...] = field(default_factory=tuple)
    ignore_folders:    tuple[str, ...] = field(default_factory=tuple)
    default_scan_path: Path | None     = None
    default_dry_run:   bool            = True
    min_file_size:     int             = 0
    hash_chunk_size:   int             = 1048576  # 1 MB
    log_level:         str             = "INFO"
    workers:           int | None      = None
    policy_mode:       str             = "safe"   # "strict" | "safe" | "warn"

    @property
    def follow_symlinks(self) -> bool:
        """Backward-compat: derived from symlink_policy."""
        return self.symlink_policy == "follow"

    @property
    def exclude_patterns(self) -> tuple[str, ...]:
        """Backward-compat alias for exclude."""
        return self.exclude


# ---------------------------------------------------------------------------
# Version validation
# ---------------------------------------------------------------------------

def _check_version(version: object) -> None:
    """
    Validate the 'version' field value.

    Accepts any "major.minor" string where major == 2 (e.g. "2.0", "2.7").
    Raises ConfigError with an actionable message on any violation.
    """
    if not isinstance(version, str):
        raise ConfigError(
            f"'version' must be a string (e.g. \"{CONFIG_VERSION}\"), "
            f"got {type(version).__name__}."
        )

    parts = version.split(".")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        raise ConfigError(
            f"Invalid config version format: {version!r}.\n"
            f"Expected 'major.minor' notation (e.g. \"{CONFIG_VERSION}\")."
        )

    major = int(parts[0])
    if major != _SUPPORTED_MAJOR:
        raise ConfigError(
            f"Invalid config version: {version!r}\n"
            f"Supported version: {_SUPPORTED_MAJOR}.x\n"
            f"Please update your config file — set the version field to:\n"
            f'  "version": "{CONFIG_VERSION}"'
        )


# ---------------------------------------------------------------------------
# Scan block validation
# ---------------------------------------------------------------------------

def _validate_scan(scan: object) -> None:
    """Validate the required 'scan' sub-object. Raises ConfigError on any violation."""
    if not isinstance(scan, dict):
        raise ConfigError(
            f"'scan' must be a JSON object, got {type(scan).__name__}."
        )

    unknown = set(scan.keys()) - _SCAN_KNOWN
    if unknown:
        raise ConfigError(
            f"Unknown 'scan' keys: {', '.join(sorted(unknown))}."
        )

    # Required fields
    for required_key in ("recursive", "symlink_policy"):
        if required_key not in scan:
            raise ConfigError(
                f"'scan.{required_key}' is required and must be present."
            )

    # Type checks for all present keys
    for key, expected in _SCAN_SCHEMA.items():
        if key not in scan:
            continue
        if not isinstance(scan[key], expected):
            raise ConfigError(
                f"'scan.{key}': expected {expected}, "
                f"got {type(scan[key]).__name__}."
            )

    # 'recursive' must be strictly boolean (not just truthy)
    if not isinstance(scan["recursive"], bool):
        raise ConfigError(
            f"'scan.recursive' must be a boolean (true or false), "
            f"got {type(scan['recursive']).__name__}."
        )

    # 'symlink_policy' enum check
    if scan["symlink_policy"] not in _VALID_SYMLINK_POLICIES:
        raise ConfigError(
            f"'scan.symlink_policy' must be one of "
            f"{sorted(_VALID_SYMLINK_POLICIES)}, "
            f"got {scan['symlink_policy']!r}."
        )

    # 'max_depth' range check
    if scan.get("max_depth") is not None:
        if scan["max_depth"] < 0:
            raise ConfigError(
                f"'scan.max_depth' must be >= 0 or null, "
                f"got {scan['max_depth']}."
            )

    # 'exclude' element types
    if "exclude" in scan:
        bad = [v for v in scan["exclude"] if not isinstance(v, str)]
        if bad:
            raise ConfigError(
                f"All values in 'scan.exclude' must be strings, got: {bad}."
            )


# ---------------------------------------------------------------------------
# Top-level validation (internal)
# ---------------------------------------------------------------------------

def _validate(data: dict) -> None:
    """
    Validate the full config dict.

    Validation order (fail-fast):
      1. Must be a dict
      2. No unknown top-level keys
      3. Required keys present ('version', 'scan')
      4. Version format and major-version range check
      5. Type checks for all present top-level keys
      6. 'log_level' enum check
      7. List element type checks
      8. Numeric range / constraint checks
      9. 'scan' sub-object validation

    Raises ConfigError on the first violation found.
    No I/O. No side effects. Deterministic.
    """
    if not isinstance(data, dict):
        raise ConfigError(
            f"Config must be a JSON object, got {type(data).__name__}."
        )

    unknown = set(data.keys()) - _TOP_KNOWN
    if unknown:
        raise ConfigError(
            f"Unknown config keys: {', '.join(sorted(unknown))}.\n"
            f"Allowed keys: {', '.join(sorted(_TOP_KNOWN))}."
        )

    # Required keys — checked before type validation so the message is specific.
    for key in sorted(_REQUIRED_TOP_KEYS):
        if key not in data:
            if key == "version":
                raise ConfigError(
                    f"Config is missing required 'version' field.\n"
                    f"Add the following line to your config file:\n"
                    f'  "version": "{CONFIG_VERSION}"'
                )
            raise ConfigError(
                f"Config is missing required '{key}' field."
            )

    # Version must be validated before anything else — fail-fast on mismatch.
    _check_version(data["version"])

    # Type checks for all present top-level keys (skip 'scan' — validated last).
    for key, expected in _TOP_SCHEMA.items():
        if key not in data or key == "scan":
            continue
        if not isinstance(data[key], expected):
            raise ConfigError(
                f"Config validation error:\n"
                f"  '{key}' must be {expected}, "
                f"got {type(data[key]).__name__}."
            )

    # 'log_level' enum check
    if "log_level" in data and data["log_level"] not in _VALID_LOG_LEVELS:
        raise ConfigError(
            f"'log_level' must be one of {sorted(_VALID_LOG_LEVELS)}, "
            f"got {data['log_level']!r}."
        )

    # 'policy_mode' enum check
    if "policy_mode" in data and data["policy_mode"] not in _VALID_POLICY_MODES:
        raise ConfigError(
            f"'policy_mode' must be one of {sorted(_VALID_POLICY_MODES)}, "
            f"got {data['policy_mode']!r}."
        )

    # List element type checks
    for key in ("ignore_extensions", "ignore_folders"):
        if key in data:
            bad = [v for v in data[key] if not isinstance(v, str)]
            if bad:
                raise ConfigError(
                    f"All values in '{key}' must be strings, got: {bad}."
                )

    # Numeric range checks
    for key in ("min_file_size", "hash_chunk_size"):
        if key in data and data[key] < 0:
            raise ConfigError(f"'{key}' must be >= 0, got {data[key]}.")

    if "hash_chunk_size" in data and data["hash_chunk_size"] == 0:
        raise ConfigError("'hash_chunk_size' must be > 0.")

    if "workers" in data and data["workers"] is not None:
        w = data["workers"]
        if w < 1:
            raise ConfigError(f"'workers' must be >= 1, got {w}.")
        if w > MAX_WORKERS_HARD_CAP:
            raise ConfigError(
                f"'workers' must be <= cpu_count × 4 "
                f"({MAX_WORKERS_HARD_CAP}), got {w}."
            )

    # Scan sub-object validated last — only reached once top-level is clean.
    _validate_scan(data["scan"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_config(data: dict) -> None:
    """
    Validate a raw config dict against the full schema.

    Raises ConfigError with a human-readable message on any violation.
    No I/O. No side effects. Deterministic.

    Use this for pre-flight checks or testing without constructing AppConfig.
    """
    _validate(data)


def parse_config(data: dict) -> AppConfig:
    """
    Validate raw dict and return an immutable AppConfig.

    No I/O. No CLI access. No side effects. Deterministic.
    Raises ConfigError on any schema or version violation.

    Pipeline:
      _validate(data) → AppConfig(...)
    """
    _validate(data)

    scan = data["scan"]

    return AppConfig(
        config_version    = data["version"],
        # Scan block — required fields
        recursive         = scan["recursive"],
        symlink_policy    = scan["symlink_policy"],
        # Scan block — optional fields
        max_depth         = scan.get("max_depth", None),
        exclude           = tuple(scan.get("exclude", [])),
        # Top-level fields
        ignore_extensions = tuple(data.get("ignore_extensions", [])),
        ignore_folders    = tuple(data.get("ignore_folders", [])),
        default_scan_path = (
            Path(data["default_scan_path"])
            if data.get("default_scan_path") else None
        ),
        default_dry_run   = data.get("default_dry_run", False),
        min_file_size     = data.get("min_file_size", 0),
        hash_chunk_size   = data.get("hash_chunk_size", 1048576),
        log_level         = data.get("log_level", "INFO"),
        workers           = data.get("workers", None),
        policy_mode       = data.get("policy_mode", "safe"),
    )


def upgrade_config(data: dict) -> dict:
    """
    Return a config dict upgraded to the current version schema.

    Handles one upgrade path:
      Pre-2.7 configs (no 'version' field): inject CONFIG_VERSION.

    Does NOT mutate the input dict — always returns a new dict.
    Raises ConfigError if the input is not a JSON object.

    For unsupported major versions (e.g. "1.x") the caller must update
    the config manually — auto-upgrade across major versions is not supported.
    """
    if not isinstance(data, dict):
        raise ConfigError("Config must be a JSON object.")

    if "version" not in data:
        # Pre-2.7 config: no version field. Inject current version at the front.
        return {"version": CONFIG_VERSION, **data}

    # Version present — return a shallow copy; validate_config() catches mismatches.
    return dict(data)


def load_config(path: Path | None) -> AppConfig:
    """
    Load, validate, and return an immutable AppConfig.

    Resolution order:
      1. path             — explicit path from --config CLI flag
      2. ./config.json    — implicit auto-load if present in working directory
      3. DEFAULT_CONFIG_DATA — in-memory defaults (no file required)

    Raises ConfigError on:
      - OS / permission errors reading the file
      - JSON syntax errors
      - Schema or version violations in the loaded data

    The error message always includes the file path for actionable diagnostics.
    No sys.exit(). No print(). Callers map ConfigError to CLI exit codes.

    Pipeline:
      load_config(path)
        → read file (or use defaults)
        → parse_config(raw)
          → validate_config(raw)   [_validate]
          → AppConfig(...)
    """
    resolved: Path | None = path

    if resolved is None:
        implicit = Path("config.json")
        resolved = implicit if implicit.exists() else None

    if resolved is None:
        # No config file found — validated defaults, no I/O required.
        return parse_config(DEFAULT_CONFIG_DATA)

    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as e:
        raise ConfigError(
            f"Cannot read config file '{resolved}': {type(e).__name__}: {e}"
        ) from e

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        raise ConfigError(
            f"Config file '{resolved}': invalid JSON — {e}"
        ) from e

    try:
        return parse_config(raw)
    except ConfigError as e:
        # Prepend file path so every CLI error message names the offending file.
        raise ConfigError(f"Config file '{resolved}': {e}") from e
