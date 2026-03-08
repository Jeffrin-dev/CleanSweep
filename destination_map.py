"""
CleanSweep destination_map.py — v2.3.0 Destination Mapping Layer.

This module is the sole authority for:
  - Parsing and validating destination map JSON (schema "2.3")
  - Resolving logical destination keys to absolute filesystem paths
  - Template variable expansion (pure, deterministic, no I/O)
  - Early validation that all ruleset destinations exist in the map

Architecture contract (permanent, locked in INVARIANTS.md §20):

  resolve_destination_path(dest_key, filename, file_size, mtime_ns, dest_map) → Path
    Pure function. Deterministic. No filesystem access. No side effects.

  parse_destination_map(data, default_base_dir) → DestinationMap
    Validates raw dict. Returns immutable DestinationMap.
    Raises DestinationMapError on any violation.
    No I/O. No CLI access. No fallback on invalid input.

  validate_ruleset_destinations(ruleset_destinations, dest_map) → None
    Pure early-validation function. Raises DestinationMapError if any
    destination key used by a ruleset is absent from the map.

Three-layer flow (locked permanently):

  Rule engine  →  dest_key (logical, no path)
  Mapping layer → absolute Path (physical, no I/O)
  Organizer    →  mkdir + atomic move

What MUST NOT cross layer boundaries:
  ✗  Rule definitions must never contain filesystem paths
  ✗  Mapping layer must never create directories or move files
  ✗  Organizer must never evaluate rule criteria or expand templates
  ✗  Template logic must never call stat(), open(), or any OS call
  ✗  Folder creation must never happen inside this module

─────────────────────────────────────────────────────────────────────────────
JSON schema (v2.3):
  {
    "version": "2.3",
    "destinations": {
      "Images":    "media/images",
      "Videos":    "media/videos/{year}/{month}",
      "LargeFiles":"archive/large/{extension}",
      "Others":    "misc"
    },
    "base_dir":         "/organized",         ← optional; default = scan folder
    "conflict_policy":  "rename",             ← optional; rename|skip|error
    "template_fallback": "misc"               ← optional; default = "misc"
  }

Template variables (all pure, all deterministic per-file):
  {extension}   — file extension without leading dot, lowercased
                  e.g. "mp4", "txt"; no extension → template_fallback
  {year}        — 4-digit year from file mtime_ns (UTC)
                  None mtime_ns → template_fallback
  {month}       — zero-padded month 01–12 from file mtime_ns (UTC)
                  None mtime_ns → template_fallback
  {size_bucket} — "small" (<1 MB), "medium" (1 MB–100 MB), "large" (≥100 MB)
                  file_size=0 → "small" (conservative default)

Conflict policies:
  rename (default) — append deterministic counter suffix: file (1).txt, file (2).txt
                     handled by FileOperationManager.atomic_move() in organizer layer
  skip             — leave file in place if any target filename collision exists
  error            — abort the entire organize operation if any collision exists

Size bucket thresholds (locked permanently):
  small:  file_size < 1_048_576        (< 1 MB)
  medium: 1_048_576 ≤ file_size < 104_857_600  (1 MB – 100 MB)
  large:  file_size ≥ 104_857_600      (≥ 100 MB)
"""

from __future__ import annotations

import datetime
import re
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION: Final[str] = "2.3"
_ACCEPTED_VERSIONS: Final[frozenset[str]] = frozenset({"2.3"})

_ALLOWED_CONFLICT_POLICIES: Final[frozenset[str]] = frozenset({"rename", "skip", "error"})
_DEFAULT_CONFLICT_POLICY:   Final[str] = "rename"
_DEFAULT_TEMPLATE_FALLBACK: Final[str] = "misc"

# Size bucket thresholds in bytes (locked permanently — any change requires a version bump)
_SIZE_SMALL_CEILING:  Final[int] = 1_048_576      # < 1 MB  → "small"
_SIZE_MEDIUM_CEILING: Final[int] = 104_857_600    # < 100 MB → "medium"
# file_size ≥ 104_857_600 → "large"

# Allowed template variable names (unknown vars are hard errors at parse time)
_TEMPLATE_VARS: Final[frozenset[str]] = frozenset({
    "extension", "year", "month", "size_bucket",
})

# Top-level JSON keys
_REQUIRED_TOP_KEYS: Final[frozenset[str]] = frozenset({"version", "destinations"})
_ALLOWED_TOP_KEYS:  Final[frozenset[str]] = frozenset({
    "version", "destinations", "base_dir", "conflict_policy", "template_fallback",
})

# Regex to find template variables in a template string
_TEMPLATE_VAR_RE: Final[re.Pattern[str]] = re.compile(r"\{(\w+)\}")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DestinationMapError(Exception):
    """
    Raised when destination map JSON fails schema validation, or when a
    logical destination key is absent from the mapping at resolution time.

    Always carries a human-readable message naming the exact violation.
    Never swallowed — callers must handle and map to exit code 2.
    """


# ---------------------------------------------------------------------------
# Immutable data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DestinationMap:
    """
    Immutable, validated destination mapping produced by parse_destination_map().

    Fields:
      destinations      — MappingProxyType mapping logical key → path template string.
                          True read-only: any attempted mutation raises TypeError.
                          Keys are simple names (no path separators).
                          Values are relative or absolute path templates.
                          Stored in sorted key order for determinism.
      base_dir          — base directory for resolving relative path templates.
                          Absolute; supplied by caller or defaulted to scan folder.
      conflict_policy   — "rename" | "skip" | "error"
      template_fallback — replacement string for unresolvable template variables
      schema_version    — "2.3"

    Immutability contract (locked permanently):
      destinations is a types.MappingProxyType — frozen at construction time.
      frozen=True on the dataclass blocks field reassignment.
      Together these make DestinationMap fully immutable after parse_destination_map() returns.
    """
    destinations:      types.MappingProxyType
    base_dir:          Path
    conflict_policy:   str = _DEFAULT_CONFLICT_POLICY
    template_fallback: str = _DEFAULT_TEMPLATE_FALLBACK
    schema_version:    str = SCHEMA_VERSION

    def __repr__(self) -> str:
        return (
            f"DestinationMap("
            f"destinations={len(self.destinations)}, "
            f"base_dir={self.base_dir!r}, "
            f"conflict_policy={self.conflict_policy!r}, "
            f"schema={self.schema_version!r})"
        )


# ---------------------------------------------------------------------------
# Template resolution — pure functions, zero I/O, zero side effects
# ---------------------------------------------------------------------------

def _resolve_size_bucket(file_size: int) -> str:
    """
    Return the size bucket label for a given file size in bytes.

    Thresholds (locked permanently — any change requires a version bump):
      small:  file_size < 1_048_576
      medium: 1_048_576 ≤ file_size < 104_857_600
      large:  file_size ≥ 104_857_600

    file_size = 0 → "small" (conservative default for unreadable files).
    """
    if file_size < _SIZE_SMALL_CEILING:
        return "small"
    if file_size < _SIZE_MEDIUM_CEILING:
        return "medium"
    return "large"


def _resolve_template(
    template:          str,
    filename:          str,
    file_size:         int,
    mtime_ns:          int | None,
    template_fallback: str,
) -> str:
    """
    Resolve all template variables in a path template string.

    Each {variable} is replaced with its computed value, or with
    template_fallback when the variable cannot be resolved.

    Supported variables:
      {extension}   — file extension without leading dot, lowercased.
                      "video.MP4" → "mp4".
                      "Makefile" (no extension) → template_fallback.
      {year}        — 4-digit year (UTC) from mtime_ns.
                      mtime_ns=None → template_fallback.
      {month}       — zero-padded month 01–12 (UTC) from mtime_ns.
                      mtime_ns=None → template_fallback.
      {size_bucket} — "small" | "medium" | "large" from file_size.
                      Always resolvable (file_size=0 → "small").

    Pure function. No I/O. No side effects. Deterministic.
    Unknown variables are guaranteed absent by parse_destination_map().
    """
    if "{" not in template:
        return template  # fast path: no variables to expand

    result = template

    if "{extension}" in result:
        suffix = Path(filename).suffix.lower().lstrip(".")
        result = result.replace("{extension}", suffix if suffix else template_fallback)

    if "{year}" in result or "{month}" in result:
        if mtime_ns is not None:
            dt = datetime.datetime.fromtimestamp(
                mtime_ns / 1_000_000_000,
                tz=datetime.timezone.utc,
            )
            year_val  = str(dt.year)
            month_val = f"{dt.month:02d}"
        else:
            year_val  = template_fallback
            month_val = template_fallback
        result = result.replace("{year}",  year_val)
        result = result.replace("{month}", month_val)

    if "{size_bucket}" in result:
        result = result.replace("{size_bucket}", _resolve_size_bucket(file_size))

    return result


# ---------------------------------------------------------------------------
# Path resolution — pure function, no I/O
# ---------------------------------------------------------------------------

def resolve_destination_path(
    dest_key:  str,
    filename:  str,
    file_size: int,
    mtime_ns:  int | None,
    dest_map:  DestinationMap,
) -> Path:
    """
    Resolve a logical destination key to an absolute filesystem path.

    Algorithm:
      1. Look up dest_key in dest_map.destinations.
         Missing key → DestinationMapError (caller must catch and report early).
      2. Expand template variables in the path template string.
      3. If expanded path is absolute, use it directly.
         If relative, resolve against dest_map.base_dir.
      4. Return the resulting absolute Path (directory, not file).

    Args:
      dest_key  — logical name produced by rules.resolve_destination()
      filename  — file basename (used for {extension})
      file_size — file size in bytes (used for {size_bucket})
      mtime_ns  — file modification time in nanoseconds, UTC (may be None)
      dest_map  — validated DestinationMap

    Returns the destination directory as an absolute Path.

    Raises DestinationMapError if dest_key is absent from the map.
    Pure function. No filesystem access. No side effects. Deterministic.
    """
    if dest_key not in dest_map.destinations:
        known = sorted(dest_map.destinations.keys())
        raise DestinationMapError(
            f"Destination key {dest_key!r} is not defined in the destination map. "
            f"Known destinations: {known}. "
            f"Add an entry for {dest_key!r} in the destination map, "
            f"or update the rule to produce a known destination key."
        )

    template    = dest_map.destinations[dest_key]
    resolved_str = _resolve_template(
        template, filename, file_size, mtime_ns, dest_map.template_fallback
    )

    resolved = Path(resolved_str)
    if resolved.is_absolute():
        return resolved

    # Relative template: resolve against base_dir and enforce boundary containment.
    # Path.resolve() normalises '..' components but does NOT enforce that the
    # result remains within base_dir.  A template like '../../../tmp/evil' would
    # escape to '/tmp/evil' without this check — path traversal vulnerability (D10).
    #
    # Enforcement rule (locked permanently):
    #   Relative path templates MUST resolve to a path inside base_dir.
    #   Absolute path templates are trusted as-is (explicit user intent).
    #   '..' components that escape base_dir are a hard error at resolution time.
    final = (dest_map.base_dir / resolved_str).resolve()
    base  = dest_map.base_dir.resolve()
    try:
        final.relative_to(base)
    except ValueError:
        raise DestinationMapError(
            f"Destination key {dest_key!r}: resolved path {str(final)!r} escapes "
            f"base_dir {str(base)!r}. "
            f"Template {template!r} contains path traversal components ('..') that "
            f"navigate outside the base directory. "
            f"Use an absolute path template or remove the '..' components."
        )
    return final


# ---------------------------------------------------------------------------
# Early ruleset validation — pure function, no I/O
# ---------------------------------------------------------------------------

def validate_ruleset_destinations(
    ruleset_destinations: set[str],
    dest_map:             DestinationMap,
) -> None:
    """
    Validate that every destination key a ruleset can produce exists in dest_map.

    Must be called before the first file is processed — "fail fast" contract.
    A missing key here means the organize run would hit DestinationMapError
    partway through, leaving files in inconsistent state. Calling this up front
    prevents that entirely.

    Args:
      ruleset_destinations — complete set of destination names the ruleset can produce.
                             Must include: {rule.destination for rule in ruleset.rules}
                             AND ruleset.default_destination.
      dest_map             — the DestinationMap to validate against.

    Raises DestinationMapError (listing all missing keys) if any key is absent.
    Pure function. No I/O. No side effects.
    """
    missing = sorted(ruleset_destinations - set(dest_map.destinations.keys()))
    if missing:
        known = sorted(dest_map.destinations.keys())
        raise DestinationMapError(
            f"The following destination keys are used by rules but not defined in "
            f"the destination map: {missing}. "
            f"Known destinations in map: {known}. "
            f"Add entries for the missing keys, or update the rules to use existing keys."
        )


# ---------------------------------------------------------------------------
# Schema validation helpers — all pure, all raise DestinationMapError
# ---------------------------------------------------------------------------

def _validate_dest_key(key: object, index: int) -> str:
    """Validate and return one destination map key string."""
    prefix = f"destinations key[{index}]"

    if not isinstance(key, str):
        raise DestinationMapError(
            f"{prefix}: key must be a string, got {type(key).__name__}."
        )
    if not key.strip():
        raise DestinationMapError(
            f"{prefix}: key must not be empty or whitespace-only."
        )
    if "/" in key or "\\" in key:
        raise DestinationMapError(
            f"{prefix}: key {key!r} contains a path separator (/ or \\). "
            f"Destination keys must be simple names, not paths."
        )
    if "\x00" in key:
        raise DestinationMapError(
            f"{prefix}: key {key!r} contains a null byte."
        )
    return key


def _validate_dest_template(value: object, key: str) -> str:
    """Validate and return one destination path template string."""
    prefix = f"destinations[{key!r}]"

    if not isinstance(value, str):
        raise DestinationMapError(
            f"{prefix}: path template must be a string, got {type(value).__name__}."
        )
    if not value.strip():
        raise DestinationMapError(
            f"{prefix}: path template must not be empty or whitespace-only."
        )
    if "\x00" in value:
        raise DestinationMapError(
            f"{prefix}: path template {value!r} contains a null byte."
        )

    # Validate that every {variable} in the template is a known variable name.
    # Unknown variables would silently produce garbage paths at runtime.
    for var_name in _TEMPLATE_VAR_RE.findall(value):
        if var_name not in _TEMPLATE_VARS:
            raise DestinationMapError(
                f"{prefix}: unknown template variable {{{var_name}}} in {value!r}. "
                f"Allowed template variables: "
                f"{sorted(f'{{{v}}}' for v in _TEMPLATE_VARS)}."
            )

    return value


# ---------------------------------------------------------------------------
# Public API — parse_destination_map
# ---------------------------------------------------------------------------

def parse_destination_map(
    data:             dict,
    default_base_dir: Path | None = None,
) -> DestinationMap:
    """
    Validate raw dict and return an immutable DestinationMap.

    Steps:
      1. Type-check top-level structure.
      2. Validate and detect schema version.
      3. Validate each destination key and path template.
         Keys stored in sorted order for determinism.
      4. Validate conflict_policy (optional, default "rename").
      5. Validate template_fallback (optional, default "misc").
      6. Resolve base_dir: from JSON "base_dir" key if present,
         else default_base_dir, else Path.cwd().

    Args:
      data             — raw dict parsed from JSON destination map file.
      default_base_dir — base directory to use when "base_dir" is absent from data.
                         Typically the scan folder (folder argument to organize()).
                         If None, falls back to Path.cwd().

    Returns immutable DestinationMap.
    Raises DestinationMapError on any schema violation.
    No I/O. No side effects. Deterministic.
    """
    if not isinstance(data, dict):
        raise DestinationMapError(
            f"Destination map must be a JSON object, got {type(data).__name__}."
        )

    unknown = set(data.keys()) - _ALLOWED_TOP_KEYS
    if unknown:
        raise DestinationMapError(
            f"Unknown destination map keys: {sorted(unknown)}. "
            f"Allowed keys: {sorted(_ALLOWED_TOP_KEYS)}."
        )

    missing = _REQUIRED_TOP_KEYS - set(data.keys())
    if missing:
        raise DestinationMapError(
            f"Missing required destination map keys: {sorted(missing)}."
        )

    # version ----------------------------------------------------------------
    if not isinstance(data["version"], str):
        raise DestinationMapError(
            f"'version' must be a string, got {type(data['version']).__name__}."
        )
    version = data["version"]
    if version not in _ACCEPTED_VERSIONS:
        raise DestinationMapError(
            f"Unsupported destination map schema version: {version!r}. "
            f"Accepted versions: {sorted(_ACCEPTED_VERSIONS)}. "
            f"Current version is {SCHEMA_VERSION!r}."
        )

    # destinations -----------------------------------------------------------
    if not isinstance(data["destinations"], dict):
        raise DestinationMapError(
            f"'destinations' must be a JSON object, "
            f"got {type(data['destinations']).__name__}."
        )
    if not data["destinations"]:
        raise DestinationMapError(
            "'destinations' must contain at least one entry."
        )

    # Validate and sort for determinism — stored in key-sorted order
    destinations: dict[str, str] = {}
    for idx, key in enumerate(sorted(data["destinations"].keys())):
        validated_key = _validate_dest_key(key, idx)
        validated_val = _validate_dest_template(data["destinations"][key], key)
        destinations[validated_key] = validated_val

    # conflict_policy --------------------------------------------------------
    conflict_policy = _DEFAULT_CONFLICT_POLICY
    if "conflict_policy" in data:
        cp = data["conflict_policy"]
        if not isinstance(cp, str):
            raise DestinationMapError(
                f"'conflict_policy' must be a string, got {type(cp).__name__}."
            )
        if cp not in _ALLOWED_CONFLICT_POLICIES:
            raise DestinationMapError(
                f"'conflict_policy' must be one of "
                f"{sorted(_ALLOWED_CONFLICT_POLICIES)}, got {cp!r}."
            )
        conflict_policy = cp

    # template_fallback ------------------------------------------------------
    template_fallback = _DEFAULT_TEMPLATE_FALLBACK
    if "template_fallback" in data:
        tf = data["template_fallback"]
        if not isinstance(tf, str):
            raise DestinationMapError(
                f"'template_fallback' must be a string, got {type(tf).__name__}."
            )
        if not tf.strip():
            raise DestinationMapError(
                "'template_fallback' must not be empty or whitespace-only."
            )
        template_fallback = tf.strip()

    # base_dir ---------------------------------------------------------------
    base_dir: Path
    if "base_dir" in data:
        bd = data["base_dir"]
        if not isinstance(bd, str):
            raise DestinationMapError(
                f"'base_dir' must be a string, got {type(bd).__name__}."
            )
        if not bd.strip():
            raise DestinationMapError(
                "'base_dir' must not be empty or whitespace-only."
            )
        base_dir = Path(bd.strip())
    elif default_base_dir is not None:
        base_dir = default_base_dir
    else:
        base_dir = Path.cwd()

    return DestinationMap(
        destinations=types.MappingProxyType(destinations),
        base_dir=base_dir,
        conflict_policy=conflict_policy,
        template_fallback=template_fallback,
        schema_version=version,
    )
