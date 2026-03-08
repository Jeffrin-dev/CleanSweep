"""
CleanSweep v2.2.0 — Rule Engine.

This module is the sole authority for:
  - JSON rule schema validation (versions "2.0" and "2.2")
  - Rule conflict detection
  - Extension normalization
  - Size constraint validation
  - Filename pattern validation
  - Priority-based deterministic destination resolution

Architecture contract (permanent, locked in INVARIANTS.md §18):

  resolve_destination(filename, ruleset, file_size=0) → str
    Pure function. Deterministic. No filesystem access. No side effects.

  parse_rules(data) → RuleSet
    Validates raw dict. Returns immutable RuleSet. Raises RuleError on any violation.
    No I/O. No CLI access. No fallback on invalid input.

─────────────────────────────────────────────────────────────────────────────
v2.2 Evaluation semantics (supersedes v2.0 extension-index lookup):

  Rules are sorted at parse time by (priority ASC, config_index ASC).
  Priority is an explicit integer field. Lower number = evaluated first.
  Equal priority → config file order preserved (stable sort).
  No hidden defaults — unspecified priority defaults to 0 (documented here).

  For each file, rules are evaluated in sorted order.
  First rule where ALL present criteria match wins (logical AND).
  If no rule matches → default_destination.

  Criteria evaluated per rule:
    extensions      — file suffix (lowercased) must be in frozenset
    min_size        — file_size (bytes) must be >= min_size (inclusive)
    max_size        — file_size (bytes) must be <= max_size (inclusive)
    filename_pattern — fnmatch glob against filename (case-insensitive:
                       both pattern and filename are lowercased before matching)

  A rule with extensions=None AND no size/pattern constraints is a parse error.
  At least one match criterion is required per rule.

─────────────────────────────────────────────────────────────────────────────
Schema versioning:

  SCHEMA_VERSION = "2.2"  — current version
  Accepted versions: "2.0", "2.2"

  Version "2.0" rules:
    - extensions required inside match
    - priority, min_size, max_size, filename_pattern are NOT allowed
    - Backward compatible — parsed identically to before

  Version "2.2" rules:
    - extensions optional inside match
    - priority at rule level (int, default 0, no hidden behaviour)
    - min_size, max_size, filename_pattern optional inside match
    - At least one match criterion required (enforced at parse time)

─────────────────────────────────────────────────────────────────────────────
JSON schema (v2.2):
  {
    "version": "2.2",
    "rules": [
      {
        "name": "LargeVideos",
        "priority": 1,
        "match": {
          "extensions": [".mp4", ".mkv"],
          "min_size": 104857600
        },
        "destination": "LargeVideos"
      },
      {
        "name": "BackupZips",
        "priority": 2,
        "match": {
          "extensions": [".zip"],
          "filename_pattern": "*backup*"
        },
        "destination": "Backups"
      }
    ],
    "default_destination": "Others"
  }

─────────────────────────────────────────────────────────────────────────────
Size constraint rules:
  - Values are raw bytes — no unit parsing in engine core
  - Non-negative integers only (type: int, value >= 0)
  - min_size and max_size are inclusive boundaries
  - If both present: max_size must be >= min_size (enforced at parse time)

Pattern constraint rules:
  - fnmatch glob syntax only — no regex, no partial regex
  - Applied to filename only (not full path)
  - Case-insensitive: both pattern and filename lowercased before matching
  - Non-empty string required
  - Whitespace: same policy as extensions — trailing/leading whitespace is a hard error

Extension rules (unchanged from v2.0):
  - Trailing whitespace is a hard error — not silently stripped
  - Lowercased
  - Leading dot ensured
  - Format validated: must be .[a-z0-9]+ after normalization
  - Duplicates within a single rule are a hard error

Destination validation rules (unchanged from v2.0):
  - Empty or whitespace-only
  - Path separators (/ or \\)
  - Path traversal components (. or ..)
  - Null bytes
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION: Final[str] = "2.2"
_ACCEPTED_VERSIONS: Final[frozenset[str]] = frozenset({"2.0", "2.2"})

# Top-level keys — identical for both schema versions
_REQUIRED_TOP_KEYS: Final[frozenset[str]] = frozenset({"version", "rules", "default_destination"})
_ALLOWED_TOP_KEYS:  Final[frozenset[str]] = frozenset({"version", "rules", "default_destination"})

# v2.0 rule-level keys (strict subset — new fields forbidden)
_REQUIRED_RULE_KEYS_V20: Final[frozenset[str]] = frozenset({"name", "match", "destination"})
_ALLOWED_RULE_KEYS_V20:  Final[frozenset[str]] = frozenset({"name", "match", "destination"})

# v2.0 match-level keys
_REQUIRED_MATCH_KEYS_V20: Final[frozenset[str]] = frozenset({"extensions"})
_ALLOWED_MATCH_KEYS_V20:  Final[frozenset[str]] = frozenset({"extensions"})

# v2.2 rule-level keys (adds "priority")
_REQUIRED_RULE_KEYS_V22: Final[frozenset[str]] = frozenset({"name", "match", "destination"})
_ALLOWED_RULE_KEYS_V22:  Final[frozenset[str]] = frozenset({"name", "match", "destination", "priority"})

# v2.2 match-level keys (adds size and pattern constraints)
_ALLOWED_MATCH_KEYS_V22: Final[frozenset[str]] = frozenset({
    "extensions", "min_size", "max_size", "filename_pattern"
})

# Valid extension after normalization: exactly one dot, then one or more
# lowercase alphanumeric characters.
_VALID_EXT_RE: Final[re.Pattern[str]] = re.compile(r"^\.[a-z0-9]+$")

# Reserved destination names
_RESERVED_DESTINATIONS: Final[frozenset[str]] = frozenset({".", ".."})

# Default priority when "priority" field is absent in v2.2 rules
_DEFAULT_PRIORITY: Final[int] = 0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RuleError(Exception):
    """
    Raised when rule JSON fails schema validation or contains conflicts.

    Always carries a human-readable message naming the exact violation.
    Never swallowed — callers must handle and map to exit code 2.
    """


# ---------------------------------------------------------------------------
# Immutable data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    """
    One compiled, validated, normalized rule (v2.2).

    Fields:
      name             — unique identifier string
      destination      — target folder name (validated, no path separators)
      extensions       — frozenset of lowercased dot-prefixed extensions,
                         or None if no extension constraint
      priority         — sort key: lower number = evaluated first (default 0)
      min_size         — inclusive lower bound in bytes, or None
      max_size         — inclusive upper bound in bytes, or None
      filename_pattern — fnmatch glob string (stored as-is; matching is
                         case-insensitive via lowercasing at match time), or None

    Invariant: at least one of (extensions, min_size, max_size, filename_pattern)
               is not None. Enforced at parse time.
    """
    name:             str
    destination:      str
    extensions:       frozenset[str] | None  = field(default=None)
    priority:         int                    = field(default=0)
    min_size:         int | None             = field(default=None)
    max_size:         int | None             = field(default=None)
    filename_pattern: str | None             = field(default=None)


@dataclass(frozen=True)
class RuleSet:
    """
    Immutable, validated set of rules produced by parse_rules().

    rules:               ordered tuple, sorted by (priority ASC, config_index ASC).
                         Evaluation order for resolve_destination() is this tuple order.
    default_destination: fallback for files matching no rule.
    schema_version:      "2.0" or "2.2" — version of the source JSON.
    _ext_index:          extension → first matching rule index in sorted rules tuple.
                         Populated for introspection and backward compatibility.
                         NOT used by resolve_destination() (linear scan used instead
                         to support multi-criteria rules correctly).
    """
    rules:               tuple[Rule, ...]
    default_destination: str
    schema_version:      str             = field(default="2.2")
    _ext_index:          dict[str, int]  = field(default_factory=dict,
                                                  compare=False, hash=False)

    def __repr__(self) -> str:
        return (
            f"RuleSet(rules={len(self.rules)}, "
            f"schema={self.schema_version!r}, "
            f"default={self.default_destination!r})"
        )


# ---------------------------------------------------------------------------
# Extension normalization and validation  (unchanged from v2.0)
# ---------------------------------------------------------------------------

def _normalize_extension(ext: str, rule_index: int, ext_index: int) -> str:
    """
    Validate and normalize one extension string.

    Validation order (each step is a hard error, not a silent fix):
      1. Reject leading/trailing whitespace
      2. Lowercase
      3. Ensure exactly one leading dot
      4. Validate format: must match ^\\.[a-z0-9]+$

    Returns normalized extension string (e.g. ".jpg").
    Raises RuleError on any violation.
    """
    prefix = f"Rule[{rule_index}].match.extensions[{ext_index}]"

    if ext != ext.strip():
        raise RuleError(
            f"{prefix}: extension {ext!r} contains leading or trailing whitespace. "
            f"Remove the whitespace or use the exact extension string."
        )

    normalized = ext.lower()

    if not normalized.startswith("."):
        normalized = "." + normalized
    if normalized.startswith(".."):
        raise RuleError(
            f"{prefix}: extension {ext!r} produces invalid normalized form "
            f"{normalized!r} (double dot). Use a single leading dot, e.g. '.jpg'."
        )

    if not _VALID_EXT_RE.match(normalized):
        raise RuleError(
            f"{prefix}: extension {ext!r} normalizes to {normalized!r} which is "
            f"not a valid extension. Extensions must match .[a-z0-9]+ "
            f"(one dot, only letters and digits after it)."
        )

    return normalized


def _check_intra_rule_duplicates(
    rule_index: int,
    raw_exts: list[str],
    normalized: list[str],
) -> None:
    """
    Reject duplicate extensions within a single rule (after normalization).

    Example: [".jpg", ".JPG", "jpg"] all normalize to ".jpg" — hard error.
    """
    seen: dict[str, str] = {}
    for raw, norm in zip(raw_exts, normalized):
        if norm in seen:
            raise RuleError(
                f"Rule[{rule_index}]: duplicate extension after normalization: "
                f"{raw!r} and {seen[norm]!r} both normalize to {norm!r}. "
                f"Each extension may appear at most once per rule."
            )
        seen[norm] = raw


# ---------------------------------------------------------------------------
# Destination validation  (unchanged from v2.0)
# ---------------------------------------------------------------------------

def _validate_destination(destination: str, context: str) -> str:
    """
    Validate a destination folder name string.

    Returns stripped destination string.
    Raises RuleError on any violation.
    """
    stripped = destination.strip()

    if not stripped:
        raise RuleError(f"{context}: destination must not be empty.")

    if "\x00" in stripped:
        raise RuleError(
            f"{context}: destination {destination!r} contains a null byte."
        )

    if "/" in stripped or "\\" in stripped:
        raise RuleError(
            f"{context}: destination {destination!r} contains a path separator "
            f"(/ or \\). Destinations must be simple folder names, not paths."
        )

    if stripped in _RESERVED_DESTINATIONS:
        raise RuleError(
            f"{context}: destination {destination!r} is a reserved name "
            f"(. and .. are not valid destination folders)."
        )

    return stripped


# ---------------------------------------------------------------------------
# Schema validation — top level
# ---------------------------------------------------------------------------

def _validate_top_level(data: dict) -> str:
    """
    Validate top-level structure. Returns detected schema version string.

    Raises RuleError on any violation.
    """
    if not isinstance(data, dict):
        raise RuleError(f"Rule file must be a JSON object, got {type(data).__name__}.")

    unknown = set(data.keys()) - _ALLOWED_TOP_KEYS
    if unknown:
        raise RuleError(f"Unknown top-level keys: {sorted(unknown)}.")

    missing = _REQUIRED_TOP_KEYS - set(data.keys())
    if missing:
        raise RuleError(f"Missing required top-level keys: {sorted(missing)}.")

    if not isinstance(data["version"], str):
        raise RuleError(
            f"'version' must be a string, got {type(data['version']).__name__}."
        )
    version = data["version"]
    if version not in _ACCEPTED_VERSIONS:
        raise RuleError(
            f"Unsupported rule schema version: {version!r}. "
            f"Accepted versions: {sorted(_ACCEPTED_VERSIONS)}. "
            f"Current version is {SCHEMA_VERSION!r}."
        )

    if not isinstance(data["rules"], list):
        raise RuleError(
            f"'rules' must be a list, got {type(data['rules']).__name__}."
        )
    if len(data["rules"]) == 0:
        raise RuleError("'rules' must contain at least one rule.")

    if not isinstance(data["default_destination"], str):
        raise RuleError(
            f"'default_destination' must be a string, "
            f"got {type(data['default_destination']).__name__}."
        )
    _validate_destination(data["default_destination"], "'default_destination'")

    return version


# ---------------------------------------------------------------------------
# Schema validation — rule level (version-aware)
# ---------------------------------------------------------------------------

def _validate_rule_v20(raw: object, index: int) -> tuple[str, list[str], str]:
    """
    Validate one v2.0 rule entry.

    Returns (name, raw_extensions, destination).
    Raises RuleError on any violation, including presence of v2.2-only fields.
    """
    prefix = f"Rule[{index}]"

    if not isinstance(raw, dict):
        raise RuleError(f"{prefix}: must be a JSON object, got {type(raw).__name__}.")

    unknown = set(raw.keys()) - _ALLOWED_RULE_KEYS_V20
    if unknown:
        raise RuleError(
            f"{prefix}: unknown keys in v2.0 schema: {sorted(unknown)}. "
            f"Use schema version '2.2' to enable 'priority' and size/pattern match fields."
        )

    missing = _REQUIRED_RULE_KEYS_V20 - set(raw.keys())
    if missing:
        raise RuleError(f"{prefix}: missing required keys: {sorted(missing)}.")

    if not isinstance(raw["name"], str):
        raise RuleError(f"{prefix}: 'name' must be a string, got {type(raw['name']).__name__}.")
    if not raw["name"].strip():
        raise RuleError(f"{prefix}: 'name' must not be empty.")

    if not isinstance(raw["destination"], str):
        raise RuleError(
            f"{prefix}: 'destination' must be a string, "
            f"got {type(raw['destination']).__name__}."
        )
    _validate_destination(raw["destination"], f"{prefix} 'destination'")

    if not isinstance(raw["match"], dict):
        raise RuleError(
            f"{prefix}: 'match' must be a JSON object, "
            f"got {type(raw['match']).__name__}."
        )

    unknown_match = set(raw["match"].keys()) - _ALLOWED_MATCH_KEYS_V20
    if unknown_match:
        raise RuleError(
            f"{prefix}: unknown 'match' keys in v2.0 schema: {sorted(unknown_match)}. "
            f"Use schema version '2.2' to enable size/pattern match fields."
        )

    missing_match = _REQUIRED_MATCH_KEYS_V20 - set(raw["match"].keys())
    if missing_match:
        raise RuleError(f"{prefix}: missing 'match' keys: {sorted(missing_match)}.")

    extensions = raw["match"]["extensions"]
    if not isinstance(extensions, list):
        raise RuleError(
            f"{prefix}: 'match.extensions' must be a list, "
            f"got {type(extensions).__name__}."
        )
    if len(extensions) == 0:
        raise RuleError(f"{prefix}: 'match.extensions' must not be empty.")
    for i, ext in enumerate(extensions):
        if not isinstance(ext, str):
            raise RuleError(
                f"{prefix}: 'match.extensions[{i}]' must be a string, "
                f"got {type(ext).__name__}."
            )

    return raw["name"].strip(), list(extensions), raw["destination"].strip()


def _validate_rule_v22(raw: object, index: int) -> tuple[str, list[str] | None, str, int,
                                                          int | None, int | None, str | None]:
    """
    Validate one v2.2 rule entry.

    Returns (name, raw_extensions_or_None, destination, priority,
             min_size, max_size, filename_pattern).

    All match criteria are optional, but at least one must be present.
    Raises RuleError on any violation.
    """
    prefix = f"Rule[{index}]"

    if not isinstance(raw, dict):
        raise RuleError(f"{prefix}: must be a JSON object, got {type(raw).__name__}.")

    unknown = set(raw.keys()) - _ALLOWED_RULE_KEYS_V22
    if unknown:
        raise RuleError(f"{prefix}: unknown keys: {sorted(unknown)}.")

    missing = _REQUIRED_RULE_KEYS_V22 - set(raw.keys())
    if missing:
        raise RuleError(f"{prefix}: missing required keys: {sorted(missing)}.")

    # name
    if not isinstance(raw["name"], str):
        raise RuleError(f"{prefix}: 'name' must be a string, got {type(raw['name']).__name__}.")
    if not raw["name"].strip():
        raise RuleError(f"{prefix}: 'name' must not be empty.")

    # destination
    if not isinstance(raw["destination"], str):
        raise RuleError(
            f"{prefix}: 'destination' must be a string, "
            f"got {type(raw['destination']).__name__}."
        )
    _validate_destination(raw["destination"], f"{prefix} 'destination'")

    # priority (optional, defaults to _DEFAULT_PRIORITY)
    priority = _DEFAULT_PRIORITY
    if "priority" in raw:
        if not isinstance(raw["priority"], int) or isinstance(raw["priority"], bool):
            raise RuleError(
                f"{prefix}: 'priority' must be an integer, "
                f"got {type(raw['priority']).__name__}."
            )
        priority = raw["priority"]

    # match object
    if not isinstance(raw["match"], dict):
        raise RuleError(
            f"{prefix}: 'match' must be a JSON object, "
            f"got {type(raw['match']).__name__}."
        )

    unknown_match = set(raw["match"].keys()) - _ALLOWED_MATCH_KEYS_V22
    if unknown_match:
        raise RuleError(f"{prefix}: unknown 'match' keys: {sorted(unknown_match)}.")

    if len(raw["match"]) == 0:
        raise RuleError(
            f"{prefix}: 'match' must contain at least one criterion "
            f"(extensions, min_size, max_size, or filename_pattern)."
        )

    match = raw["match"]

    # extensions (optional in v2.2)
    raw_extensions: list[str] | None = None
    if "extensions" in match:
        extensions = match["extensions"]
        if not isinstance(extensions, list):
            raise RuleError(
                f"{prefix}: 'match.extensions' must be a list, "
                f"got {type(extensions).__name__}."
            )
        if len(extensions) == 0:
            raise RuleError(f"{prefix}: 'match.extensions' must not be empty.")
        for i, ext in enumerate(extensions):
            if not isinstance(ext, str):
                raise RuleError(
                    f"{prefix}: 'match.extensions[{i}]' must be a string, "
                    f"got {type(ext).__name__}."
                )
        raw_extensions = list(extensions)

    # min_size (optional)
    min_size: int | None = None
    if "min_size" in match:
        v = match["min_size"]
        if not isinstance(v, int) or isinstance(v, bool):
            raise RuleError(
                f"{prefix}: 'match.min_size' must be an integer (bytes), "
                f"got {type(v).__name__}."
            )
        if v < 0:
            raise RuleError(
                f"{prefix}: 'match.min_size' must be >= 0, got {v}."
            )
        min_size = v

    # max_size (optional)
    max_size: int | None = None
    if "max_size" in match:
        v = match["max_size"]
        if not isinstance(v, int) or isinstance(v, bool):
            raise RuleError(
                f"{prefix}: 'match.max_size' must be an integer (bytes), "
                f"got {type(v).__name__}."
            )
        if v < 0:
            raise RuleError(
                f"{prefix}: 'match.max_size' must be >= 0, got {v}."
            )
        max_size = v

    # size range coherence: max >= min when both specified
    if min_size is not None and max_size is not None:
        if max_size < min_size:
            raise RuleError(
                f"{prefix}: 'match.max_size' ({max_size}) must be >= "
                f"'match.min_size' ({min_size})."
            )

    # filename_pattern (optional)
    filename_pattern: str | None = None
    if "filename_pattern" in match:
        p = match["filename_pattern"]
        if not isinstance(p, str):
            raise RuleError(
                f"{prefix}: 'match.filename_pattern' must be a string, "
                f"got {type(p).__name__}."
            )
        if p != p.strip():
            raise RuleError(
                f"{prefix}: 'match.filename_pattern' {p!r} contains leading or "
                f"trailing whitespace. Remove the whitespace."
            )
        if not p.strip():
            raise RuleError(
                f"{prefix}: 'match.filename_pattern' must not be empty."
            )
        filename_pattern = p

    return (
        raw["name"].strip(),
        raw_extensions,
        raw["destination"].strip(),
        priority,
        min_size,
        max_size,
        filename_pattern,
    )


# ---------------------------------------------------------------------------
# Conflict detection — duplicate names only
# ---------------------------------------------------------------------------

def _detect_name_conflicts(names: list[str]) -> None:
    """
    Detect duplicate rule names across all rules.

    Names must be unique. Overlapping extensions across rules are NOT an error.
    Raises RuleError naming both conflicting rule indices.
    """
    seen: dict[str, int] = {}
    for idx, name in enumerate(names):
        if name in seen:
            raise RuleError(
                f"Duplicate rule name {name!r}: "
                f"appears at index {seen[name]} and {idx}."
            )
        seen[name] = idx


# ---------------------------------------------------------------------------
# Rule matching — pure function, no I/O
# ---------------------------------------------------------------------------

def _matches_rule(rule: Rule, filename: str, suffix: str, file_size: int) -> bool:
    """
    Return True if all criteria in rule match the given file attributes.

    Criteria are evaluated as logical AND — all present constraints must pass.
    A criterion absent from the rule (None) is not evaluated (no constraint).

    Args:
      rule      — compiled Rule
      filename  — basename only (not full path), used for pattern matching
      suffix    — lowercased file extension (e.g. ".mp4"), used for extension matching
      file_size — file size in bytes

    No I/O. No side effects. Deterministic.
    """
    if rule.extensions is not None:
        if suffix not in rule.extensions:
            return False

    if rule.min_size is not None:
        if file_size < rule.min_size:
            return False

    if rule.max_size is not None:
        if file_size > rule.max_size:
            return False

    if rule.filename_pattern is not None:
        # Case-insensitive: lowercase both sides before matching
        if not fnmatch.fnmatchcase(filename.lower(), rule.filename_pattern.lower()):
            return False

    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_rules(data: dict) -> RuleSet:
    """
    Validate raw dict and return an immutable RuleSet.

    Steps:
      1. Validate and detect schema version ("2.0" or "2.2")
      2. Validate each rule entry per its schema version
      3. Normalize and validate all extensions per rule
      4. Detect intra-rule duplicate extensions (hard error)
      5. Detect duplicate rule names (hard error)
      6. Sort rules by (priority ASC, config_index ASC)
      7. Return frozen RuleSet

    Overlapping extensions across rules are ALLOWED.
    First rule in sorted evaluation order that matches wins.

    Raises RuleError on any violation.
    No I/O. No side effects. Deterministic.
    """
    version = _validate_top_level(data)

    compiled: list[Rule] = []
    names: list[str] = []

    for i, raw in enumerate(data["rules"]):
        if version == "2.0":
            name, raw_exts, destination = _validate_rule_v20(raw, i)
            normalized_list = [
                _normalize_extension(e, rule_index=i, ext_index=j)
                for j, e in enumerate(raw_exts)
            ]
            _check_intra_rule_duplicates(i, raw_exts, normalized_list)
            rule = Rule(
                name=name,
                destination=destination,
                extensions=frozenset(normalized_list),
                priority=_DEFAULT_PRIORITY,
            )

        else:  # version == "2.2"
            name, raw_exts, destination, priority, min_size, max_size, pattern = (
                _validate_rule_v22(raw, i)
            )
            ext_frozenset: frozenset[str] | None = None
            if raw_exts is not None:
                normalized_list = [
                    _normalize_extension(e, rule_index=i, ext_index=j)
                    for j, e in enumerate(raw_exts)
                ]
                _check_intra_rule_duplicates(i, raw_exts, normalized_list)
                ext_frozenset = frozenset(normalized_list)

            rule = Rule(
                name=name,
                destination=destination,
                extensions=ext_frozenset,
                priority=priority,
                min_size=min_size,
                max_size=max_size,
                filename_pattern=pattern,
            )

        compiled.append(rule)
        names.append(name)

    _detect_name_conflicts(names)

    # Sort by (priority ASC, config_index ASC) — stable, deterministic
    indexed = list(enumerate(compiled))
    indexed.sort(key=lambda pair: (pair[1].priority, pair[0]))
    sorted_rules = tuple(rule for _, rule in indexed)

    # Build extension → rule index for introspection and backward compat.
    # Maps extension to its first-match index in the sorted rules tuple.
    # Only rules with an extensions constraint contribute.
    ext_index: dict[str, int] = {}
    for idx, rule in enumerate(sorted_rules):
        if rule.extensions is not None:
            for ext in rule.extensions:
                if ext not in ext_index:
                    ext_index[ext] = idx

    return RuleSet(
        rules=sorted_rules,
        default_destination=data["default_destination"].strip(),
        schema_version=version,
        _ext_index=ext_index,
    )


def resolve_destination(
    filename: str,
    ruleset: RuleSet,
    file_size: int = 0,
) -> str:
    """
    Return the destination folder name for filename under ruleset.

    Pure function. Deterministic. No filesystem access. No side effects.

    Algorithm:
      1. Extract basename and suffix (Path)
      2. Lowercase the suffix for extension matching
      3. Evaluate rules in sorted order (priority ASC, config_index ASC)
      4. Return first matching rule's destination, or default_destination

    Args:
      filename  — file name (basename) or full path; only the basename is used
      ruleset   — compiled, sorted RuleSet
      file_size — file size in bytes (default 0, safe for extension-only rulesets)

    Edge cases:
      - No extension (e.g. "Makefile")  → evaluated against non-extension criteria
      - Dotfile (e.g. ".bashrc")        → suffix=".bashrc" treated as extension
      - Multiple dots (e.g. "a.tar.gz") → uses last suffix (.gz)
      - file_size=0 with no size rules  → works correctly (size not evaluated)
    """
    p = Path(filename)
    basename = p.name
    suffix = p.suffix.lower()

    for rule in ruleset.rules:
        if _matches_rule(rule, basename, suffix, file_size):
            return rule.destination

    return ruleset.default_destination


def find_all_matching_rules(
    ruleset:   RuleSet,
    basename:  str,
    suffix:    str,
    file_size: int,
) -> list["Rule"]:
    """
    Return every rule in ruleset that matches the given file attributes.

    Evaluates rules in sorted order (priority ASC, config_index ASC) —
    identical to resolve_destination's evaluation pass.

    Returns an empty list when no rule matches.
    Returns a single-element list when exactly one rule matches (no conflict).
    Returns 2+ elements when a policy conflict exists.

    Pure function. Deterministic. No I/O. No side effects.
    Used exclusively by policy.py for conflict detection.

    Args:
      ruleset   — compiled, sorted RuleSet
      basename  — file basename (not full path)
      suffix    — lowercased file extension (e.g. ".mp4")
      file_size — file size in bytes
    """
    return [
        rule for rule in ruleset.rules
        if _matches_rule(rule, basename, suffix, file_size)
    ]


# ---------------------------------------------------------------------------
# Built-in default ruleset — v2.0 schema for backward compatibility
# ---------------------------------------------------------------------------

_DEFAULT_RULES_DATA: Final[dict] = {
    "version": "2.0",
    "rules": [
        {
            "name": "Images",
            "match": {"extensions": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]},
            "destination": "Images",
        },
        {
            "name": "Documents",
            "match": {"extensions": [".pdf", ".docx", ".txt", ".xlsx", ".csv", ".md"]},
            "destination": "Documents",
        },
        {
            "name": "Videos",
            "match": {"extensions": [".mp4", ".mkv", ".avi", ".mov", ".wmv"]},
            "destination": "Videos",
        },
        {
            "name": "Audio",
            "match": {"extensions": [".mp3", ".wav", ".flac", ".aac", ".ogg"]},
            "destination": "Audio",
        },
        {
            "name": "Archives",
            "match": {"extensions": [".zip", ".tar", ".gz", ".rar", ".7z"]},
            "destination": "Archives",
        },
        {
            "name": "Code",
            "match": {"extensions": [".py", ".js", ".ts", ".html", ".css", ".json", ".yaml"]},
            "destination": "Code",
        },
    ],
    "default_destination": "Others",
}

# Module-level constant — built once, never mutated
DEFAULT_RULESET: Final[RuleSet] = parse_rules(_DEFAULT_RULES_DATA)
