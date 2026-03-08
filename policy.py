"""
CleanSweep policy.py — v2.8.0 Policy Enforcement Layer.

Governs rule conflict behaviour when multiple rules match a single file.
This is a pure logic module: no filesystem access, no printing, no parsing.

Policy modes (valid values for the "policy_mode" config key and --policy flag):
  strict  — abort entire execution on the first conflict; zero files processed
  safe    — skip ambiguous files; engine continues with unambiguous files
  warn    — apply first (highest-priority) rule; emit RULE_OVERRIDE warning

Architecture contract (permanent):
  resolve_with_policy(filename, ruleset, policy_mode, file_size) -> PolicyResult
    Pure function. Deterministic. No filesystem access. No side effects.
    Called exclusively by planner.plan_with_policy().

  PolicyConflictError — raised only in strict mode.
  PolicyResult        — always returned in safe and warn modes.

Module boundaries (locked):
  ✗  No filesystem reads or writes
  ✗  No print() calls (logging via logger.py only)
  ✗  No rule schema parsing (rules.py)
  ✗  No configuration parsing (config.py)
  ✗  No CLI argument logic (main.py)

Log event codes (v1.2 observability layer):
  RULE_CONFLICT   — any multi-rule match, regardless of mode
  RULE_SKIPPED    — file excluded due to safe-mode conflict
  RULE_OVERRIDE   — first-rule resolution applied in warn mode
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import logger
from rules import Rule, RuleSet, find_all_matching_rules


# ---------------------------------------------------------------------------
# Mode constants
# ---------------------------------------------------------------------------

STRICT: Final[str] = "strict"
SAFE:   Final[str] = "safe"
WARN:   Final[str] = "warn"

VALID_POLICY_MODES: Final[frozenset[str]] = frozenset({STRICT, SAFE, WARN})
DEFAULT_POLICY_MODE: Final[str] = SAFE


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PolicyConflict:
    """
    Immutable record of a single rule-conflict event.

    matching_rules lists rule names in evaluation order
    (priority ASC, config_index ASC) — deterministic across runs.
    """
    filename:       str
    matching_rules: tuple[str, ...]   # rule names, evaluation order


@dataclass(frozen=True)
class PolicyResult:
    """
    Outcome of policy evaluation for one file.

    Exactly one outcome applies per instance:
      destination is not None → file should be processed at that destination
      skipped is True         → file is excluded (safe-mode conflict)

    Fields:
      destination — resolved destination key; None only when skipped=True
      skipped     — True when safe mode suppressed this file
      override    — True when warn mode applied first-rule resolution
      conflict    — populated on any multi-rule match (safe and warn modes)
    """
    destination: str | None           = None
    skipped:     bool                  = False
    override:    bool                  = False
    conflict:    PolicyConflict | None = None


@dataclass(frozen=True)
class PolicyMetrics:
    """
    Aggregated policy statistics for one batch run.

    Attached to BatchReport and rendered by report.py.

    Fields:
      mode               — active policy mode string
      conflicts_detected — total files with 2+ matching rules
      files_skipped      — files excluded by safe mode
      overrides_applied  — first-rule selections in warn mode
      conflict_details   — tuple of every PolicyConflict, sorted by filename
    """
    mode:               str
    conflicts_detected: int
    files_skipped:      int
    overrides_applied:  int
    conflict_details:   tuple[PolicyConflict, ...]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PolicyConflictError(Exception):
    """
    Raised exclusively in strict mode on the first rule conflict.

    Carries a human-readable message (CLI-ready) and the triggering conflict.
    Maps to exit code 2 (INVALID_ARGS) — a rule-configuration problem, not a
    runtime/filesystem error.

    Attributes:
      conflict — the PolicyConflict that triggered the abort
    """
    def __init__(self, message: str, conflict: PolicyConflict) -> None:
        super().__init__(message)
        self.conflict: PolicyConflict = conflict


# ---------------------------------------------------------------------------
# Core policy function
# ---------------------------------------------------------------------------

def resolve_with_policy(
    filename:    str,
    ruleset:     RuleSet,
    policy_mode: str,
    file_size:   int = 0,
) -> PolicyResult:
    """
    Resolve the destination for filename, applying the given policy mode.

    Pure function. Deterministic. No I/O. No side effects.

    Algorithm:
      1. Derive basename and suffix from filename (Path semantics).
      2. Collect ALL matching rules via find_all_matching_rules().
         Rules already sorted by (priority ASC, config_index ASC) in RuleSet.
      3. 0 matches → default_destination, no conflict.
      4. 1 match   → that rule's destination, no conflict.
      5. 2+ matches → apply policy_mode (strict / safe / warn).

    Args:
      filename    — file basename or full path; only the basename is used
      ruleset     — compiled, sorted RuleSet from rules.parse_rules()
      policy_mode — one of STRICT, SAFE, WARN
      file_size   — file size in bytes (default 0)

    Returns:
      PolicyResult with destination set  — no conflict, or warn mode selected rule
      PolicyResult with skipped=True     — safe mode excluded this file

    Raises:
      PolicyConflictError — strict mode only, on first conflict encountered

    Log events emitted (via logger):
      RULE_CONFLICT  — any 2+ match, all modes
      RULE_SKIPPED   — safe mode skip
      RULE_OVERRIDE  — warn mode first-rule selection
    """
    p        = Path(filename)
    basename = p.name
    suffix   = p.suffix.lower()

    matching = find_all_matching_rules(ruleset, basename, suffix, file_size)

    # ── No conflict ──────────────────────────────────────────────────────────
    if len(matching) == 0:
        return PolicyResult(destination=ruleset.default_destination)

    if len(matching) == 1:
        return PolicyResult(destination=matching[0].destination)

    # ── Conflict detected ────────────────────────────────────────────────────
    conflict = PolicyConflict(
        filename=filename,
        matching_rules=tuple(r.name for r in matching),
    )
    rule_list = ", ".join(conflict.matching_rules)

    logger.log_warn(
        f"RULE_CONFLICT: {filename!r} matched [{rule_list}] — policy: {policy_mode}"
    )

    # ── Strict mode: abort ───────────────────────────────────────────────────
    if policy_mode == STRICT:
        formatted_rules = "[" + "], [".join(conflict.matching_rules) + "]"
        msg = (
            f"ERROR: Rule conflict detected\n"
            f"File: {filename}\n"
            f"Matching rules: {formatted_rules}\n\n"
            f"Policy: strict\n"
            f"Execution aborted."
        )
        logger.log_error(f"RULE_CONFLICT: strict abort — {filename!r} matched [{rule_list}]")
        raise PolicyConflictError(msg, conflict)

    # ── Safe mode: skip file ─────────────────────────────────────────────────
    if policy_mode == SAFE:
        logger.log_warn(
            f"RULE_SKIPPED: {filename!r} excluded — matched [{rule_list}] (policy: safe)"
        )
        return PolicyResult(skipped=True, conflict=conflict)

    # ── Warn mode: first rule wins (deterministic) ───────────────────────────
    selected = matching[0]
    logger.log_warn(
        f"RULE_OVERRIDE: {filename!r} matched [{rule_list}] "
        f"— using [{selected.name}] (policy: warn)"
    )
    return PolicyResult(
        destination=selected.destination,
        override=True,
        conflict=conflict,
    )
