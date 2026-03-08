"""
CleanSweep planner.py — v2.4.0 Planner Layer.

Pure planning layer. Zero filesystem I/O.

Responsibilities:
  - Define MoveAction: immutable action descriptor
  - Define FileMetadata: pre-read file attributes
  - plan_actions(): pure function — metadata + rules + dest_map → action list

Architectural contracts (permanent):
  ✗  No filesystem reads or writes (no stat, no open, no mkdir, no move)
  ✗  No rule schema parsing (delegated to rules.py)
  ✗  No destination map parsing (delegated to destination_map.py)
  ✗  No conflict resolution against disk state
  ✗  No printing or logging

  plan_actions(file_metadata, ruleset, dest_map, scan_root) → tuple[MoveAction, ...]
    Pure. Deterministic. Side-effect-free.
    Same inputs always produce identical outputs.
    Raises DestinationMapError if ruleset produces keys absent from dest_map.

Three-layer flow (locked v2.4.0):
  Planner     → tuple[MoveAction, ...]   (zero I/O)
  BatchEngine → validation + execution lifecycle
  Organizer   → single-file filesystem operations
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from destination_map import (
    DestinationMap,
    DestinationMapError,
    resolve_destination_path,
    validate_ruleset_destinations,
)
from rules import RuleSet, resolve_destination


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FileMetadata:
    """
    Pre-read file attributes. Collected by batch_engine before planning.

    Immutable by design — planner operates on a snapshot of reality,
    not a live filesystem view.
    """
    path:     Path       # absolute source file path
    size:     int        # bytes (0 if stat failed)
    mtime_ns: int | None # nanosecond mtime (None if stat failed)


@dataclass(frozen=True)
class MoveAction:
    """
    Immutable descriptor for a single file move operation.

    Created exclusively by plan_actions().
    Consumed exclusively by BatchEngine.
    Never mutated after creation.

    dst_filename=None: preserve src.name as destination filename.
    """
    src:          Path        # absolute source file path
    dst_dir:      Path        # absolute destination directory (not a file path)
    dst_filename: str | None  # None = preserve src.name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_ruleset_destinations(ruleset: RuleSet) -> set[str]:
    """Return all destination keys a ruleset can produce (rules + default)."""
    return {rule.destination for rule in ruleset.rules} | {ruleset.default_destination}


# ---------------------------------------------------------------------------
# Pure planning function
# ---------------------------------------------------------------------------

def plan_actions(
    file_metadata: tuple[FileMetadata, ...],
    ruleset:       RuleSet,
    dest_map:      DestinationMap | None,
    scan_root:     Path,
) -> tuple[MoveAction, ...]:
    """
    Pure function: file metadata + rules + dest_map → immutable action tuple.

    Contracts:
      - Zero filesystem I/O. No stat, no open, no mkdir, no move.
      - Deterministic: same inputs always produce identical outputs.
      - Output sorted by source filename for stable, reproducible ordering.
      - dest_map=None: destinations resolve relative to scan_root (v2.2 compat).
      - Actions list immutable after return (frozen dataclass in tuple).

    Args:
      file_metadata: pre-read file attributes (already sorted by filename).
      ruleset:       active rule set for destination key resolution.
      dest_map:      optional destination map for path resolution.
      scan_root:     root directory of the scan (v2.2 compat path only).

    Raises:
      DestinationMapError: if ruleset produces a destination key absent from dest_map.

    Returns:
      Immutable tuple of MoveAction, sorted by src.name.
    """
    if not file_metadata:
        return ()

    # Early validation: all ruleset keys must exist in dest_map.
    # Pure — no I/O. Raises DestinationMapError on violation.
    if dest_map is not None:
        validate_ruleset_destinations(_extract_ruleset_destinations(ruleset), dest_map)

    actions: list[MoveAction] = []

    for meta in file_metadata:
        # Resolve logical destination key (pure rule evaluation)
        dest_key = resolve_destination(meta.path.name, ruleset, meta.size)

        # Resolve physical destination directory (pure path computation)
        if dest_map is not None:
            dst_dir = resolve_destination_path(
                dest_key,
                meta.path.name,
                meta.size,
                meta.mtime_ns,
                dest_map,
            )
        else:
            # v2.2 compat: destination relative to scan_root
            dst_dir = scan_root / dest_key

        actions.append(MoveAction(
            src=meta.path,
            dst_dir=dst_dir,
            dst_filename=None,
        ))

    # Deterministic sort: by source filename (stable, reproducible across runs)
    return tuple(sorted(actions, key=lambda a: a.src.name))


# ---------------------------------------------------------------------------
# Policy-aware plan result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PolicyPlanResult:
    """
    Output of plan_with_policy() — actions tuple plus policy metrics.

    Fields:
      actions        — immutable tuple of MoveAction to execute
                       (files skipped by policy are absent)
      metrics        — aggregated policy statistics for this batch
      aborted        — True when strict mode halted planning on a conflict;
                       actions will be empty and abort_detail will be set
      abort_detail   — human-readable abort message (strict mode only)
      abort_conflict — the PolicyConflict that caused the abort (strict only)
    """
    actions:        tuple[MoveAction, ...]
    metrics:        "PolicyMetrics"
    aborted:        bool                        = False
    abort_detail:   str                         = ""
    abort_conflict: "PolicyConflict | None"     = None


# ---------------------------------------------------------------------------
# Policy-aware planning function
# ---------------------------------------------------------------------------

def plan_with_policy(
    file_metadata: tuple["FileMetadata", ...],
    ruleset:       RuleSet,
    dest_map:      "DestinationMap | None",
    scan_root:     Path,
    policy_mode:   str,
) -> PolicyPlanResult:
    """
    Policy-aware planning: metadata + rules + dest_map + policy → PolicyPlanResult.

    Wraps plan_actions() with per-file policy evaluation. Files that trigger a
    conflict are handled according to policy_mode before any MoveAction is built:
      strict — entire plan aborted on first conflict; PolicyPlanResult.aborted=True
      safe   — conflicting file excluded; included in metrics.files_skipped
      warn   — first rule applied; override recorded in metrics.overrides_applied

    Contracts:
      - Pure. Deterministic. Zero filesystem I/O.
      - policy_mode must be one of "strict", "safe", "warn".
      - Output actions sorted by src.name (inherited from plan_actions).
      - PolicyMetrics conflict_details sorted by filename (stable ordering).

    Args:
      file_metadata — pre-read file attributes, sorted by filename.
      ruleset       — active rule set.
      dest_map      — optional destination map for path resolution.
      scan_root     — root directory for v2.2-compat path resolution.
      policy_mode   — conflict behaviour: "strict" | "safe" | "warn".

    Returns:
      PolicyPlanResult with actions and metrics.
      On strict-mode abort: aborted=True, actions=(), abort_detail and
      abort_conflict set.

    Raises:
      DestinationMapError — if ruleset produces a destination key absent from dest_map.
    """
    from policy import (
        resolve_with_policy,
        PolicyConflictError,
        PolicyConflict,
        PolicyMetrics,
    )

    if not file_metadata:
        from policy import PolicyMetrics
        return PolicyPlanResult(
            actions=(),
            metrics=PolicyMetrics(
                mode=policy_mode,
                conflicts_detected=0,
                files_skipped=0,
                overrides_applied=0,
                conflict_details=(),
            ),
        )

    # Early dest_map validation — pure, no I/O.
    if dest_map is not None:
        validate_ruleset_destinations(_extract_ruleset_destinations(ruleset), dest_map)

    # ── Per-file policy evaluation ──────────────────────────────────────────
    accepted_meta:    list[FileMetadata]    = []
    conflict_details: list[PolicyConflict] = []
    files_skipped:    int                  = 0
    overrides_applied: int                 = 0

    for meta in file_metadata:
        try:
            result = resolve_with_policy(
                filename    = meta.path.name,
                ruleset     = ruleset,
                policy_mode = policy_mode,
                file_size   = meta.size,
            )
        except PolicyConflictError as exc:
            # Strict mode: abort entire plan immediately.
            from policy import PolicyMetrics
            return PolicyPlanResult(
                actions=(),
                metrics=PolicyMetrics(
                    mode=policy_mode,
                    conflicts_detected=1,
                    files_skipped=0,
                    overrides_applied=0,
                    conflict_details=(exc.conflict,),
                ),
                aborted=True,
                abort_detail=str(exc),
                abort_conflict=exc.conflict,
            )

        if result.skipped:
            files_skipped += 1
            if result.conflict is not None:
                conflict_details.append(result.conflict)
            continue

        if result.override and result.conflict is not None:
            overrides_applied += 1
            conflict_details.append(result.conflict)

        if result.conflict is not None and not result.skipped and not result.override:
            # Should not occur — defensive guard
            pass

        accepted_meta.append(meta)

    conflicts_detected = len(conflict_details)

    # ── Build MoveActions for accepted files ─────────────────────────────────
    actions_list: list[MoveAction] = []
    for meta in accepted_meta:
        dest_key = resolve_destination(meta.path.name, ruleset, meta.size)
        if dest_map is not None:
            dst_dir = resolve_destination_path(
                dest_key,
                meta.path.name,
                meta.size,
                meta.mtime_ns,
                dest_map,
            )
        else:
            dst_dir = scan_root / dest_key

        actions_list.append(MoveAction(
            src=meta.path,
            dst_dir=dst_dir,
            dst_filename=None,
        ))

    sorted_actions = tuple(sorted(actions_list, key=lambda a: a.src.name))

    # Sort conflict_details by filename for deterministic report ordering
    sorted_conflicts = tuple(
        sorted(conflict_details, key=lambda c: c.filename)
    )

    from policy import PolicyMetrics
    return PolicyPlanResult(
        actions=sorted_actions,
        metrics=PolicyMetrics(
            mode=policy_mode,
            conflicts_detected=conflicts_detected,
            files_skipped=files_skipped,
            overrides_applied=overrides_applied,
            conflict_details=sorted_conflicts,
        ),
    )
