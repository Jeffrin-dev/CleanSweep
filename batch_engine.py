"""
CleanSweep batch_engine.py — v2.4.0 Batch Execution Engine.

Owns the complete action lifecycle:
  VALIDATE → PREPARE → EXECUTE → FINALIZE

Architectural contracts (permanent):
  - Accepts immutable action tuple from planner.plan_actions()
  - Runs full validation pass before any filesystem writes
  - Continue-on-failure: all actions attempted; failures logged and reported
  - Contains ZERO rule logic (no RuleSet evaluation here)
  - Execution order is deterministic (actions sorted by src.name in planner)
  - Dry-run follows identical code path — skips filesystem mutations only
  - All filesystem writes routed through organizer.execute()

Phases (sequential, non-skippable):
  VALIDATE  — read-only filesystem checks; abort if any fail
  PREPARE   — create destination directories (idempotent mkdir)
  EXECUTE   — call organizer.execute() per action; continue on failure, log all errors
  FINALIZE  — assemble BatchReport; record duration

Conflict policy handling (VALIDATE phase):
  rename (default) — no pre-check; FileOperationManager handles suffix counter
  skip             — existing destination file → action excluded, result="skipped"
  error            — existing destination file → entire batch aborted
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import logger
from destination_map import DestinationMap, DestinationMapError
from planner import FileMetadata, MoveAction, plan_actions, plan_with_policy
from rules import RuleSet, DEFAULT_RULESET


# ---------------------------------------------------------------------------
# Phase constants
# ---------------------------------------------------------------------------

class BatchPhase:
    VALIDATE = "VALIDATE"
    PREPARE  = "PREPARE"
    EXECUTE  = "EXECUTE"
    FINALIZE = "FINALIZE"


# ---------------------------------------------------------------------------
# Validation types
# ---------------------------------------------------------------------------

@dataclass
class ValidationFailure:
    """A single pre-execution validation failure."""
    action_index: int
    src:          str
    reason:       str   # machine-readable reason code
    detail:       str = ""  # human-readable supplementary message


@dataclass
class _ValidateResult:
    """Internal: output of the validation pass."""
    failures:       list[ValidationFailure]
    skipped_indices: set[int]        # action indices excluded by "skip" policy
    skip_details:   dict[int, str]   # index → conflict detail message


# ---------------------------------------------------------------------------
# Batch report
# ---------------------------------------------------------------------------

@dataclass
class BatchReport:
    """
    Structured execution report. Deterministic. Reproducible across runs.

    Fields:
      total_planned:        count of actions planned by planner
      total_executed:       count of actions that succeeded (or dry-run simulated)
      fail_index:           action index of first failure; None if all succeeded
      conflict_resolutions: count of FOM collision renames applied
      skipped:              count of actions skipped (conflict_policy="skip")
      duration_seconds:     wall-clock time from run() entry to return
      phase_reached:        last phase entered (useful for diagnostics)
      dry_run:              True if this was a dry-run
      results:              per-action result dicts (file, destination, status, ...)
      validation_failures:  list of ValidationFailure (empty on clean run)
      total_bytes_moved:    aggregate bytes moved/simulated across all executed actions
      destinations_created: count of destination directories created during PREPARE
    """
    total_planned:        int
    total_executed:       int
    fail_index:           int | None
    conflict_resolutions: int
    skipped:              int
    duration_seconds:     float
    phase_reached:        str
    dry_run:              bool
    results:              list[dict]              = field(default_factory=list)
    validation_failures:  list[ValidationFailure] = field(default_factory=list)
    total_bytes_moved:    int                     = 0
    destinations_created: int                     = 0
    policy_metrics:       "object | None"         = None  # PolicyMetrics | None


# ---------------------------------------------------------------------------
# Metadata collection (I/O — lives here, not in planner)
# ---------------------------------------------------------------------------

def collect_file_metadata(paths: list[Path]) -> tuple[FileMetadata, ...]:
    """
    Read size + mtime_ns for each path via stat().

    OSError on any individual file → size=0, mtime_ns=None (never raises).
    Returns tuple sorted by filename for deterministic planning input.
    """
    result: list[FileMetadata] = []
    for p in sorted(paths, key=lambda x: x.name):
        try:
            st = p.stat()
            result.append(FileMetadata(path=p, size=st.st_size, mtime_ns=st.st_mtime_ns))
        except OSError:
            result.append(FileMetadata(path=p, size=0, mtime_ns=None))
    return tuple(result)


# ---------------------------------------------------------------------------
# BatchEngine
# ---------------------------------------------------------------------------

class BatchEngine:
    """
    Controlled execution engine for file move operations.

    Owns the full lifecycle: VALIDATE → PREPARE → EXECUTE → FINALIZE.
    Contains zero rule logic.
    All filesystem writes routed through organizer.execute().

    Continue-on-failure policy: if action N fails during EXECUTE, the engine
    logs the error and continues with the remaining actions. All failures are
    collected and reported in BatchReport. fail_index records the first failure.
    Completed moves are preserved regardless of subsequent failures.
    """

    def __init__(self, scan_root: Path) -> None:
        self._scan_root = scan_root.resolve()

    # -----------------------------------------------------------------------
    # Public: run from pre-planned actions
    # -----------------------------------------------------------------------

    def run(
        self,
        actions:         tuple[MoveAction, ...],
        dry_run:         bool,
        conflict_policy: str = "rename",
    ) -> BatchReport:
        """
        Execute a pre-planned action list.

        Phases: VALIDATE → PREPARE → EXECUTE → FINALIZE.
        Continues on per-file failure; all actions attempted. First failure
        index recorded in BatchReport.fail_index.
        Dry-run follows identical code path; skips filesystem writes only.

        Args:
          actions:         immutable action tuple from planner.plan_actions()
          dry_run:         if True, simulate all operations without writes
          conflict_policy: "rename" | "skip" | "error" (from dest_map or default)

        Returns:
          BatchReport with full execution summary.
        """
        start         = time.monotonic()
        phase_reached = BatchPhase.VALIDATE

        # ── VALIDATE ─────────────────────────────────────────────────────────
        vr = self._validate(actions, dry_run, conflict_policy)

        if vr.failures:
            duration = time.monotonic() - start
            results = [
                {
                    "file":        Path(f.src).name,
                    "destination": str(Path(f.src).parent),
                    "status":      "validation_failed",
                    "error":       f.reason,
                    "detail":      f.detail,
                }
                for f in vr.failures
            ]
            return BatchReport(
                total_planned        = len(actions),
                total_executed       = 0,
                fail_index           = vr.failures[0].action_index,
                conflict_resolutions = 0,
                skipped              = len(vr.skipped_indices),
                duration_seconds     = duration,
                phase_reached        = BatchPhase.VALIDATE,
                dry_run              = dry_run,
                results              = results,
                validation_failures  = list(vr.failures),
            )

        # Build skipped results upfront
        skipped_results: list[dict] = []
        for idx in sorted(vr.skipped_indices):
            action = actions[idx]
            skipped_results.append({
                "file":        action.src.name,
                "destination": str(action.dst_dir),
                "status":      "skipped",
                "reason":      vr.skip_details.get(idx, "conflict_skip"),
            })

        # Filter actions: exclude skipped
        active_actions = tuple(
            a for i, a in enumerate(actions)
            if i not in vr.skipped_indices
        )

        # ── PREPARE ──────────────────────────────────────────────────────────
        phase_reached = BatchPhase.PREPARE
        dirs_created  = 0
        if not dry_run:
            prep_error, dirs_created = self._prepare(active_actions)
            if prep_error:
                duration = time.monotonic() - start
                return BatchReport(
                    total_planned        = len(actions),
                    total_executed       = 0,
                    fail_index           = 0,
                    conflict_resolutions = 0,
                    skipped              = len(vr.skipped_indices),
                    duration_seconds     = duration,
                    phase_reached        = BatchPhase.PREPARE,
                    dry_run              = dry_run,
                    results              = skipped_results + [{
                        "file":        "__prepare__",
                        "destination": "",
                        "status":      "failed",
                        "error":       prep_error,
                    }],
                    total_bytes_moved    = 0,
                    destinations_created = dirs_created,
                )

        # ── EXECUTE ──────────────────────────────────────────────────────────
        phase_reached = BatchPhase.EXECUTE

        # Defer import to avoid circular import at module load time
        from organizer import execute as organizer_execute  # noqa: PLC0415

        move_results:        list[dict] = []
        executed             = 0
        fail_index: int | None = None
        conflict_resolutions = 0
        total_bytes_moved    = 0

        # completed_moves: list of (actual_dst_path, original_src_path) for rollback.
        # Populated only when not dry_run; rollback reverses all completed moves on failure.
        completed_moves: list[tuple[Path, Path]] = []

        for i, action in enumerate(active_actions):
            # Capture file size before execution: for "moved", src no longer exists after.
            try:
                file_size = os.path.getsize(action.src)
            except OSError:
                file_size = 0

            result = organizer_execute(action, dry_run=dry_run)
            move_results.append(result)

            status = result.get("status", "failed")
            if status in ("moved", "dry_run"):
                executed += 1
                total_bytes_moved += file_size
                if result.get("collision"):
                    conflict_resolutions += 1
                # Track the actual destination for rollback
                if not dry_run and "actual_dst" in result:
                    completed_moves.append((Path(result["actual_dst"]), action.src))
            else:
                if fail_index is None:
                    fail_index = i
                logger.log_error(
                    f"[BatchEngine] Action {i}/{len(active_actions)} failed: "
                    f"{action.src.name} → {action.dst_dir} "
                    f"— {result.get('error', 'unknown error')}"
                )

        # ── ROLLBACK (on any failure, reverse all completed moves) ────────────
        if fail_index is not None and not dry_run and completed_moves:
            logger.log_info(
                f"[BatchEngine] Partial failure at action {fail_index}. "
                f"Rolling back {len(completed_moves)} completed move(s)."
            )
            for actual_dst, original_src in reversed(completed_moves):
                try:
                    actual_dst.rename(original_src)
                    logger.log_info(f"Rolled back: {actual_dst} → {original_src}")
                except OSError as rb_exc:
                    logger.log_error(
                        f"[BatchEngine] Rollback failed for {actual_dst}: {rb_exc}"
                    )
            # Reset counters — moves were reversed
            executed = 0
            total_bytes_moved = 0
            conflict_resolutions = 0

        # ── FINALIZE ─────────────────────────────────────────────────────────
        phase_reached = BatchPhase.FINALIZE
        duration      = time.monotonic() - start

        all_results = skipped_results + move_results

        return BatchReport(
            total_planned        = len(actions),
            total_executed       = executed,
            fail_index           = fail_index,
            conflict_resolutions = conflict_resolutions,
            skipped              = len(vr.skipped_indices),
            duration_seconds     = duration,
            phase_reached        = BatchPhase.FINALIZE,
            dry_run              = dry_run,
            results              = all_results,
            total_bytes_moved    = total_bytes_moved,
            destinations_created = dirs_created,
        )

    # -----------------------------------------------------------------------
    # Public: run from file list (convenience entry point)
    # -----------------------------------------------------------------------

    def run_from_files(
        self,
        files:       list[Path],
        ruleset:     RuleSet,
        dest_map:    DestinationMap | None,
        dry_run:     bool,
        policy_mode: str = "safe",
    ) -> BatchReport:
        """
        Full pipeline: metadata collection → policy-aware planning → run().

        I/O for stat() calls lives here, not in planner.

        Args:
          files:       list of source files (need not be pre-sorted)
          ruleset:     active rule set
          dest_map:    optional destination map
          dry_run:     if True, simulate without writes
          policy_mode: conflict behaviour — "strict" | "safe" | "warn" (default: "safe")

        Returns:
          BatchReport with full execution summary and policy_metrics.
          On strict-mode abort: BatchReport with fail_index=0 and a
          policy-conflict error in results.
        """
        if not files:
            from policy import PolicyMetrics
            return BatchReport(
                total_planned        = 0,
                total_executed       = 0,
                fail_index           = None,
                conflict_resolutions = 0,
                skipped              = 0,
                duration_seconds     = 0.0,
                phase_reached        = BatchPhase.FINALIZE,
                dry_run              = dry_run,
                results              = [],
                policy_metrics       = PolicyMetrics(
                    mode=policy_mode,
                    conflicts_detected=0,
                    files_skipped=0,
                    overrides_applied=0,
                    conflict_details=(),
                ),
            )

        # Collect metadata (I/O — sorted by filename inside collect_file_metadata)
        metadata = collect_file_metadata(files)

        # Policy-aware planning (pure — zero I/O).
        # DestinationMapError propagates to caller.
        plan_result = plan_with_policy(
            metadata, ruleset, dest_map, self._scan_root, policy_mode
        )

        # Strict-mode abort: return failure report without executing anything.
        if plan_result.aborted:
            import time
            ts = time.monotonic()
            return BatchReport(
                total_planned        = 0,
                total_executed       = 0,
                fail_index           = 0,
                conflict_resolutions = 0,
                skipped              = 0,
                duration_seconds     = 0.0,
                phase_reached        = BatchPhase.VALIDATE,
                dry_run              = dry_run,
                results              = [{
                    "file":        plan_result.abort_conflict.filename
                                   if plan_result.abort_conflict else "",
                    "destination": "",
                    "status":      "validation_failed",
                    "error":       "policy_conflict_strict",
                    "detail":      plan_result.abort_detail,
                }],
                validation_failures  = [ValidationFailure(
                    action_index = 0,
                    src          = plan_result.abort_conflict.filename
                                   if plan_result.abort_conflict else "",
                    reason       = "policy_conflict_strict",
                    detail       = plan_result.abort_detail,
                )],
                policy_metrics       = plan_result.metrics,
            )

        conflict_policy = dest_map.conflict_policy if dest_map is not None else "rename"
        report = self.run(plan_result.actions, dry_run=dry_run, conflict_policy=conflict_policy)

        # Attach policy metrics and merge policy-skipped count.
        # Use object.__setattr__ since BatchReport is a dataclass but NOT frozen.
        report.policy_metrics = plan_result.metrics
        report.skipped        = report.skipped + plan_result.metrics.files_skipped

        return report

    # -----------------------------------------------------------------------
    # VALIDATE phase
    # -----------------------------------------------------------------------

    def _validate(
        self,
        actions:         tuple[MoveAction, ...],
        dry_run:         bool,
        conflict_policy: str,
    ) -> _ValidateResult:
        """
        Pre-execution validation pass. Read-only filesystem access only.

        Checks performed (in order):
          1. Source file exists
          2. Source is a regular file (not a directory)
          3. Source is readable (os.access R_OK)
          4. No circular move (src.parent == dst_dir and same filename)
          5. Conflict policy enforcement against existing destination files
          6. Cross-action filename collision detection (same dst_dir + filename)
          7. Destination parent writable (skipped on dry_run)

        Policy outcomes:
          rename — existing dst file → no issue (FOM handles renaming)
          skip   — existing dst file → action excluded (added to skipped_indices)
          error  — existing dst file → ValidationFailure (batch aborted)

        Returns:
          _ValidateResult with failures list and skipped_indices set.
          Empty failures = batch may proceed.
        """
        failures:        list[ValidationFailure] = []
        skipped_indices: set[int]               = set()
        skip_details:    dict[int, str]          = {}

        # Track (dst_dir, effective_filename) → first_action_index for collision detection
        seen_destinations: dict[tuple[str, str], int] = {}

        for i, action in enumerate(actions):
            effective_name = action.dst_filename or action.src.name

            # 1. Source must exist
            if not action.src.exists():
                failures.append(ValidationFailure(
                    i, str(action.src), "source_not_found",
                    f"Source file does not exist: {action.src}",
                ))
                continue

            # 2. Source must be a regular file
            if not action.src.is_file():
                failures.append(ValidationFailure(
                    i, str(action.src), "source_not_a_file",
                    f"Source is not a regular file: {action.src}",
                ))
                continue

            # 3. Source must be readable
            if not os.access(action.src, os.R_OK):
                failures.append(ValidationFailure(
                    i, str(action.src), "source_not_readable",
                    f"Source file is not readable: {action.src}",
                ))
                continue

            # 4. Circular move check
            if (action.src.parent == action.dst_dir
                    and effective_name == action.src.name):
                failures.append(ValidationFailure(
                    i, str(action.src), "circular_move",
                    f"Source and destination are identical: {action.src}",
                ))
                continue

            # 5. Conflict policy: check if destination filename already exists on disk
            dst_file = action.dst_dir / effective_name
            if dst_file.exists():
                if conflict_policy == "skip":
                    skipped_indices.add(i)
                    skip_details[i] = f"conflict_skip: {dst_file}"
                    continue
                elif conflict_policy == "error":
                    failures.append(ValidationFailure(
                        i, str(action.src), "conflict_policy_error",
                        (
                            f"conflict_error: {action.src.name!r} already exists in "
                            f"{str(action.dst_dir)!r}. Aborting (conflict_policy='error')."
                        ),
                    ))
                    # For "error" policy: first collision aborts entire batch
                    break
                # "rename" policy: no validation issue; FOM handles the suffix counter

            # 6. Cross-action filename collision (two actions mapping to same dst).
            # Only flagged when conflict_policy != "rename": with "rename" policy,
            # FileOperationManager handles suffix counters at execution time.
            if conflict_policy != "rename":
                collision_key = (str(action.dst_dir), effective_name)
                if collision_key in seen_destinations:
                    prev_idx = seen_destinations[collision_key]
                    failures.append(ValidationFailure(
                        i, str(action.src),
                        "cross_action_collision",
                        (
                            f"Action {i} and action {prev_idx} both target "
                            f"{action.dst_dir / effective_name}"
                        ),
                    ))
                else:
                    seen_destinations[collision_key] = i

        # 7. Destination parent writable (read-only os.access check — runs on both
        #    real and dry-run paths per the "same engine path" contract).
        if not failures:
            checked_dirs: set[str] = set()
            for i, action in enumerate(actions):
                if i in skipped_indices:
                    continue
                dst_key = str(action.dst_dir)
                if dst_key in checked_dirs:
                    continue
                checked_dirs.add(dst_key)

                # Walk up to the highest existing ancestor
                candidate = action.dst_dir
                while not candidate.exists() and candidate != candidate.parent:
                    candidate = candidate.parent

                if candidate.exists() and not os.access(candidate, os.W_OK):
                    failures.append(ValidationFailure(
                        i, str(action.src),
                        "destination_not_writable",
                        f"Destination not writable: {candidate}",
                    ))

        return _ValidateResult(
            failures        = failures,
            skipped_indices = skipped_indices,
            skip_details    = skip_details,
        )

    # -----------------------------------------------------------------------
    # PREPARE phase
    # -----------------------------------------------------------------------

    def _prepare(self, active_actions: tuple[MoveAction, ...]) -> tuple[str | None, int]:
        """
        Create all destination directories before any file moves begin.

        Idempotent (mkdir parents=True, exist_ok=True).
        Sorted for deterministic directory creation order.

        Returns:
          (None, dirs_created) on success.
          (error_string, dirs_created_so_far) on first OSError (batch aborted by caller).
        """
        dirs_needed: set[Path] = {a.dst_dir for a in active_actions}
        dirs_created = 0

        for dst_dir in sorted(dirs_needed, key=str):
            already_exists = dst_dir.exists()
            try:
                dst_dir.mkdir(parents=True, exist_ok=True)
                if not already_exists:
                    dirs_created += 1
            except OSError as exc:
                return (f"Cannot create destination directory {dst_dir!r}: {exc}", dirs_created)

        return (None, dirs_created)
