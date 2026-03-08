"""
CleanSweep organizer.py — v2.4.0 Organizer Layer.

v2.4.0 upgrade — Batch Execution Engine integration:
  execute() is the new primary filesystem API.
  organize() retained as a backward-compatible shim — routes through
  planner + BatchEngine internally. All planning logic removed from this module.

v2.3.0 (backward compat): dest_map support preserved via organize() shim.
v2.2.0 (backward compat): file sizes read before rule evaluation.
v2.0.0 (backward compat): optional ruleset parameter.
v1.8.0 (backward compat): atomic moves, no direct shutil.move.

Architectural contract (v2.4.0, permanent):

  PRIMARY API:
    execute(action: MoveAction, dry_run: bool) → dict
      Performs a single atomic file move.
      All filesystem writes happen here — nowhere else in the organizer.
      No validation logic. No planning logic. No rule logic.
      Called exclusively by BatchEngine.

  COMPATIBILITY SHIM:
    organize(folder, dry_run, ...) → dict
      Retained for backward compatibility with existing test suite.
      Internally routes through planner.plan_actions() + BatchEngine.run().
      No direct business logic — all delegated to planner/batch_engine.

What MUST NOT happen in this module (permanent):
  ✗  Rule evaluation (delegated to rules.resolve_destination)
  ✗  Destination key resolution (delegated to destination_map)
  ✗  Validation logic (delegated to BatchEngine._validate)
  ✗  Planning logic (delegated to planner.plan_actions)
  ✗  Printing or logging (delegated to report.py / logger.py)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from destination_map import DestinationMap, DestinationMapError
from file_operation_manager import FileOperationManager
from planner import MoveAction
from rules import RuleSet, DEFAULT_RULESET

if TYPE_CHECKING:
    from action_controller import ActionController


# ---------------------------------------------------------------------------
# Deprecated v1.x constants — kept for external consumers, not used internally
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Images":    {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"},
    "Documents": {".pdf", ".docx", ".txt", ".xlsx", ".csv", ".md"},
    "Videos":    {".mp4", ".mkv", ".avi", ".mov", ".wmv"},
    "Audio":     {".mp3", ".wav", ".flac", ".aac", ".ogg"},
    "Archives":  {".zip", ".tar", ".gz", ".rar", ".7z"},
    "Code":      {".py", ".js", ".ts", ".html", ".css", ".json", ".yaml"},
}


def get_category(
    file:      Path,
    ruleset:   RuleSet | None = None,
    file_size: int = 0,
) -> str:
    """Return the destination category name for file (v2.2 API, backward-compatible)."""
    from rules import resolve_destination
    rs = ruleset if ruleset is not None else DEFAULT_RULESET
    return resolve_destination(file.name, rs, file_size)


# ---------------------------------------------------------------------------
# Primary API — execute()
# ---------------------------------------------------------------------------

def execute(
    action:  MoveAction,
    dry_run: bool,
) -> dict:
    """
    Execute a single MoveAction atomically.

    This is the only filesystem write API in organizer.py (v2.4.0+).
    Called exclusively by BatchEngine during EXECUTE phase.

    Uses FileOperationManager.atomic_move() — temp + rename strategy.
    Destination directory must already exist (created by BatchEngine._prepare).

    Args:
      action:  immutable MoveAction from planner.plan_actions()
      dry_run: if True, simulate without writing to disk

    Returns:
      dict with keys:
        file        — source filename
        destination — destination directory (str)
        status      — "moved" | "dry_run" | "failed"
        collision   — True if FOM applied a rename suffix (status="moved" only)
        error       — error string (status="failed" only)
    """
    fom    = FileOperationManager(dry_run=dry_run)
    result = fom.atomic_move(action.src, action.dst_dir, action.dst_filename)

    if result.status == "dry_run":
        return {
            "file":        action.src.name,
            "destination": str(action.dst_dir),
            "status":      "dry_run",
            "collision":   result.collision,
        }

    if result.status == "moved":
        return {
            "file":        action.src.name,
            "destination": str(action.dst_dir),
            "actual_dst":  result.dst,   # actual final path (may differ on collision rename)
            "status":      "moved",
            "collision":   result.collision,
        }

    # "failed" — src is always untouched
    return {
        "file":        action.src.name,
        "destination": str(action.dst_dir),
        "status":      "failed",
        "collision":   False,
        "error":       result.error,
    }


# ---------------------------------------------------------------------------
# Backward-compatible shim — safe_move()
# ---------------------------------------------------------------------------

def safe_move(
    file:            Path,
    destination_dir: Path,
    dry_run:         bool,
    controller:      "ActionController | None" = None,
) -> dict:
    """
    Move one file to destination_dir (v2.3 compat shim).

    Retained for backward compatibility. New code should use execute().
    """
    if controller is None:
        from action_controller import ActionController
        controller = ActionController(dry_run=dry_run, scan_root=file.parent)
    result = controller.move(src=file, dst_dir=destination_dir, dry_run=dry_run)
    if result.status == "dry_run":
        return {"file": file.name, "destination": str(destination_dir), "status": "dry_run"}
    elif result.status == "success":
        return {"file": file.name, "destination": str(destination_dir), "status": "moved"}
    else:
        return {
            "file":        file.name,
            "destination": str(destination_dir),
            "status":      "failed",
            "error":       result.error,
        }


# ---------------------------------------------------------------------------
# Backward-compatible shim — organize()
# ---------------------------------------------------------------------------

def organize(
    folder:              Path,
    dry_run:             bool = False,
    controller:          "ActionController | None" = None,
    ruleset:             RuleSet | None = None,
    rollback_on_failure: bool = True,
    dest_map:            DestinationMap | None = None,
) -> dict:
    """
    Categorize and move files in folder into subdirectories.

    v2.4.0: backward-compatible shim. Routes through planner + BatchEngine.
    All planning, validation, and execution is delegated — no logic here.

    v2.3.0 API preserved: dest_map parameter supported.
    v2.2.0 API preserved: ruleset parameter supported.
    v2.0.0 API preserved: dry_run parameter supported.
    v1.8.0 API preserved: atomic moves guaranteed via FOM.

    Returns dict with keys:
      dry_run, total, results, ruleset_name, dest_map_active
      rollback_triggered (always False in v2.4 — hard-abort policy)
      rolled_back (always [])
      temps_cleaned (always 0)
      error (only if batch aborted — conflict_policy='error' or validation fail)
    """
    from batch_engine import BatchEngine

    rs           = ruleset if ruleset is not None else DEFAULT_RULESET
    ruleset_name = "default" if ruleset is None else "custom"

    # ── File discovery (sorted, immediate children only) ─────────────────────
    try:
        files = sorted(
            [f for f in folder.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
    except (PermissionError, FileNotFoundError, OSError) as exc:
        return {
            "dry_run":         dry_run,
            "total":           0,
            "results":         [],
            "ruleset_name":    ruleset_name,
            "dest_map_active": dest_map is not None,
            "error":           type(exc).__name__,
        }

    if not files:
        return {
            "dry_run":         dry_run,
            "total":           0,
            "results":         [],
            "ruleset_name":    ruleset_name,
            "dest_map_active": dest_map is not None,
        }

    # ── Run through planner + BatchEngine ────────────────────────────────────
    engine = BatchEngine(scan_root=folder)
    report = engine.run_from_files(
        files    = files,
        ruleset  = rs,
        dest_map = dest_map,
        dry_run  = dry_run,
    )

    # ── Convert BatchReport → legacy dict format ──────────────────────────────
    return _report_to_legacy(report, files, dry_run, ruleset_name, dest_map)


def _report_to_legacy(
    report:      object,   # BatchReport — avoid circular import type at module level
    files:       list[Path],
    dry_run:     bool,
    ruleset_name: str,
    dest_map:    DestinationMap | None,
) -> dict:
    """
    Convert a BatchReport to the legacy organize() dict format.

    This is the ONLY place the format conversion lives.
    Handles all status transformations for backward compatibility.
    """
    results: list[dict] = []

    for r in report.results:
        status = r.get("status", "failed")

        # Normalize statuses for legacy consumers
        if status == "validation_failed":
            # Check if this is a conflict_policy="error" abort
            detail = r.get("detail", r.get("error", ""))
            if "conflict_error" in detail or r.get("error") == "conflict_policy_error":
                # Return early with legacy error format (matching v2.3 behavior)
                return {
                    "dry_run":         dry_run,
                    "total":           len(files),
                    "results":         [],
                    "ruleset_name":    ruleset_name,
                    "dest_map_active": dest_map is not None,
                    "error":           detail,
                }
            # Other validation failures map to "failed"
            results.append({
                "file":        r.get("file", ""),
                "destination": r.get("destination", ""),
                "status":      "failed",
                "error":       r.get("error", ""),
            })

        elif status == "skipped":
            results.append({
                "file":        r.get("file", ""),
                "destination": r.get("destination", ""),
                "status":      "skipped",
                "reason":      "conflict_skip",
            })

        elif status in ("moved", "dry_run", "failed"):
            entry: dict = {
                "file":        r.get("file", ""),
                "destination": r.get("destination", ""),
                "status":      status,
            }
            if status == "failed" and "error" in r:
                entry["error"] = r["error"]
            results.append(entry)

    # Stable sort by filename — matches legacy behavior
    results_sorted = sorted(results, key=lambda r: r.get("file", ""))

    base: dict = {
        "dry_run":            dry_run,
        "total":              len(files),
        "results":            results_sorted,
        "ruleset_name":       ruleset_name,
        "dest_map_active":    dest_map is not None,
        "rollback_triggered": False,
        "rolled_back":        [],
        "temps_cleaned":      0,
    }

    # Propagate top-level error from BatchReport if validation failed globally
    if (report.fail_index is not None
            and report.phase_reached == "VALIDATE"
            and report.validation_failures):
        vf = report.validation_failures[0]
        if "conflict_error" in vf.detail or vf.reason == "conflict_policy_error":
            base["error"] = vf.detail
        elif vf.reason == "dest_map_key_missing":
            base["error"] = vf.detail

    return base
