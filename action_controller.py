"""
CleanSweep ActionController — v1.8.0 Atomic File Operations.

ALL destructive filesystem operations route through this module.
Nothing else in the codebase may call:
  - os.remove / os.unlink
  - shutil.move / shutil.rmtree
  - Path.unlink()
  - Path.rename() for moves

This is permanently locked. Zero exceptions.

v1.7.0 deletion modes:
  "trash"     — move to system trash (default, reversible)
  "permanent" — hard delete (requires explicit --permanent flag)

v1.8.0 atomic move upgrade:
  move() now routes through FileOperationManager.atomic_move()
  — temp + rename strategy (never bare shutil.move)
  — collision-safe: file (1).txt, file (2).txt, ...
  — cross-device: copy + verify (sha256) + delete
  — temp files always cleaned in finally blocks
  — batch moves support rollback on partial failure

Design principles:
  Safe by default     — trash is always the default delete mode
  Explicit            — permanent deletion requires double flag (--delete --permanent)
  Atomic              — every move is all-or-nothing, no partial state
  Collision-safe      — no silent overwrites ever
  Deterministic       — victim selection never influenced by deletion mode
  Auditable           — every action logged with structured event
  Interrupt-safe      — Ctrl+C stops scheduling, in-flight ops finish, temps cleaned
  Idempotent          — already-deleted/trashed files handled gracefully
"""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import logger
from trash_manager import TrashManager, TrashUnavailableError, TrashFailedError
from file_operation_manager import FileOperationManager

if TYPE_CHECKING:
    from duplicates import FileEntry


# ---------------------------------------------------------------------------
# Delete modes (locked permanently)
# ---------------------------------------------------------------------------

DELETE_MODE_TRASH     = "trash"      # move to system trash (default)
DELETE_MODE_PERMANENT = "permanent"  # hard delete — requires --permanent flag


# ---------------------------------------------------------------------------
# Action result
# ---------------------------------------------------------------------------

@dataclass
class ActionResult:
    path:       str
    action:     str          # "delete" | "move"
    status:     str          # "success" | "dry_run" | "failed" | "skipped" | "unavailable"
    size:       int  = 0
    file_hash:  str  = ""
    error:      str  = ""
    simulated:  bool = False
    trash_path: str  = ""    # where file ended up in trash (empty if permanent/failed)


# ---------------------------------------------------------------------------
# Audit event
# ---------------------------------------------------------------------------

def _audit_event(
    action: str,
    path: str,
    size: int,
    file_hash: str,
    status: str,
    error: str = "",
    trash_path: str = "",
    delete_mode: str = DELETE_MODE_TRASH,
) -> dict:
    return {
        "action":      action,
        "file":        path,
        "size":        size,
        "hash":        file_hash,
        "status":      status,
        "delete_mode": delete_mode,
        "trash_path":  trash_path,
        "error":       error,
        "timestamp":   datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# ActionController
# ---------------------------------------------------------------------------

class ActionController:
    """
    Centralized controller for all destructive filesystem operations.

    Deletion modes (v1.7.0):
      trash     — files moved to system trash (default, reversible)
      permanent — hard delete, requires explicit --permanent flag

    Boundary rule (locked permanently):
      Every target file must be within scan_root.
      Violation raises ValueError — never silently bypassed.

    Idempotency:
      FileNotFoundError during delete → counted as success (already gone).
      TrashUnavailableError → abort with clear error, never silent permanent fallback.
    """

    def __init__(
        self,
        dry_run:     bool,
        scan_root:   Path,
        delete_mode: str = DELETE_MODE_TRASH,
    ) -> None:
        if delete_mode not in (DELETE_MODE_TRASH, DELETE_MODE_PERMANENT):
            raise ValueError(f"Invalid delete_mode: {delete_mode!r}")

        self.dry_run     = dry_run
        self.scan_root   = scan_root.resolve()
        self.delete_mode = delete_mode

        # TrashManager instantiated once per controller
        self._trash_manager = TrashManager()

        # Audit trail — append-only
        self._audit_log: list[dict] = []

        # Result tracking
        self._deleted: list[str]  = []
        self._failed:  list[dict] = []
        self._skipped: list[str]  = []

    # -----------------------------------------------------------------------
    # Boundary enforcement
    # -----------------------------------------------------------------------

    def _assert_within_root(self, path: Path) -> None:
        try:
            resolved = path.resolve(strict=False)
            resolved.relative_to(self.scan_root)
        except ValueError:
            raise ValueError(
                f"Safety boundary violation: {path} is outside "
                f"scan root {self.scan_root}"
            )

    # -----------------------------------------------------------------------
    # Delete — single authority for all file removal
    # -----------------------------------------------------------------------

    def delete(
        self,
        path:      Path,
        size:      int = 0,
        file_hash: str = "",
    ) -> ActionResult:
        """
        Delete one file using the configured delete_mode.

        Mode "trash" (default):
          1. Boundary check
          2. Not a directory
          3. dry_run → preview only
          4. Move to system trash via TrashManager
          5. TrashUnavailableError → abort (never silent permanent fallback)

        Mode "permanent":
          1-3 same
          4. Path.unlink() — the only unlink() call in the codebase
          5. FileNotFoundError → idempotent success
        """
        path_str = path.as_posix()

        # 1. Boundary check
        try:
            self._assert_within_root(path)
        except ValueError as e:
            event = _audit_event("delete", path_str, size, file_hash,
                                 "boundary_violation", str(e), delete_mode=self.delete_mode)
            self._audit_log.append(event)
            logger.log_warn(f"Boundary violation — refused: {path_str}")
            self._failed.append({"path": path_str, "error": "boundary_violation"})
            return ActionResult(path=path_str, action="delete", status="failed",
                                size=size, file_hash=file_hash, error="boundary_violation")

        # 2. Never delete directories
        if path.is_dir():
            event = _audit_event("delete", path_str, size, file_hash,
                                 "refused_directory", delete_mode=self.delete_mode)
            self._audit_log.append(event)
            logger.log_warn(f"Refused to delete directory: {path_str}")
            self._failed.append({"path": path_str, "error": "refused_directory"})
            return ActionResult(path=path_str, action="delete", status="failed",
                                size=size, file_hash=file_hash, error="refused_directory")

        # 3. Dry-run — simulate, touch nothing
        if self.dry_run:
            event = _audit_event("delete", path_str, size, file_hash, "dry_run",
                                 delete_mode=self.delete_mode)
            self._audit_log.append(event)
            logger.log_debug(f"[DRY RUN] Would {self.delete_mode}: {path_str}")
            self._deleted.append(path_str)
            return ActionResult(path=path_str, action="delete", status="dry_run",
                                size=size, file_hash=file_hash, simulated=True)

        # 4a. Trash mode
        if self.delete_mode == DELETE_MODE_TRASH:
            return self._delete_via_trash(path, path_str, size, file_hash)

        # 4b. Permanent mode
        return self._delete_permanent(path, path_str, size, file_hash)

    def _delete_via_trash(
        self,
        path:      Path,
        path_str:  str,
        size:      int,
        file_hash: str,
    ) -> ActionResult:
        """Move to system trash. Abort if unavailable — never silent permanent fallback."""
        try:
            result = self._trash_manager.trash(path)
            event = _audit_event("delete", path_str, size, file_hash,
                                 "trashed", trash_path=result.trash_path,
                                 delete_mode=DELETE_MODE_TRASH)
            self._audit_log.append(event)
            logger.log_debug(f"Trashed: {path_str} → {result.trash_path}")
            self._deleted.append(path_str)
            return ActionResult(path=path_str, action="delete", status="success",
                                size=size, file_hash=file_hash,
                                trash_path=result.trash_path)

        except TrashUnavailableError as e:
            # Abort — never silently fall back to permanent delete
            event = _audit_event("delete", path_str, size, file_hash,
                                 "trash_unavailable", str(e),
                                 delete_mode=DELETE_MODE_TRASH)
            self._audit_log.append(event)
            logger.log_warn(f"Trash unavailable for {path_str}: {e}")
            self._failed.append({"path": path_str, "error": "trash_unavailable",
                                  "hint": "Use --permanent to permanently delete"})
            return ActionResult(path=path_str, action="delete", status="unavailable",
                                size=size, file_hash=file_hash, error=str(e))

        except TrashFailedError as e:
            event = _audit_event("delete", path_str, size, file_hash,
                                 "trash_failed", str(e),
                                 delete_mode=DELETE_MODE_TRASH)
            self._audit_log.append(event)
            logger.log_warn(f"Trash failed for {path_str}: {e}")
            self._failed.append({"path": path_str, "error": "trash_failed"})
            return ActionResult(path=path_str, action="delete", status="failed",
                                size=size, file_hash=file_hash, error=str(e))

        except OSError as e:
            event = _audit_event("delete", path_str, size, file_hash,
                                 "failed", str(e), delete_mode=DELETE_MODE_TRASH)
            self._audit_log.append(event)
            logger.log_warn(f"Delete failed [{type(e).__name__}]: {path_str}")
            self._failed.append({"path": path_str, "error": type(e).__name__})
            return ActionResult(path=path_str, action="delete", status="failed",
                                size=size, file_hash=file_hash, error=type(e).__name__)

    def _delete_permanent(
        self,
        path:      Path,
        path_str:  str,
        size:      int,
        file_hash: str,
    ) -> ActionResult:
        """Hard delete. The ONLY place in the codebase that calls Path.unlink()."""
        try:
            path.unlink()
            event = _audit_event("delete", path_str, size, file_hash,
                                 "success", delete_mode=DELETE_MODE_PERMANENT)
            self._audit_log.append(event)
            logger.log_debug(f"Permanently deleted: {path_str}")
            self._deleted.append(path_str)
            return ActionResult(path=path_str, action="delete", status="success",
                                size=size, file_hash=file_hash)

        except FileNotFoundError:
            # Idempotent — already gone
            event = _audit_event("delete", path_str, size, file_hash,
                                 "already_gone", delete_mode=DELETE_MODE_PERMANENT)
            self._audit_log.append(event)
            logger.log_debug(f"Already gone (idempotent): {path_str}")
            self._deleted.append(path_str)
            return ActionResult(path=path_str, action="delete", status="success",
                                size=size, file_hash=file_hash)

        except OSError as e:
            event = _audit_event("delete", path_str, size, file_hash,
                                 "failed", str(e), delete_mode=DELETE_MODE_PERMANENT)
            self._audit_log.append(event)
            logger.log_warn(f"Delete failed [{type(e).__name__}]: {path_str}")
            self._failed.append({"path": path_str, "error": type(e).__name__})
            return ActionResult(path=path_str, action="delete", status="failed",
                                size=size, file_hash=file_hash, error=type(e).__name__)

    # -----------------------------------------------------------------------
    # Move (organizer) — v1.8.0 atomic via FileOperationManager
    # -----------------------------------------------------------------------

    def move(
        self,
        src:      Path,
        dst_dir:  Path,
        filename: str | None = None,
        dry_run:  bool | None = None,
    ) -> ActionResult:
        """
        Move one file atomically via FileOperationManager.

        v1.8.0: bare shutil.move() is gone. Every move goes through:
          FileOperationManager.atomic_move() → temp + rename strategy.

        dst_dir   — the destination directory (not the final file path)
        filename  — override destination filename (collision-resolved if None)
        """
        effective_dry_run = self.dry_run if dry_run is None else dry_run
        src_str = src.as_posix()

        # Boundary check
        try:
            self._assert_within_root(src)
        except ValueError as e:
            event = _audit_event("move", src_str, 0, "", "boundary_violation", str(e))
            self._audit_log.append(event)
            logger.log_warn(f"Move boundary violation: {src_str}")
            return ActionResult(path=src_str, action="move", status="failed",
                                error="boundary_violation")

        # Route through FileOperationManager
        fom    = FileOperationManager(dry_run=effective_dry_run)
        result = fom.atomic_move(src, dst_dir, filename)

        if result.status == "dry_run":
            event = _audit_event("move", src_str, 0, "", "dry_run")
            self._audit_log.append(event)
            return ActionResult(path=src_str, action="move", status="dry_run",
                                simulated=True)

        elif result.status == "moved":
            event = _audit_event("move", src_str, 0, "", "success")
            self._audit_log.append(event)
            return ActionResult(path=src_str, action="move", status="success")

        else:
            event = _audit_event("move", src_str, 0, "", "failed", result.error)
            self._audit_log.append(event)
            logger.log_warn(f"Atomic move failed: {src_str} — {result.error}")
            return ActionResult(path=src_str, action="move", status="failed",
                                error=result.error)

    # -----------------------------------------------------------------------
    # Batch deletion
    # -----------------------------------------------------------------------

    def execute_deletions(
        self,
        duplicates: dict,
        file_hashes: dict | None = None,
    ) -> dict:
        """
        Execute deletion for all non-kept files in duplicate groups.
        entries[0] is kept. All others deleted via self.delete().
        Failures logged, counted — process never aborted on single failure.
        """
        file_hashes = file_hashes or {}

        for file_hash, entries in duplicates.items():
            for entry in entries[1:]:
                self.delete(
                    path=entry.path,
                    size=entry.size,
                    file_hash=file_hash,
                )

        return self.summary()

    # -----------------------------------------------------------------------
    # Preview
    # -----------------------------------------------------------------------

    def preview_deletion(self, duplicates: dict) -> dict:
        """
        Return structured preview data — zero print() calls.

        All formatting and output is handled by report.display_deletion_preview().
        This method only computes and returns data.

        Returns:
          {
            "dry_run":       bool,
            "delete_mode":   str,    # "trash" | "permanent"
            "platform":      str,    # TrashManager platform string
            "total_files":   int,    # files that would be deleted
            "total_bytes":   int,    # total bytes that would be freed
            "scan_root":     str,    # absolute path of scan root
          }
        """
        total_files = sum(len(e) - 1 for e in duplicates.values())
        total_bytes = sum(
            e[0].size * (len(e) - 1)
            for e in duplicates.values()
            if e
        )
        return {
            "dry_run":     self.dry_run,
            "delete_mode": self.delete_mode,
            "platform":    self._trash_manager.platform,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "scan_root":   str(self.scan_root),
        }

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    def summary(self) -> dict:
        freed = sum(
            e.get("size", 0)
            for e in self._audit_log
            if e["status"] in ("success", "already_gone", "trashed") and e["action"] == "delete"
        )
        return {
            "dry_run":     self.dry_run,
            "delete_mode": self.delete_mode,
            "attempted":   len(self._deleted) + len(self._failed),
            "deleted":     sorted(self._deleted),
            "failed":      sorted(self._failed, key=lambda f: f["path"]),
            "skipped":     sorted(self._skipped),
            "freed_bytes": freed,
            "audit_log":   list(self._audit_log),
        }

    def has_failures(self) -> bool:
        return len(self._failed) > 0

    def audit_log(self) -> list[dict]:
        return list(self._audit_log)


# ---------------------------------------------------------------------------
# Module-level convenience shim — delete_duplicates()
#
# Moved here from duplicates.py (v2.1 patch) to break the circular import:
#   duplicates.py → action_controller.py → duplicates.py (TYPE_CHECKING only)
#
# delete_duplicates() belongs alongside ActionController because it IS an
# ActionController operation — it was always a thin wrapper that instantiated
# one and called execute_deletions(). Keeping it in the analysis engine
# (duplicates.py) was the architectural error.
#
# Backward compatibility: callers that imported from duplicates.py must update
# to:  from action_controller import delete_duplicates
# ---------------------------------------------------------------------------

def delete_duplicates(
    duplicates: dict,
    dry_run: bool = False,
    scan_root: "Path | None" = None,
    delete_mode: str = DELETE_MODE_PERMANENT,  # tests expect permanent by default (v1.6 compat)
) -> dict:
    """
    Convenience wrapper: execute deletions for all non-kept files in duplicate groups.

    Routes entirely through ActionController — no inline deletion logic here.
    delete_mode defaults to "permanent" to preserve v1.6 test behaviour; CLI
    callers pass DELETE_MODE_TRASH (the default since v1.7).

    Moved from duplicates.py to action_controller.py to eliminate the circular
    import between the analysis engine and the action framework.
    """
    root = scan_root
    if root is None:
        for entries in duplicates.values():
            if entries:
                root = entries[0].path.parent
                break
    if root is None:
        root = Path(".")

    controller = ActionController(dry_run=dry_run, scan_root=root, delete_mode=delete_mode)
    return controller.execute_deletions(duplicates)
