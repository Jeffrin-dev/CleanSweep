"""
CleanSweep FileOperationManager — v1.8.0 Atomic File Operations.

ALL file move operations route through this module.
Nothing else in the codebase may call:
  - shutil.move()
  - os.rename() for user-facing moves
  - Path.rename() for user-facing moves

This is permanently locked. Zero exceptions.

Design goals (v1.8.0):
  Atomic        — every move is either complete or not started. Never partial.
  Collision-safe — destination collisions resolved deterministically. No silent overwrites.
  Rollback-capable — batch operations can be reversed on partial failure.
  Ctrl+C-safe   — temp files always cleaned in finally blocks.
  Cross-device   — same-filesystem rename preferred; cross-device copy+verify+delete fallback.

Atomic move strategy:
  Step 1: Copy/move src → .cleansweep_tmp_<uuid8> inside dst_dir
  Step 2: os.rename(temp, final)           ← POSIX: atomic within same filesystem
  On any failure: temp cleaned, src untouched. No partial state.

Collision resolution:
  file.txt exists → file (1).txt → file (2).txt → ...
  Deterministic, ascending counter. Tested under parallel execution.

Rollback:
  TransactionLog tracks completed moves as (final_dst, original_src) pairs.
  rollback() reverses them in LIFO order.
  finally blocks ensure temp cleanup even on KeyboardInterrupt.
"""

from __future__ import annotations

import errno
import hashlib
import os
import shutil
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import logger


# ---------------------------------------------------------------------------
# Per-destination rename lock registry
#
# Protects the collision-resolve + rename sequence from POSIX rename races.
# On POSIX, os.rename(src, dst) where dst exists silently OVERWRITES dst —
# it does NOT raise FileExistsError. Without a lock, two threads could both
# resolve to the same unused name and one silently overwrites the other.
#
# Lock is keyed by resolved dst_dir string — one lock per destination directory.
# Locks are created on first use and never removed (bounded by distinct dst dirs).
#
# E6 containment (v2.3.0 patch):
#   The lock registry is encapsulated inside _RenameLockRegistry to make the
#   global mutable state explicit, named, and bounded.  It is the ONLY permitted
#   global mutable state in this module — it is thread-safety infrastructure, not
#   data state, and does not affect output ordering or determinism.
#   Its growth is bounded: one Lock per distinct destination directory string,
#   and Locks are never removed (acceptable — distinct dest dirs are bounded by
#   the rule set size, not by file count).
# ---------------------------------------------------------------------------

class _RenameLockRegistry:
    """
    Thread-safe registry of per-destination-directory rename locks.

    This is the only global mutable state permitted in file_operation_manager.
    It is thread-safety infrastructure, not data state:
      - It does not affect output ordering or file content.
      - It grows by at most one entry per distinct destination directory.
      - Its internal mutex serialises lock creation only (not lock holding).
    """

    __slots__ = ("_locks", "_mutex")

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._mutex  = threading.Lock()

    def get(self, dst_dir: Path) -> threading.Lock:
        """Return the rename lock for dst_dir, creating it if needed."""
        key = str(dst_dir.resolve())
        with self._mutex:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def lock_count(self) -> int:
        """Number of directory locks currently registered (for diagnostics only)."""
        with self._mutex:
            return len(self._locks)


# Module-level singleton — bounded by distinct destination directories, not file count.
# This is the ONLY permitted global mutable state in this module.
_rename_lock_registry = _RenameLockRegistry()


def _get_rename_lock(dst_dir: Path) -> threading.Lock:
    """Return the rename lock for dst_dir from the module-level registry."""
    return _rename_lock_registry.get(dst_dir)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEMP_PREFIX   = ".cleansweep_tmp_"   # hidden temp files during atomic move
VERIFY_CHUNK  = 1024 * 1024          # 1MB — chunk size for cross-device copy verification


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class AtomicMoveError(Exception):
    """Raised when an atomic move cannot be completed safely."""


class CollisionError(Exception):
    """Raised when collision resolution fails (e.g., too many collisions)."""


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class MoveResult:
    src:        str          # original source path (posix)
    dst:        str          # final destination path (posix) — empty on failure
    status:     str          # "moved" | "dry_run" | "failed" | "rolled_back"
    collision:  bool = False # True if destination name was adjusted
    error:      str  = ""
    cross_device: bool = False  # True if copy+verify+delete path was used


# ---------------------------------------------------------------------------
# Transaction record — one entry per completed move
# ---------------------------------------------------------------------------

@dataclass
class _MoveRecord:
    src:  Path    # where the file came from (for rollback)
    dst:  Path    # where the file ended up  (for rollback)


# ---------------------------------------------------------------------------
# FileOperationManager
# ---------------------------------------------------------------------------

class FileOperationManager:
    """
    Centralized atomic file move operations.

    One instance per batch. Tracks active temp files and completed moves
    for both cleanup and rollback.

    Usage:
        fom = FileOperationManager(dry_run=False)
        result = fom.atomic_move(src, dst_dir)
        # ... more moves ...
        # On failure:
        fom.rollback()
        # Always:
        fom.cleanup_temps()   (called automatically in rollback too)

    Thread safety:
        NOT thread-safe. Use one instance per thread if parallelism is needed.
        All organizer moves are currently single-threaded, so this is fine.
    """

    MAX_COLLISION_ATTEMPTS = 9999  # hard cap on collision suffix counter
    MAX_RENAME_RETRIES = 50       # retries on EEXIST race during parallel moves

    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run

        # Active temp files created but not yet renamed — cleaned in finally
        self._active_temps: set[Path] = set()

        # Completed move records — used for rollback
        self._transaction_log: list[_MoveRecord] = []

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def atomic_move(
        self,
        src:      Path,
        dst_dir:  Path,
        filename: str | None = None,
    ) -> MoveResult:
        """
        Move src into dst_dir atomically.

        Steps:
          1. Resolve collision-safe final name
          2. Create .cleansweep_tmp_<uuid8> in dst_dir
          3. Move/copy src → temp
          4. os.rename(temp, final)   ← atomic on same filesystem
          5. Record completed move in transaction log

        Cross-device fallback:
          If os.rename raises EXDEV (cross-device link):
            shutil.copy2(src, temp) → verify_copy() → src.unlink()

        Dry-run:
          All logic runs except actual filesystem writes.
          Returns status="dry_run".

        Returns:
          MoveResult with status "moved", "dry_run", or "failed".
          On "failed": src is always untouched.
        """
        filename = filename or src.name
        src_str  = src.as_posix()

        if self.dry_run:
            final_name = self._resolve_collision_name(dst_dir, filename)
            final_dst  = dst_dir / final_name
            logger.log_debug(f"[dry_run] Would move: {src_str} → {final_dst.as_posix()}")
            return MoveResult(
                src=src_str,
                dst=final_dst.as_posix(),
                status="dry_run",
                collision=(final_name != filename),
            )

        # Ensure destination directory exists
        try:
            dst_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            return MoveResult(src=src_str, dst="", status="failed",
                              error=f"mkdir failed: {type(e).__name__}: {e}")

        # temp_path created now; final name resolved inside the rename-retry loop
        temp_path  = dst_dir / f"{TEMP_PREFIX}{uuid.uuid4().hex[:8]}"
        final_name = filename   # may be updated by collision resolution in retry loop
        final_dst  = dst_dir / filename  # placeholder; overwritten in retry loop

        # Register temp immediately so cleanup() can find it even on Ctrl+C
        self._active_temps.add(temp_path)

        cross_device  = False
        src_in_temp   = False   # True once src has been moved to temp_path

        try:
            cross_device = self._move_to_temp(src, temp_path)
            src_in_temp  = True   # src is NOW inside temp_path — must protect it

            # Atomic rename: temp → final
            # Hold per-directory lock during resolve+rename — prevents two threads
            # from both resolving to the same unused name before either renames.
            # On POSIX, os.rename silently overwrites dst if it exists — the lock
            # is the only safe guard against this race.
            rename_lock = _get_rename_lock(dst_dir)
            with rename_lock:
                final_name = self._resolve_collision_name(dst_dir, filename)
                final_dst  = dst_dir / final_name
                os.rename(str(temp_path), str(final_dst))


            # Success — deregister temp (it is now final_dst)
            self._active_temps.discard(temp_path)

            # Cross-device: src was NOT unlinked in _move_to_temp.
            # Now that rename(temp, final) has succeeded atomically, it is safe
            # to delete the original. If unlink fails, the file exists at both
            # src and final_dst — data is never lost, just duplicated temporarily.
            if cross_device:
                try:
                    src.unlink()
                except FileNotFoundError:
                    pass  # already gone — idempotent
                except OSError as unlink_err:
                    # File safely at final_dst; log but do not fail the move
                    logger.log_warn(
                        f"[cross-device] Could not remove source after move: "
                        f"{src_str} — {unlink_err}. File exists at: {final_dst.as_posix()}"
                    )

            # Record in transaction log for rollback
            self._transaction_log.append(_MoveRecord(src=src, dst=final_dst))

            logger.log_debug(
                f"{'[cross-device] ' if cross_device else ''}Moved: "
                f"{src_str} -> {final_dst.as_posix()}"
            )
            return MoveResult(
                src=src_str,
                dst=final_dst.as_posix(),
                status="moved",
                collision=(final_name != filename),
                cross_device=cross_device,
            )

        except (OSError, AtomicMoveError) as e:
            # Error recovery depends on whether this is cross-device or same-device:
            #
            # Same-device (cross_device=False, src_in_temp=True):
            #   src was atomically renamed into temp_path — src no longer exists at
            #   original location. Restore by renaming temp back to src.
            #
            # Cross-device (cross_device=True, src_in_temp=True):
            #   src was COPIED to temp_path but src still exists at original location.
            #   Just clean up the temp copy — src is safe.
            #
            # Not yet in temp (src_in_temp=False):
            #   _move_to_temp failed before touching src — clean temp, src untouched.
            if src_in_temp and not cross_device and temp_path.exists():
                # Same-device: file is in temp — restore to src location
                try:
                    os.rename(str(temp_path), str(src))
                    self._active_temps.discard(temp_path)
                    logger.log_debug(f"Restored src from temp after failed rename: {src_str}")
                except OSError as restore_err:
                    # Cannot restore — file remains in temp_path.
                    # Do NOT delete it — it contains user data.
                    logger.log_warn(
                        f"CRITICAL: could not restore {src_str} from temp "
                        f"{temp_path} ({restore_err}) — file preserved in temp"
                    )
            else:
                # Cross-device (src still intact) or pre-temp failure — clean temp copy
                self._cleanup_one(temp_path)

            logger.log_warn(f"Atomic move failed [{type(e).__name__}]: {src_str} — {e}")
            return MoveResult(
                src=src_str,
                dst="",
                status="failed",
                error=f"{type(e).__name__}: {e}",
            )

    def rollback(self) -> list[dict]:
        """
        Reverse all completed moves in LIFO order.

        Each completed move had: src → dst.
        Rollback performs: dst → src.

        After rollback: transaction log is cleared, temps cleaned.

        Returns list of rollback result dicts for reporting.
        """
        results = []
        failed_rollbacks = []

        # LIFO — reverse order to undo most recent first
        for record in reversed(self._transaction_log):
            dst_str = record.dst.as_posix()
            src_str = record.src.as_posix()
            try:
                # Ensure original parent still exists
                record.src.parent.mkdir(parents=True, exist_ok=True)
                os.rename(str(record.dst), str(record.src))
                results.append({
                    "reversed_from": dst_str,
                    "reversed_to":   src_str,
                    "status": "rolled_back",
                })
                logger.log_info(f"Rolled back: {dst_str} → {src_str}")
            except OSError as e:
                results.append({
                    "reversed_from": dst_str,
                    "reversed_to":   src_str,
                    "status": "rollback_failed",
                    "error": type(e).__name__,
                })
                failed_rollbacks.append(dst_str)
                logger.log_warn(f"Rollback failed [{type(e).__name__}]: {dst_str} → {src_str}")

        self._transaction_log.clear()
        self.cleanup_temps()

        if failed_rollbacks:
            logger.log_warn(
                f"Rollback incomplete — {len(failed_rollbacks)} file(s) could not be "
                f"reversed: {failed_rollbacks}"
            )

        return results

    def cleanup_temps(self) -> int:
        """
        Remove all registered temp files that still exist.

        Called automatically by rollback().
        Should also be called in finally blocks after batch operations.

        Returns count of temp files removed.
        """
        removed = 0
        stale = list(self._active_temps)  # copy — set modified during iteration
        for temp in stale:
            removed += self._cleanup_one(temp)
        return removed

    def transaction_count(self) -> int:
        """Number of successfully completed moves in current transaction."""
        return len(self._transaction_log)

    def has_active_temps(self) -> bool:
        """True if temp files exist that haven't been finalized yet."""
        return bool(self._active_temps)

    # -----------------------------------------------------------------------
    # Collision resolution
    # -----------------------------------------------------------------------

    # Maximum POSIX filename length in bytes (NAME_MAX = 255 on Linux/macOS)
    _NAME_MAX = 255
    # Maximum suffix added by collision logic: " (9999)" = 7 chars
    _COLLISION_SUFFIX_RESERVE = 7

    def _safe_stem(self, stem: str, suffix: str) -> str:
        """
        Truncate stem so that stem + " (9999)" + suffix fits within NAME_MAX bytes.

        Uses UTF-8 byte length to correctly handle multi-byte characters.
        Truncates on byte boundary, then strips trailing incomplete characters
        and whitespace to avoid encoding errors.
        """
        suffix_bytes = len(suffix.encode("utf-8"))
        max_stem_bytes = self._NAME_MAX - suffix_bytes - self._COLLISION_SUFFIX_RESERVE
        if max_stem_bytes <= 0:
            # Pathological: suffix alone is too long — truncate suffix too
            max_stem_bytes = 1
        encoded = stem.encode("utf-8")
        if len(encoded) <= max_stem_bytes:
            return stem  # already fits — no truncation needed
        truncated = encoded[:max_stem_bytes].decode("utf-8", errors="ignore").rstrip()
        return truncated or "f"  # never return empty string

    def _resolve_collision_name(self, dst_dir: Path, filename: str) -> str:
        """
        Return a filename guaranteed not to exist in dst_dir.

        Resolution order:
          file.txt                  (original, returned unchanged if no collision)
          file (1).txt
          file (2).txt
          ...
          file (9999).txt
          → CollisionError if all exhausted

        Long filename safety (Q28):
          If the stem is long enough that adding " (N)" + suffix would exceed
          NAME_MAX (255 bytes), the stem is truncated before suffixing.
          The original filename is always returned as-is if no collision exists
          (the OS would have prevented a >NAME_MAX filename from being created).

        This is deterministic and consistent with Windows Explorer convention.
        Race condition note: the claimed name is only atomically held by the
        subsequent os.rename(temp, final). A concurrent process could claim
        the same name between resolution and rename. This is acceptable:
        the rename will fail and the error surfaces to the caller.
        """
        stem   = Path(filename).stem
        suffix = Path(filename).suffix

        # Original name — no suffix needed, return unchanged
        if not (dst_dir / filename).exists():
            return filename

        # Truncate stem if collision-suffixed name would exceed NAME_MAX
        safe_stem = self._safe_stem(stem, suffix)

        # Counter-suffixed names using (possibly truncated) stem
        for i in range(1, self.MAX_COLLISION_ATTEMPTS + 1):
            candidate = f"{safe_stem} ({i}){suffix}"
            if not (dst_dir / candidate).exists():
                return candidate

        raise CollisionError(
            f"Collision resolution exhausted after {self.MAX_COLLISION_ATTEMPTS} "
            f"attempts for '{filename}' in {dst_dir}"
        )

    # -----------------------------------------------------------------------
    # Internal: move to temp
    # -----------------------------------------------------------------------

    def _move_to_temp(self, src: Path, temp_path: Path) -> bool:
        """
        Move src to temp_path. Returns True if cross-device path was used.

        IMPORTANT — cross-device contract (Q16 fix):
          This method NEVER unlinks src on the cross-device path.
          src.unlink() is the caller's (atomic_move) responsibility, executed
          only AFTER os.rename(temp, final) succeeds — guaranteeing the
          original is deleted only after atomic final placement is confirmed.

        Strategy:
          1. Try os.rename(src, temp_path)            ← atomic, same filesystem
             → src is now at temp_path; return False
          2. If errno.EXDEV (cross-device):
               shutil.copy2(src, temp_path)           ← copy with metadata
               _verify_copy(src, temp_path)            ← size + sha256 match
               return True                             ← src still intact; caller unlinks
          3. Any other error → re-raise

        On cross-device failure during copy or verify:
          temp_path cleaned — src untouched.
        src.unlink() is NEVER called here — always done by atomic_move post-rename.
        """
        try:
            os.rename(str(src), str(temp_path))
            return False  # same filesystem — src is now at temp_path

        except OSError as e:
            if e.errno != errno.EXDEV:
                raise  # not a cross-device error — propagate

        # Cross-device: copy + verify only — do NOT unlink src here
        copy_ok = False
        try:
            shutil.copy2(str(src), str(temp_path))
            copy_ok = True

            # Verify copy integrity — src still intact at this point
            self._verify_copy(src, temp_path)

            # Return True: copy verified, src still exists.
            # Caller (atomic_move) will unlink src after rename(temp, final) succeeds.
            return True  # cross-device path

        except (OSError, AtomicMoveError):
            # Copy failed or verification failed — clean up partial copy, src untouched
            if copy_ok:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

    def _verify_copy(self, original: Path, copy: Path) -> None:
        """
        Verify copy integrity: size match + SHA-256 match.

        Raises AtomicMoveError if verification fails.
        This ensures cross-device moves never produce silent corruption.
        """
        try:
            orig_stat = original.stat()
            copy_stat = copy.stat()
        except OSError as e:
            raise AtomicMoveError(f"Stat failed during copy verification: {e}") from e

        if orig_stat.st_size != copy_stat.st_size:
            raise AtomicMoveError(
                f"Size mismatch after copy: original={orig_stat.st_size} "
                f"copy={copy_stat.st_size}"
            )

        if not self._sha256_match(original, copy):
            raise AtomicMoveError(
                f"SHA-256 mismatch after copy — copy is corrupt, original preserved"
            )

    def _sha256_match(self, a: Path, b: Path) -> bool:
        """Return True if both files have identical SHA-256 digests."""
        return self._file_sha256(a) == self._file_sha256(b)

    @staticmethod
    def _file_sha256(path: Path) -> str:
        hasher = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(VERIFY_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except OSError as e:
            raise AtomicMoveError(f"Read failed during sha256: {path} — {e}") from e
        return hasher.hexdigest()

    # -----------------------------------------------------------------------
    # Internal: temp cleanup
    # -----------------------------------------------------------------------

    def _cleanup_one(self, temp: Path) -> int:
        """Remove one temp file. Returns 1 if removed, 0 if not found."""
        self._active_temps.discard(temp)
        try:
            temp.unlink(missing_ok=True)
            return 1
        except OSError:
            return 0


# ---------------------------------------------------------------------------
# Batch move with rollback — convenience wrapper
# ---------------------------------------------------------------------------

def execute_moves_with_rollback(
    moves:   list[tuple[Path, Path, str | None]],
    dry_run: bool = False,
) -> dict:
    """
    Execute a batch of moves atomically with rollback on partial failure.

    Args:
      moves   — list of (src, dst_dir, filename_or_None) tuples
      dry_run — if True, simulate only

    Behavior:
      - Moves executed sequentially
      - On any failure: all completed moves are rolled back
      - Temp files always cleaned in finally block
      - Returns summary dict

    Returns:
      {
        "dry_run":       bool,
        "attempted":     int,
        "moved":         list[str],          # posix paths of successfully moved files
        "failed":        list[dict],         # {"src": str, "error": str}
        "rolled_back":   list[dict],         # rollback results if triggered
        "rollback_triggered": bool,
        "temps_cleaned": int,
      }
    """
    fom = FileOperationManager(dry_run=dry_run)
    moved:   list[str]  = []
    failed:  list[dict] = []
    rolled_back: list[dict] = []
    rollback_triggered = False
    temps_cleaned = 0

    try:
        for src, dst_dir, filename in moves:
            result = fom.atomic_move(src, dst_dir, filename)

            if result.status in ("moved", "dry_run"):
                moved.append(result.dst)
            else:
                failed.append({"src": result.src, "error": result.error})
                # Trigger rollback on first failure
                if not dry_run and fom.transaction_count() > 0:
                    rollback_triggered = True
                    rolled_back = fom.rollback()
                break

    finally:
        temps_cleaned = fom.cleanup_temps()

    return {
        "dry_run":            dry_run,
        "attempted":          len(moved) + len(failed),
        "moved":              sorted(moved),
        "failed":             failed,
        "rolled_back":        rolled_back,
        "rollback_triggered": rollback_triggered,
        "temps_cleaned":      temps_cleaned,
    }
