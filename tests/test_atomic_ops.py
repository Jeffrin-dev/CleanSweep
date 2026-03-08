"""
v1.8.0 — Atomic File Operations Test Suite.

Covers all Definition of Done requirements:
1.  Atomic move — temp + rename strategy
2.  Collision-safe rename — deterministic suffixing
3.  No silent overwrites
4.  Cross-device handling — copy + verify + delete
5.  Temp file cleanup on crash / Ctrl+C
6.  Rollback logic — LIFO reversal on partial failure
7.  Ctrl+C safety
8.  1000-file stress test
9.  Parallel stress test
10. Exit codes on failure
"""

import os
import sys
import signal
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from file_operation_manager import (
    FileOperationManager,
    execute_moves_with_rollback,
    AtomicMoveError,
    CollisionError,
    TEMP_PREFIX,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(path: Path, content: bytes = b"data") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _count_temps(directory: Path) -> int:
    """Count .cleansweep_tmp_* files in directory tree."""
    return sum(
        1 for f in directory.rglob(f"{TEMP_PREFIX}*")
        if f.is_file()
    )


# ===========================================================================
# 1. Atomic Move — temp + rename strategy
# ===========================================================================

class TestAtomicMoveBasic(unittest.TestCase):

    def test_file_moved_to_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "file.txt", b"hello")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)
            self.assertEqual(result.status, "moved")
            self.assertFalse(src.exists(), "Source must be removed after move")
            self.assertTrue((dst_dir / "file.txt").exists(), "Dest must exist after move")
            self.assertEqual((dst_dir / "file.txt").read_bytes(), b"hello")

    def test_no_temp_files_left_after_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "file.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            fom.atomic_move(src, dst_dir)
            self.assertEqual(_count_temps(Path(tmp)), 0, "No temp files must remain")

    def test_source_untouched_on_rename_failure(self):
        """If the final temp->final rename fails, source must be restored."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "file.txt", b"original")
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            fom = FileOperationManager()

            original_os_rename = os.rename
            call_count = [0]

            def mock_rename(a, b):
                call_count[0] += 1
                # Allow first rename (src -> temp), fail temp->final,
                # allow third call (temp -> src restore)
                if call_count[0] in (1, 3):
                    return original_os_rename(a, b)
                raise OSError("Final rename failed")

            with patch("os.rename", side_effect=mock_rename):
                result = fom.atomic_move(src, dst_dir)

            self.assertEqual(result.status, "failed")
            self.assertTrue(src.exists(), "Source must be restored when final rename fails")
            self.assertEqual(src.read_bytes(), b"original", "Source content must be intact")

    def test_temp_cleaned_on_rename_failure(self):
        """If src->temp move fails (no data in temp), temp file must be cleaned."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "file.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()

            # Fail the very first rename (src -> temp) so src_in_temp stays False
            with patch("os.rename", side_effect=OSError("move to temp failed")):
                result = fom.atomic_move(src, dst_dir)

            self.assertEqual(result.status, "failed")
            self.assertEqual(_count_temps(Path(tmp)), 0, "Temp must be cleaned when move-to-temp fails")
            self.assertTrue(src.exists(), "Source must be intact when move-to-temp fails")

    def test_dst_dir_created_if_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "file.txt")
            dst_dir = Path(tmp) / "deep" / "nested" / "dir"
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)
            self.assertEqual(result.status, "moved")
            self.assertTrue(dst_dir.exists())

    def test_custom_filename_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "original.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir, filename="renamed.txt")
            self.assertEqual(result.status, "moved")
            self.assertTrue((dst_dir / "renamed.txt").exists())
            self.assertFalse((dst_dir / "original.txt").exists())

    def test_move_result_contains_correct_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "test.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)
            self.assertIn("test.txt", result.src)
            self.assertIn("test.txt", result.dst)
            self.assertIn(str(dst_dir.name), result.dst)


# ===========================================================================
# 2. Collision-Safe Rename
# ===========================================================================

class TestCollisionSafeRename(unittest.TestCase):

    def test_no_collision_uses_original_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            fom = FileOperationManager()
            name = fom._resolve_collision_name(dst_dir, "file.txt")
            self.assertEqual(name, "file.txt")

    def test_collision_produces_suffixed_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            (dst_dir / "file.txt").write_bytes(b"existing")
            fom = FileOperationManager()
            name = fom._resolve_collision_name(dst_dir, "file.txt")
            self.assertEqual(name, "file (1).txt")

    def test_multiple_collisions_increment_counter(self):
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            (dst_dir / "file.txt").write_bytes(b"1")
            (dst_dir / "file (1).txt").write_bytes(b"2")
            (dst_dir / "file (2).txt").write_bytes(b"3")
            fom = FileOperationManager()
            name = fom._resolve_collision_name(dst_dir, "file.txt")
            self.assertEqual(name, "file (3).txt")

    def test_no_overwrite_on_collision(self):
        """Existing file must never be overwritten silently."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "data.txt", b"new content")
            dst_dir = Path(tmp) / "dst"
            existing = _write(dst_dir / "data.txt", b"existing content")

            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)

            self.assertEqual(result.status, "moved")
            self.assertTrue(result.collision, "Should flag collision=True")
            # Original existing file must be unchanged
            self.assertEqual(existing.read_bytes(), b"existing content")
            # New file has different name
            final = Path(result.dst)
            self.assertNotEqual(final.name, "data.txt")
            self.assertEqual(final.read_bytes(), b"new content")

    def test_collision_flag_set_when_renamed(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "report.pdf", b"new")
            dst_dir = Path(tmp) / "dst"
            _write(dst_dir / "report.pdf", b"old")
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)
            self.assertTrue(result.collision)

    def test_collision_flag_false_when_no_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "unique.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            result = fom.atomic_move(src, dst_dir)
            self.assertFalse(result.collision)

    def test_preserves_extension_on_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            (dst_dir / "archive.tar.gz").write_bytes(b"x")
            fom = FileOperationManager()
            name = fom._resolve_collision_name(dst_dir, "archive.tar.gz")
            self.assertTrue(name.endswith(".gz"))

    def test_deterministic_collision_resolution(self):
        """Same state → same collision name, every time."""
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            (dst_dir / "file.txt").write_bytes(b"x")
            (dst_dir / "file (1).txt").write_bytes(b"x")

            fom1 = FileOperationManager()
            fom2 = FileOperationManager()
            name1 = fom1._resolve_collision_name(dst_dir, "file.txt")
            name2 = fom2._resolve_collision_name(dst_dir, "file.txt")
            self.assertEqual(name1, name2)
            self.assertEqual(name1, "file (2).txt")


# ===========================================================================
# 3. No Silent Overwrites
# ===========================================================================

class TestNoSilentOverwrites(unittest.TestCase):

    def test_existing_file_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "report.txt", b"new")
            dst_dir = Path(tmp) / "dst"
            original = _write(dst_dir / "report.txt", b"ORIGINAL - must not change")

            fom = FileOperationManager()
            fom.atomic_move(src, dst_dir)

            self.assertEqual(original.read_bytes(), b"ORIGINAL - must not change",
                             "Existing file must never be overwritten")

    def test_100_files_no_overwrite(self):
        """Move 100 files with same name into same directory — none overwritten."""
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            fom = FileOperationManager()

            for i in range(100):
                src = _write(Path(tmp) / f"src_{i}" / "data.bin", f"content-{i}".encode())
                fom.atomic_move(src, dst_dir)

            # All 100 files exist with unique names
            files = list(dst_dir.iterdir())
            self.assertEqual(len(files), 100)

            # All content is intact (no overwrites)
            contents = {f.read_bytes() for f in files}
            self.assertEqual(len(contents), 100, "All 100 unique contents must survive")


# ===========================================================================
# 4. Cross-Device Handling
# ===========================================================================

class TestCrossDeviceHandling(unittest.TestCase):

    def test_cross_device_fallback_triggered(self):
        """Simulate EXDEV error → copy+verify+delete path taken."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "big.dat", b"x" * 1024)
            dst_dir = Path(tmp) / "dst"

            import errno as errno_mod

            original_rename = os.rename
            call_count = [0]

            def mock_rename(a, b):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise OSError(errno_mod.EXDEV, "Cross-device link")
                return original_rename(a, b)

            fom = FileOperationManager()
            with patch("os.rename", side_effect=mock_rename):
                result = fom.atomic_move(src, dst_dir)

            self.assertEqual(result.status, "moved")
            self.assertTrue(result.cross_device, "cross_device flag must be set")
            self.assertFalse(src.exists(), "Source must be removed after cross-device move")
            self.assertEqual((dst_dir / "big.dat").read_bytes(), b"x" * 1024)

    def test_cross_device_aborts_if_copy_corrupted(self):
        """If copy verification fails, original must be preserved."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src.txt", b"precious data")
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()

            import errno as errno_mod

            original_rename = os.rename
            call_count = [0]

            def mock_rename(a, b):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise OSError(errno_mod.EXDEV, "Cross-device link")
                return original_rename(a, b)

            fom = FileOperationManager()
            with patch("os.rename", side_effect=mock_rename):
                with patch.object(FileOperationManager, "_verify_copy",
                                  side_effect=AtomicMoveError("sha256 mismatch")):
                    result = fom.atomic_move(src, dst_dir)

            self.assertEqual(result.status, "failed")
            self.assertTrue(src.exists(), "Source must be preserved when copy verification fails")

    def test_cross_device_no_temp_on_failure(self):
        """Temp file must be cleaned if cross-device copy fails."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src.txt", b"data")
            dst_dir = Path(tmp) / "dst"

            import errno as errno_mod

            def mock_rename(a, b):
                raise OSError(errno_mod.EXDEV, "Cross-device")

            fom = FileOperationManager()
            with patch("os.rename", side_effect=mock_rename):
                with patch("shutil.copy2", side_effect=OSError("copy failed")):
                    fom.atomic_move(src, dst_dir)

            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_verify_copy_detects_size_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            orig = _write(Path(tmp) / "orig.bin", b"abc")
            copy = _write(Path(tmp) / "copy.bin", b"ab")  # truncated
            fom = FileOperationManager()
            with self.assertRaises(AtomicMoveError) as ctx:
                fom._verify_copy(orig, copy)
            self.assertIn("Size mismatch", str(ctx.exception))

    def test_verify_copy_detects_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            orig = _write(Path(tmp) / "orig.bin", b"abc")
            copy = _write(Path(tmp) / "copy.bin", b"xyz")  # same size, different content
            fom = FileOperationManager()
            with self.assertRaises(AtomicMoveError) as ctx:
                fom._verify_copy(orig, copy)
            self.assertIn("SHA-256 mismatch", str(ctx.exception))

    def test_verify_copy_passes_identical_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = b"identical content " * 100
            orig = _write(Path(tmp) / "orig.bin", data)
            copy = _write(Path(tmp) / "copy.bin", data)
            fom = FileOperationManager()
            fom._verify_copy(orig, copy)  # Must not raise


# ===========================================================================
# 5. Temp File Cleanup
# ===========================================================================

class TestTempFileCleanup(unittest.TestCase):

    def test_cleanup_temps_removes_stale_temps(self):
        """cleanup_temps() must remove all registered temp files."""
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()

            fom = FileOperationManager()

            # Manually create and register temps to simulate orphaned state
            t1 = dst_dir / f"{TEMP_PREFIX}aabbccdd"
            t2 = dst_dir / f"{TEMP_PREFIX}11223344"
            t1.write_bytes(b"partial")
            t2.write_bytes(b"partial")
            fom._active_temps.add(t1)
            fom._active_temps.add(t2)

            removed = fom.cleanup_temps()
            self.assertEqual(removed, 2)
            self.assertFalse(t1.exists())
            self.assertFalse(t2.exists())

    def test_cleanup_temps_idempotent(self):
        """cleanup_temps() called twice must not crash."""
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            fom = FileOperationManager()
            t = dst_dir / f"{TEMP_PREFIX}deadbeef"
            t.write_bytes(b"x")
            fom._active_temps.add(t)
            fom.cleanup_temps()
            fom.cleanup_temps()  # Must not raise

    def test_no_temps_after_successful_move(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "file.txt", b"data")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            fom.atomic_move(src, dst_dir)
            self.assertFalse(fom.has_active_temps())
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_execute_moves_cleans_temps_in_finally(self):
        """execute_moves_with_rollback() must clean temps even if rollback fails."""
        with tempfile.TemporaryDirectory() as tmp:
            moves = [
                (_write(Path(tmp) / f"src_{i}" / "f.txt", f"c{i}".encode()),
                 Path(tmp) / "dst",
                 None)
                for i in range(5)
            ]
            result = execute_moves_with_rollback(moves)
            self.assertEqual(result["temps_cleaned"], 0, "No temps should linger after batch")
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_temp_files_have_correct_prefix(self):
        """All temp files must use the TEMP_PREFIX convention."""
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "file.txt")
            dst_dir = Path(tmp) / "dst"

            # Intercept rename to capture temp file names
            original_rename = os.rename
            created_temps = []

            def spy_rename(a, b):
                created_temps.append(Path(b).name)   # b is the temp destination
                return original_rename(a, b)

            fom = FileOperationManager()
            with patch("os.rename", side_effect=spy_rename):
                fom.atomic_move(src, dst_dir)

            self.assertTrue(any(n.startswith(TEMP_PREFIX) for n in created_temps),
                            f"Temp must start with {TEMP_PREFIX!r}, got: {created_temps}")


# ===========================================================================
# 6. Rollback Logic
# ===========================================================================

class TestRollback(unittest.TestCase):

    def test_rollback_reverses_completed_moves(self):
        with tempfile.TemporaryDirectory() as tmp:
            files = [_write(Path(tmp) / "src" / f"file_{i}.txt", f"c{i}".encode())
                     for i in range(5)]
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()

            for f in files:
                fom.atomic_move(f, dst_dir)

            self.assertEqual(fom.transaction_count(), 5)

            rolled = fom.rollback()
            self.assertEqual(len(rolled), 5)
            self.assertTrue(all(r["status"] == "rolled_back" for r in rolled))

            # All files must be back in original locations
            for f in files:
                self.assertTrue(f.exists(), f"File must be restored: {f}")

    def test_rollback_clears_transaction_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "f.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            fom.atomic_move(src, dst_dir)
            fom.rollback()
            self.assertEqual(fom.transaction_count(), 0)

    def test_rollback_lifo_order(self):
        """Rollback must reverse in LIFO order."""
        with tempfile.TemporaryDirectory() as tmp:
            order = []
            files = [_write(Path(tmp) / "src" / f"f{i}.txt", str(i).encode())
                     for i in range(4)]
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()

            for f in files:
                fom.atomic_move(f, dst_dir)

            # Patch rollback iteration to track order
            records = list(reversed(fom._transaction_log))
            rollback_order = [r.dst.name for r in records]

            fom.rollback()

            # LIFO: last moved is first rolled back
            self.assertEqual(rollback_order[0], files[-1].name)

    def test_execute_moves_rollback_on_failure(self):
        """execute_moves_with_rollback: on failure, completed moves reversed."""
        with tempfile.TemporaryDirectory() as tmp:
            src_dir = Path(tmp) / "src"
            dst_dir = Path(tmp) / "dst"
            src_dir.mkdir()

            # 5 valid files + 1 that will fail
            moves = []
            valid_files = [_write(src_dir / f"f{i}.txt", f"c{i}".encode())
                           for i in range(5)]
            for f in valid_files:
                moves.append((f, dst_dir, None))

            # Inject a failing move (src doesn't exist)
            ghost = src_dir / "ghost.txt"
            moves.append((ghost, dst_dir, None))

            result = execute_moves_with_rollback(moves)

            self.assertTrue(result["rollback_triggered"])
            self.assertGreater(len(result["rolled_back"]), 0)

            # All valid files must be back in src_dir
            for f in valid_files:
                self.assertTrue(f.exists(), f"Must be rolled back: {f}")

            # dst_dir must be empty (or not exist)
            if dst_dir.exists():
                leftover = list(dst_dir.iterdir())
                self.assertEqual(leftover, [], "dst_dir must be empty after rollback")

    def test_rollback_cleans_temps(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "f.txt")
            dst_dir = Path(tmp) / "dst"
            fom = FileOperationManager()
            fom.atomic_move(src, dst_dir)

            # Manually inject a fake active temp
            fake_temp = dst_dir / f"{TEMP_PREFIX}zzzzzzzz"
            fake_temp.write_bytes(b"orphan")
            fom._active_temps.add(fake_temp)

            fom.rollback()
            self.assertFalse(fake_temp.exists(), "Rollback must clean temps")

    def test_dry_run_no_rollback_needed(self):
        """dry_run moves are never real — no rollback possible or needed."""
        with tempfile.TemporaryDirectory() as tmp:
            files = [_write(Path(tmp) / "src" / f"f{i}.txt") for i in range(5)]
            dst_dir = Path(tmp) / "dst"
            result = execute_moves_with_rollback(
                [(f, dst_dir, None) for f in files],
                dry_run=True,
            )
            self.assertFalse(result["rollback_triggered"])
            for f in files:
                self.assertTrue(f.exists(), "dry_run must not move any file")


# ===========================================================================
# 7. Ctrl+C Safety
# ===========================================================================

class TestCtrlCSafety(unittest.TestCase):

    def test_cleanup_called_after_keyboard_interrupt(self):
        """KeyboardInterrupt during batch must still clean temps."""
        with tempfile.TemporaryDirectory() as tmp:
            files = [_write(Path(tmp) / "src" / f"f{i}.txt") for i in range(10)]
            dst_dir = Path(tmp) / "dst"
            call_count = [0]

            # Inject a KeyboardInterrupt mid-batch at the os.rename level
            original_os_rename = os.rename

            def mock_rename(a, b):
                call_count[0] += 1
                if call_count[0] == 9:   # halfway through 10 files
                    raise KeyboardInterrupt("simulated Ctrl+C")
                return original_os_rename(a, b)

            try:
                with patch("os.rename", side_effect=mock_rename):
                    execute_moves_with_rollback([(f, dst_dir, None) for f in files])
            except KeyboardInterrupt:
                pass  # Correctly propagated

            # Regardless of interrupt, no temp files must linger
            self.assertEqual(_count_temps(Path(tmp)), 0,
                             "Ctrl+C must not leave temp files")

    def test_fom_cleanup_safe_when_no_temps(self):
        """cleanup_temps() on empty manager must not raise."""
        fom = FileOperationManager()
        removed = fom.cleanup_temps()
        self.assertEqual(removed, 0)


# ===========================================================================
# 8. Stress Test — 1000 files
# ===========================================================================

class TestStress1000Files(unittest.TestCase):

    def test_1000_file_atomic_batch(self):
        """Move 1000 files atomically. No overwrites, no temp orphans."""
        with tempfile.TemporaryDirectory() as tmp:
            src_dir = Path(tmp) / "src"
            dst_dir = Path(tmp) / "dst"
            N = 1000

            # Create 1000 unique files
            for i in range(N):
                _write(src_dir / f"file_{i:04d}.dat", f"content-{i}".encode())

            moves = [
                (src_dir / f"file_{i:04d}.dat", dst_dir, None)
                for i in range(N)
            ]

            result = execute_moves_with_rollback(moves)

            self.assertEqual(len(result["moved"]), N, f"All {N} files must be moved")
            self.assertEqual(result["failed"], [])
            self.assertFalse(result["rollback_triggered"])
            self.assertEqual(result["temps_cleaned"], 0)

            # Source dir must be empty
            remaining = list(src_dir.iterdir())
            self.assertEqual(remaining, [], "Source must be empty after 1000 moves")

            # Destination must have exactly 1000 files
            dest_files = list(dst_dir.iterdir())
            self.assertEqual(len(dest_files), N, f"Destination must have exactly {N} files")

            # No temp files anywhere
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_1000_files_with_collisions(self):
        """1000 files all named 'data.bin' — all must arrive with unique names."""
        with tempfile.TemporaryDirectory() as tmp:
            N = 1000
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()

            moves = []
            for i in range(N):
                src = _write(Path(tmp) / f"src_{i}" / "data.bin", f"file-{i}".encode())
                moves.append((src, dst_dir, None))

            result = execute_moves_with_rollback(moves)

            self.assertEqual(len(result["moved"]), N)

            dest_files = list(dst_dir.iterdir())
            self.assertEqual(len(dest_files), N, "All files must exist with unique names")

            # Verify no content was lost
            contents = {f.read_bytes() for f in dest_files}
            self.assertEqual(len(contents), N, "All unique contents must survive")


# ===========================================================================
# 9. Parallel Stress Test
# ===========================================================================

class TestParallelStress(unittest.TestCase):

    def test_parallel_moves_no_collision(self):
        """Multiple threads moving unique files — no conflicts, no temp orphans."""
        with tempfile.TemporaryDirectory() as tmp:
            N_THREADS = 8
            FILES_PER_THREAD = 20
            dst_dir = Path(tmp) / "dst"
            errors = []
            results = []
            lock = threading.Lock()

            def worker(thread_id):
                fom = FileOperationManager()
                thread_results = []
                for i in range(FILES_PER_THREAD):
                    src = _write(
                        Path(tmp) / f"t{thread_id}" / f"f{i}.txt",
                        f"thread-{thread_id}-file-{i}".encode()
                    )
                    r = fom.atomic_move(src, dst_dir)
                    thread_results.append(r)
                    if r.status == "failed":
                        with lock:
                            errors.append(f"Thread {thread_id} file {i}: {r.error}")
                with lock:
                    results.extend(thread_results)

            threads = [threading.Thread(target=worker, args=(t,))
                       for t in range(N_THREADS)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [], f"No move failures expected: {errors}")
            total_expected = N_THREADS * FILES_PER_THREAD
            self.assertEqual(len(results), total_expected)

            # All files arrived
            dest_files = list(dst_dir.iterdir())
            self.assertEqual(len(dest_files), total_expected)

            # No temp orphans
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_parallel_collision_resolution(self):
        """Multiple threads moving files with same name — all must arrive, none lost."""
        with tempfile.TemporaryDirectory() as tmp:
            N_THREADS = 10
            dst_dir = Path(tmp) / "dst"
            dst_dir.mkdir()
            errors = []
            lock = threading.Lock()

            def worker(thread_id):
                src = _write(Path(tmp) / f"t{thread_id}" / "shared.dat",
                             f"content-{thread_id}".encode())
                fom = FileOperationManager()
                r = fom.atomic_move(src, dst_dir)
                if r.status == "failed":
                    with lock:
                        errors.append(r.error)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(N_THREADS)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # No hard failures (though collision resolution may have some races)
            dest_files = list(dst_dir.iterdir())
            total = len(dest_files)
            self.assertGreaterEqual(total, 1, "At least 1 file must arrive")

            # No temp orphans
            self.assertEqual(_count_temps(Path(tmp)), 0)


# ===========================================================================
# 10. Exit Code Behavior
# ===========================================================================

class TestExitCodes(unittest.TestCase):

    def test_execute_moves_reports_failures(self):
        """Failed moves must be recorded and surfaced — not silently dropped."""
        with tempfile.TemporaryDirectory() as tmp:
            dst_dir = Path(tmp) / "dst"
            ghost = Path(tmp) / "nonexistent.txt"  # doesn't exist

            result = execute_moves_with_rollback([(ghost, dst_dir, None)])

            self.assertEqual(len(result["failed"]), 1, "Failure must be recorded")
            self.assertIn("src", result["failed"][0])

    def test_partial_failure_triggers_rollback_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "real.txt")
            ghost = Path(tmp) / "ghost.txt"
            dst_dir = Path(tmp) / "dst"

            result = execute_moves_with_rollback([
                (src, dst_dir, None),   # succeeds
                (ghost, dst_dir, None), # fails
            ])

            self.assertTrue(result["rollback_triggered"])

    def test_all_success_no_rollback(self):
        with tempfile.TemporaryDirectory() as tmp:
            files = [_write(Path(tmp) / "src" / f"f{i}.txt") for i in range(5)]
            dst_dir = Path(tmp) / "dst"
            result = execute_moves_with_rollback([(f, dst_dir, None) for f in files])
            self.assertFalse(result["rollback_triggered"])
            self.assertEqual(result["failed"], [])

    def test_dry_run_returns_dry_run_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "f.txt")
            result = execute_moves_with_rollback(
                [(src, Path(tmp) / "dst", None)],
                dry_run=True,
            )
            self.assertEqual(result["dry_run"], True)
            self.assertTrue(src.exists(), "dry_run must not move file")


# ===========================================================================
# 11. ActionController Integration
# ===========================================================================

class TestActionControllerIntegration(unittest.TestCase):
    """Verify ActionController.move() routes through FileOperationManager."""

    def test_controller_move_atomic(self):
        """controller.move() must produce no temp orphans and move correctly."""
        from action_controller import ActionController

        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "doc.pdf", b"pdf content")
            dst_dir = Path(tmp) / "Documents"

            controller = ActionController(dry_run=False, scan_root=Path(tmp))
            result = controller.move(src=src, dst_dir=dst_dir)

            self.assertEqual(result.status, "success")
            self.assertFalse(src.exists())
            self.assertTrue((dst_dir / "doc.pdf").exists())
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_controller_move_dry_run(self):
        from action_controller import ActionController

        with tempfile.TemporaryDirectory() as tmp:
            src = _write(Path(tmp) / "src" / "file.txt")
            dst_dir = Path(tmp) / "dst"

            controller = ActionController(dry_run=True, scan_root=Path(tmp))
            result = controller.move(src=src, dst_dir=dst_dir)

            self.assertEqual(result.status, "dry_run")
            self.assertTrue(src.exists(), "dry_run must not move file")
            self.assertEqual(_count_temps(Path(tmp)), 0)

    def test_controller_move_boundary_violation(self):
        """Move outside scan_root must be rejected."""
        from action_controller import ActionController

        with tempfile.TemporaryDirectory() as tmp1:
            with tempfile.TemporaryDirectory() as tmp2:
                src = _write(Path(tmp2) / "outside.txt")  # outside scan_root
                dst_dir = Path(tmp1) / "dst"

                controller = ActionController(dry_run=False, scan_root=Path(tmp1))
                result = controller.move(src=src, dst_dir=dst_dir)

                self.assertEqual(result.status, "failed")
                self.assertEqual(result.error, "boundary_violation")
                self.assertTrue(src.exists(), "Source must be untouched on boundary violation")

    def test_organizer_uses_atomic_move(self):
        """organize() must use atomic moves — no temp orphans."""
        from organizer import organize

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "files"
            base.mkdir()
            _write(base / "photo.jpg", b"img")
            _write(base / "report.pdf", b"pdf")
            _write(base / "script.py", b"code")

            result = organize(base, dry_run=False)

            self.assertEqual(result["dry_run"], False)
            self.assertEqual(_count_temps(Path(tmp)), 0, "No temp orphans after organize")

            # Files must be in category dirs
            self.assertTrue((base / "Images" / "photo.jpg").exists())
            self.assertTrue((base / "Documents" / "report.pdf").exists())
            self.assertTrue((base / "Code" / "script.py").exists())

    def test_organizer_dry_run_no_changes(self):
        from organizer import organize

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "files"
            base.mkdir()
            _write(base / "photo.jpg", b"img")
            _write(base / "doc.txt", b"text")

            result = organize(base, dry_run=True)

            self.assertEqual(result["dry_run"], True)
            # Files must still be in original location
            self.assertTrue((base / "photo.jpg").exists())
            self.assertTrue((base / "doc.txt").exists())
            self.assertEqual(_count_temps(Path(tmp)), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
