"""
v1.7.0 — Trash Strategy Tests.

Validates:
1. Files appear in system trash after --delete
2. Trash location is correct (XDG on Linux)
3. Large batch trash works
4. Partial trash failure handled — continues
5. TrashUnavailableError → abort, no silent permanent fallback
6. --permanent requires --delete flag
7. Dry-run unchanged — no mutation
8. Determinism unchanged across trash runs
9. Parallel deletion still safe with trash
10. Idempotency: already-trashed files handled gracefully
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_dupes(base: Path, n_groups: int = 3, per_group: int = 3) -> None:
    for g in range(n_groups):
        content = f"duplicate group {g:04d}".encode()
        for i in range(per_group):
            (base / f"g{g:02d}_{i}.txt").write_bytes(content)


# ---------------------------------------------------------------------------
# Test 1 & 2 — Files appear in XDG trash
# ---------------------------------------------------------------------------

class TestTrashLinuxXDG(unittest.TestCase):
    """Files must move to XDG Trash with correct structure."""

    def test_file_appears_in_xdg_trash(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "trashme.txt"
            f.write_bytes(b"trash this file")

            from trash_manager import TrashManager, _xdg_trash_root
            tm = TrashManager()

            if tm.platform != "linux":
                self.skipTest("XDG test only runs on Linux")

            result = tm.trash(f)

            self.assertEqual(result.status, "trashed")
            self.assertFalse(f.exists(), "Original file must be gone")
            self.assertTrue(Path(result.trash_path).exists(),
                "File must exist in trash")

    def test_trashinfo_file_created(self):
        """XDG spec requires a .trashinfo metadata file."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "with_info.txt"
            f.write_bytes(b"needs trashinfo")

            from trash_manager import TrashManager, _xdg_trash_root
            tm = TrashManager()

            if tm.platform != "linux":
                self.skipTest("XDG test only runs on Linux")

            result = tm.trash(f)
            trash_name = Path(result.trash_path).name
            info_file = _xdg_trash_root() / "info" / f"{trash_name}.trashinfo"

            self.assertTrue(info_file.exists(), "trashinfo file must be created")
            content = info_file.read_text()
            self.assertIn("[Trash Info]", content)
            self.assertIn("Path=", content)
            self.assertIn("DeletionDate=", content)

    def test_unique_names_on_collision(self):
        """Two files with same name must get unique names in trash."""
        with tempfile.TemporaryDirectory() as tmp1:
            with tempfile.TemporaryDirectory() as tmp2:
                f1 = Path(tmp1) / "collision.txt"
                f2 = Path(tmp2) / "collision.txt"
                f1.write_bytes(b"first")
                f2.write_bytes(b"second")

                from trash_manager import TrashManager
                tm = TrashManager()

                if tm.platform != "linux":
                    self.skipTest("XDG test only runs on Linux")

                r1 = tm.trash(f1)
                r2 = tm.trash(f2)

                # Both must succeed with different trash names
                self.assertEqual(r1.status, "trashed")
                self.assertEqual(r2.status, "trashed")
                self.assertNotEqual(r1.trash_path, r2.trash_path,
                    "Collision must produce unique trash names")


# ---------------------------------------------------------------------------
# Test 3 — Large batch trash
# ---------------------------------------------------------------------------

class TestLargeBatchTrash(unittest.TestCase):
    """100 duplicate files trashed — must complete without crash."""

    def test_100_files_trashed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for g in range(20):
                content = f"batch group {g:04d}".encode()
                for i in range(5):
                    (base / f"g{g:02d}_{i}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH

            result = scan_duplicates(base, keep="first")
            self.assertEqual(result["total_duplicate_files"], 80)

            controller = ActionController(
                dry_run=False,
                scan_root=base,
                delete_mode=DELETE_MODE_TRASH,
            )
            deletion = controller.execute_deletions(result["duplicates"])

            self.assertEqual(len(deletion["deleted"]), 80)
            self.assertEqual(len(deletion["failed"]), 0)
            self.assertEqual(len(list(base.iterdir())), 20)


# ---------------------------------------------------------------------------
# Test 4 — Partial trash failure handled
# ---------------------------------------------------------------------------

class TestPartialTrashFailure(unittest.TestCase):
    """One trash failure must not abort remaining deletions."""

    def test_partial_failure_continues(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same partial trash content"
            for i in range(4):
                (base / f"f{i}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH
            from trash_manager import TrashFailedError

            result = scan_duplicates(base, keep="first")
            controller = ActionController(
                dry_run=False, scan_root=base,
                delete_mode=DELETE_MODE_TRASH,
            )

            call_count = [0]
            original_trash = controller._trash_manager.trash

            def failing_trash(path):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise TrashFailedError("Simulated trash failure")
                return original_trash(path)

            controller._trash_manager.trash = failing_trash
            deletion = controller.execute_deletions(result["duplicates"])

            self.assertEqual(len(deletion["failed"]), 1)
            self.assertGreater(len(deletion["deleted"]), 0,
                "Remaining files must still be trashed after one failure")


# ---------------------------------------------------------------------------
# Test 5 — TrashUnavailable → abort, no silent permanent fallback
# ---------------------------------------------------------------------------

class TestTrashUnavailableAborts(unittest.TestCase):
    """If trash unavailable, must abort with clear error. No silent permanent delete."""

    def test_unavailable_records_failure_not_permanent_delete(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "protected.txt"
            f.write_bytes(b"do not permanently delete this")

            from action_controller import ActionController, DELETE_MODE_TRASH
            from trash_manager import TrashUnavailableError

            controller = ActionController(
                dry_run=False, scan_root=base,
                delete_mode=DELETE_MODE_TRASH,
            )

            with mock.patch.object(
                controller._trash_manager, "trash",
                side_effect=TrashUnavailableError("No trash available"),
            ):
                result = controller.delete(f, size=len(f.read_bytes()))

            # File must still exist — not permanently deleted
            self.assertTrue(f.exists(),
                "File must NOT be permanently deleted when trash unavailable")
            self.assertEqual(result.status, "unavailable")
            self.assertTrue(controller.has_failures())

    def test_unavailable_hint_present_in_failure(self):
        """Failure entry must include hint to use --permanent."""
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "f.txt"
            f.write_bytes(b"x")

            from action_controller import ActionController, DELETE_MODE_TRASH
            from trash_manager import TrashUnavailableError

            controller = ActionController(dry_run=False, scan_root=base,
                                          delete_mode=DELETE_MODE_TRASH)

            with mock.patch.object(controller._trash_manager, "trash",
                                   side_effect=TrashUnavailableError("none")):
                controller.delete(f)

            summary = controller.summary()
            failed = summary["failed"]
            self.assertEqual(len(failed), 1)
            self.assertIn("hint", failed[0],
                "Failure entry must contain hint about --permanent")
            self.assertIn("--permanent", failed[0]["hint"])


# ---------------------------------------------------------------------------
# Test 6 — --permanent requires --delete
# ---------------------------------------------------------------------------

class TestPermanentRequiresDelete(unittest.TestCase):
    """--permanent without --delete must be rejected cleanly."""

    def test_permanent_without_delete_exits_2(self):
        """--permanent without --delete is an invalid argument combination: exit 2."""
        import subprocess
        cwd = str(Path(__file__).parent.parent)
        result = subprocess.run(
            [sys.executable, "main.py", "duplicates", ".", "--permanent"],
            capture_output=True, text=True, cwd=cwd,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--permanent requires --delete", result.stderr)

    def test_permanent_with_delete_accepted(self):
        """--delete --permanent must be accepted (not rejected at parse time)."""
        with tempfile.TemporaryDirectory() as tmp:
            import subprocess
            cwd = str(Path(__file__).parent.parent)
            result = subprocess.run(
                [sys.executable, "main.py", "duplicates", tmp,
                 "--delete", "--permanent"],
                capture_output=True, text=True, cwd=cwd,
            )
            # Should succeed (no duplicates → no deletion) or find no dupes
            self.assertNotIn("--permanent requires --delete", result.stderr)


# ---------------------------------------------------------------------------
# Test 7 — Dry-run unchanged
# ---------------------------------------------------------------------------

class TestDryRunUnchangedV17(unittest.TestCase):
    """Dry-run must still produce zero mutations in v1.7.0."""

    def test_dry_run_no_mutation_trash_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_dupes(base)
            files_before = sorted(f.name for f in base.iterdir())

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH

            result = scan_duplicates(base, keep="first")
            controller = ActionController(
                dry_run=True, scan_root=base,
                delete_mode=DELETE_MODE_TRASH,
            )
            deletion = controller.execute_deletions(result["duplicates"])

            files_after = sorted(f.name for f in base.iterdir())
            self.assertEqual(files_before, files_after,
                "Dry-run must not touch any files")
            self.assertGreater(len(deletion["deleted"]), 0,
                "Dry-run must still report what would be deleted")


# ---------------------------------------------------------------------------
# Test 8 — Determinism unchanged
# ---------------------------------------------------------------------------

class TestDeterminismUnchangedV17(unittest.TestCase):
    """Trash mode must produce identical victim sets across runs."""

    def test_victim_selection_identical_across_trash_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for n in ["z.txt", "a.txt", "m.txt"]:
                (base / n).write_bytes(b"same determinism content")

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH

            victims_across_runs = []
            for _ in range(3):
                result = scan_duplicates(base, keep="first")
                controller = ActionController(
                    dry_run=True, scan_root=base,
                    delete_mode=DELETE_MODE_TRASH,
                )
                deletion = controller.execute_deletions(result["duplicates"])
                victims_across_runs.append(tuple(sorted(deletion["deleted"])))

            self.assertEqual(len(set(victims_across_runs)), 1,
                "Victim selection must be deterministic in trash mode")


# ---------------------------------------------------------------------------
# Test 9 — Parallel deletion safe with trash
# ---------------------------------------------------------------------------

class TestParallelTrashSafe(unittest.TestCase):
    """Trash mode is called from main thread — parallel hashing unaffected."""

    def test_parallel_hash_then_serial_trash(self):
        """Hashing is parallel; trashing is serial in main thread."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for g in range(5):
                content = f"parallel trash group {g:04d}".encode()
                for i in range(3):
                    (base / f"g{g:02d}_{i}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH

            result = scan_duplicates(base, keep="first", max_workers=4)
            self.assertEqual(result["total_duplicate_files"], 10)

            controller = ActionController(
                dry_run=False, scan_root=base,
                delete_mode=DELETE_MODE_TRASH,
            )
            deletion = controller.execute_deletions(result["duplicates"])

            self.assertEqual(len(deletion["deleted"]), 10)
            self.assertEqual(len(deletion["failed"]), 0)


# ---------------------------------------------------------------------------
# Test 10 — Idempotency with trash
# ---------------------------------------------------------------------------

class TestIdempotencyTrash(unittest.TestCase):
    """Already-trashed files must not crash on second run."""

    def test_second_trash_run_no_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same idempotency trash"
            (base / "a.txt").write_bytes(content)
            (base / "b.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController, DELETE_MODE_TRASH

            result = scan_duplicates(base, keep="first")

            # First run
            c1 = ActionController(dry_run=False, scan_root=base,
                                   delete_mode=DELETE_MODE_TRASH)
            d1 = c1.execute_deletions(result["duplicates"])
            self.assertEqual(len(d1["deleted"]), 1)
            self.assertEqual(len(d1["failed"]), 0)

            # Second run with same duplicate dict — file already gone
            c2 = ActionController(dry_run=False, scan_root=base,
                                   delete_mode=DELETE_MODE_TRASH)
            d2 = c2.execute_deletions(result["duplicates"])

            # TrashManager.trash() handles already-gone files as success
            self.assertEqual(len(d2["failed"]), 0,
                "Second run must not fail on already-trashed files")


# ---------------------------------------------------------------------------
# Test 11 — Audit log records delete_mode
# ---------------------------------------------------------------------------

class TestAuditLogDeleteMode(unittest.TestCase):
    """Audit log must record which delete mode was used."""

    def test_trash_mode_recorded_in_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "auditable.txt"
            f.write_bytes(b"audit mode test")

            from action_controller import ActionController, DELETE_MODE_TRASH

            controller = ActionController(dry_run=False, scan_root=base,
                                          delete_mode=DELETE_MODE_TRASH)
            controller.delete(f, size=15)

            log = controller.audit_log()
            self.assertEqual(len(log), 1)
            self.assertIn("delete_mode", log[0])
            self.assertEqual(log[0]["delete_mode"], DELETE_MODE_TRASH)

    def test_permanent_mode_recorded_in_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "permanent.txt"
            f.write_bytes(b"permanent delete test")

            from action_controller import ActionController, DELETE_MODE_PERMANENT

            controller = ActionController(dry_run=False, scan_root=base,
                                          delete_mode=DELETE_MODE_PERMANENT)
            controller.delete(f, size=21)

            log = controller.audit_log()
            self.assertEqual(len(log), 1)
            self.assertEqual(log[0]["delete_mode"], DELETE_MODE_PERMANENT)


if __name__ == "__main__":
    unittest.main()
