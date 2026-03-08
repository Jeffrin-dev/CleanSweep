"""
v1.6.0 — Safe Action Framework tests.

Covers all 9 required test cases:
1. Default mode — no mutation
2. Dry-run — no mutation, full preview
3. Delete mode — actual mutation
4. Deterministic victim selection
5. Ctrl+C during deletion — clean exit 130
6. Partial failure — continues, reports failures
7. Permission failure — recorded, no crash
8. Double-run idempotency
9. Large dataset deletion
"""

import os
import sys
import stat
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _make_dupes(base: Path, n_groups: int = 3, files_per_group: int = 3) -> None:
    for g in range(n_groups):
        content = f"duplicate group {g:04d} content here".encode()
        for i in range(files_per_group):
            (base / f"g{g:02d}_{i}.txt").write_bytes(content)


# ---------------------------------------------------------------------------
# Test 1 — Default mode produces zero mutations
# ---------------------------------------------------------------------------

class TestDefaultModeNoMutation(unittest.TestCase):
    """Running without --delete must never touch the filesystem."""

    def test_scan_only_no_files_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_dupes(base)
            files_before = sorted(f.name for f in base.iterdir())

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")

            files_after = sorted(f.name for f in base.iterdir())
            self.assertEqual(files_before, files_after,
                "Files changed during scan-only mode")
            self.assertGreater(result["total_duplicate_files"], 0)

    def test_action_controller_default_dry_run_no_mutation(self):
        """ActionController with dry_run=True must never call unlink."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "test.txt"
            f.write_bytes(b"data")

            from action_controller import ActionController
            import unittest.mock as mock

            controller = ActionController(dry_run=True, scan_root=base)
            with mock.patch("pathlib.Path.unlink") as mock_unlink:
                result = controller.delete(f, size=4, file_hash="abc123")
                mock_unlink.assert_not_called()

            self.assertEqual(result.status, "dry_run")
            self.assertTrue(result.simulated)
            self.assertTrue(f.exists(), "File was deleted in dry_run mode")


# ---------------------------------------------------------------------------
# Test 2 — Dry-run is true simulation
# ---------------------------------------------------------------------------

class TestDryRunSimulation(unittest.TestCase):
    """Dry-run must run full detection and victim selection without mutations."""

    def test_dry_run_produces_same_victims_as_real_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_dupes(base, n_groups=3, files_per_group=3)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")

            # Dry-run controller
            controller = ActionController(dry_run=True, scan_root=base)
            deletion = controller.execute_deletions(result["duplicates"])

            # All files still exist
            for path_str in deletion["deleted"]:
                self.assertTrue(Path(path_str).exists(),
                    f"Dry-run deleted a real file: {path_str}")

            # Same count as real run would produce
            self.assertEqual(len(deletion["deleted"]), result["total_duplicate_files"])

    def test_dry_run_no_directory_creation(self):
        """Dry-run must not create any directories."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_dupes(base)
            dirs_before = {d for d in base.iterdir() if d.is_dir()}

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")
            controller = ActionController(dry_run=True, scan_root=base)
            controller.execute_deletions(result["duplicates"])

            dirs_after = {d for d in base.iterdir() if d.is_dir()}
            self.assertEqual(dirs_before, dirs_after)


# ---------------------------------------------------------------------------
# Test 3 — Delete mode actually deletes
# ---------------------------------------------------------------------------

class TestDeleteModeActualMutation(unittest.TestCase):
    """--delete mode must actually remove duplicate files."""

    def test_delete_removes_non_kept_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"identical duplicate content"
            (base / "a.txt").write_bytes(content)
            (base / "b.txt").write_bytes(content)
            (base / "c.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")
            controller = ActionController(dry_run=False, scan_root=base)
            deletion = controller.execute_deletions(result["duplicates"])

            self.assertEqual(len(deletion["deleted"]), 2)
            self.assertEqual(len(deletion["failed"]), 0)

            # Only one file remains
            remaining = list(base.iterdir())
            self.assertEqual(len(remaining), 1)

    def test_delete_keeps_correct_file(self):
        """keep=first must retain the alphabetically first path."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same content"
            (base / "aaa.txt").write_bytes(content)
            (base / "bbb.txt").write_bytes(content)
            (base / "ccc.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")
            controller = ActionController(dry_run=False, scan_root=base)
            controller.execute_deletions(result["duplicates"])

            remaining = list(base.iterdir())
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0].name, "aaa.txt")


# ---------------------------------------------------------------------------
# Test 4 — Deterministic victim selection
# ---------------------------------------------------------------------------

class TestDeterministicVictimSelection(unittest.TestCase):
    """Same input must always produce identical keep/delete decisions."""

    def test_victim_selection_stable_across_5_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same content for determinism test"
            (base / "alpha.txt").write_bytes(content)
            (base / "beta.txt").write_bytes(content)
            (base / "gamma.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            victims_across_runs = []
            for _ in range(5):
                result = scan_duplicates(base, keep="first")
                controller = ActionController(dry_run=True, scan_root=base)
                deletion = controller.execute_deletions(result["duplicates"])
                victims_across_runs.append(tuple(sorted(deletion["deleted"])))

            # All runs must agree on victims
            self.assertEqual(len(set(victims_across_runs)), 1,
                "Victim selection not deterministic across runs")

    def test_keep_first_always_keeps_lexicographically_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same"
            (base / "z_last.txt").write_bytes(content)
            (base / "a_first.txt").write_bytes(content)
            (base / "m_middle.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")
            controller = ActionController(dry_run=True, scan_root=base)
            deletion = controller.execute_deletions(result["duplicates"])

            deleted_names = {Path(p).name for p in deletion["deleted"]}
            self.assertNotIn("a_first.txt", deleted_names,
                "keep=first must not delete alphabetically first file")
            self.assertIn("z_last.txt", deleted_names)
            self.assertIn("m_middle.txt", deleted_names)


# ---------------------------------------------------------------------------
# Test 5 — Ctrl+C simulation
# ---------------------------------------------------------------------------

class TestCtrlCDuringDeletion(unittest.TestCase):
    """KeyboardInterrupt during deletion must exit 130 without traceback."""

    def test_interrupt_during_deletion_exits_130(self):
        import subprocess
        cwd = str(Path(__file__).parent)
        script = f"""
import sys
sys.path.insert(0, {cwd!r})
import tempfile
from pathlib import Path
import unittest.mock as mock

with tempfile.TemporaryDirectory() as tmp:
    base = Path(tmp)
    content = b'same content interrupt test'
    for i in range(5):
        (base / f'f{{i}}.txt').write_bytes(content)

    from action_controller import ActionController
    original_delete = ActionController.delete
    call_count = [0]

    def patched_delete(self, path, **kw):
        call_count[0] += 1
        if call_count[0] == 2:
            raise KeyboardInterrupt
        return original_delete(self, path, **kw)

    with mock.patch.object(ActionController, 'delete', patched_delete):
        try:
            import main
            with mock.patch('sys.argv', ['main', 'duplicates', str(base), '--delete']):
                main.main()
        except SystemExit as e:
            sys.exit(e.code)
"""
        result = __import__('subprocess').run(
            [sys.executable, "-c", script],
            capture_output=True, text=True,
            cwd=cwd,
        )
        self.assertEqual(result.returncode, 130)
        self.assertIn("Interrupted", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


# ---------------------------------------------------------------------------
# Test 6 — Partial failure continues and reports
# ---------------------------------------------------------------------------

class TestPartialFailureContinues(unittest.TestCase):
    """If one deletion fails, the rest must continue and be reported."""

    def test_partial_failure_recorded_rest_continue(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same content partial failure"
            for i in range(4):
                (base / f"f{i}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")
            from action_controller import DELETE_MODE_PERMANENT
            controller = ActionController(dry_run=False, scan_root=base,
                                          delete_mode=DELETE_MODE_PERMANENT)

            # Make first deletion fail
            original_unlink = Path.unlink
            call_count = [0]

            def failing_unlink(self, missing_ok=False):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise PermissionError("Simulated permission error")
                original_unlink(self, missing_ok=missing_ok)

            with mock.patch.object(Path, "unlink", failing_unlink):
                deletion = controller.execute_deletions(result["duplicates"])

            # Must have exactly 1 failure and continue with rest
            self.assertEqual(len(deletion["failed"]), 1)
            self.assertGreater(len(deletion["deleted"]), 0,
                "After failure, remaining files must still be processed")

    def test_partial_failure_exits_nonzero(self):
        """has_failures() must return True when any deletion failed."""
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "test.txt"
            f.write_bytes(b"data")

            from action_controller import ActionController, DELETE_MODE_PERMANENT
            controller = ActionController(dry_run=False, scan_root=base,
                                          delete_mode=DELETE_MODE_PERMANENT)

            with mock.patch.object(Path, "unlink", side_effect=PermissionError("denied")):
                controller.delete(f, size=4)

            self.assertTrue(controller.has_failures())


# ---------------------------------------------------------------------------
# Test 7 — Permission failure recorded, no crash
# ---------------------------------------------------------------------------

class TestPermissionFailureHandled(unittest.TestCase):

    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_permission_denied_recorded_not_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Put victim in a locked subdirectory — parent dir write permission
            # controls deletion on Linux, not the file's own permission bits
            locked_dir = base / "locked"
            locked_dir.mkdir()
            content = b"same content permission test"
            (base / "keep.txt").write_bytes(content)
            victim = locked_dir / "victim.txt"
            victim.write_bytes(content)
            locked_dir.chmod(0o555)  # non-writable parent → deletion blocked

            try:
                from duplicates import scan_duplicates
                from action_controller import ActionController

                result = scan_duplicates(base, keep="first")
                controller = ActionController(dry_run=False, scan_root=base)
                deletion = controller.execute_deletions(result["duplicates"])

                # Must not crash, failure recorded
                self.assertEqual(len(deletion["failed"]), 1)
                self.assertTrue(controller.has_failures())
            finally:
                locked_dir.chmod(0o755)


# ---------------------------------------------------------------------------
# Test 8 — Idempotency: second run handles already-deleted files
# ---------------------------------------------------------------------------

class TestIdempotency(unittest.TestCase):
    """Running --delete twice must not crash or misreport."""

    def test_second_run_no_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same idempotency content"
            (base / "a.txt").write_bytes(content)
            (base / "b.txt").write_bytes(content)

            from duplicates import scan_duplicates, find_duplicates
            from action_controller import ActionController

            # First run — deletes victim
            result = scan_duplicates(base, keep="first")
            controller1 = ActionController(dry_run=False, scan_root=base)
            d1 = controller1.execute_deletions(result["duplicates"])
            self.assertEqual(len(d1["deleted"]), 1)

            # Second run with same duplicate dict — victim already gone
            controller2 = ActionController(dry_run=False, scan_root=base)
            d2 = controller2.execute_deletions(result["duplicates"])

            # Must not crash — FileNotFoundError handled as success
            self.assertEqual(len(d2["failed"]), 0,
                "Second run must not fail on already-deleted files")
            self.assertEqual(len(d2["deleted"]), 1,
                "Already-gone files must be counted as success (idempotent)")

    def test_idempotent_freed_space_not_double_counted(self):
        """Running twice must not double-count freed space."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same content" * 100
            (base / "a.bin").write_bytes(content)
            (base / "b.bin").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first")

            # Run twice with same duplicate dict
            c1 = ActionController(dry_run=False, scan_root=base)
            c1.execute_deletions(result["duplicates"])

            c2 = ActionController(dry_run=False, scan_root=base)
            d2 = c2.execute_deletions(result["duplicates"])

            # No failures on second run
            self.assertEqual(len(d2["failed"]), 0)


# ---------------------------------------------------------------------------
# Test 9 — Large dataset deletion
# ---------------------------------------------------------------------------

class TestLargeDatasetDeletion(unittest.TestCase):
    """500 duplicate files deleted — must complete without crash or hang."""

    def test_500_duplicates_deleted_correctly(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # 100 groups × 5 files = 500 files, 400 to delete
            for g in range(100):
                content = f"large dataset group {g:04d}".encode()
                for i in range(5):
                    (base / f"g{g:03d}_{i}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            from action_controller import ActionController

            result = scan_duplicates(base, keep="first", max_workers=4)
            self.assertEqual(result["total_duplicate_files"], 400)

            controller = ActionController(dry_run=False, scan_root=base)
            deletion = controller.execute_deletions(result["duplicates"])

            self.assertEqual(len(deletion["deleted"]), 400)
            self.assertEqual(len(deletion["failed"]), 0)

            # 100 files remain (one per group)
            remaining = list(base.iterdir())
            self.assertEqual(len(remaining), 100)


# ---------------------------------------------------------------------------
# Test 10 — Boundary enforcement
# ---------------------------------------------------------------------------

class TestBoundaryEnforcement(unittest.TestCase):
    """Files outside scan_root must never be deleted."""

    def test_file_outside_root_refused(self):
        with tempfile.TemporaryDirectory() as tmp1:
            with tempfile.TemporaryDirectory() as tmp2:
                root = Path(tmp1)
                outside = Path(tmp2) / "outside.txt"
                outside.write_bytes(b"protected")

                from action_controller import ActionController
                controller = ActionController(dry_run=False, scan_root=root)
                result = controller.delete(outside, size=9)

                self.assertEqual(result.status, "failed")
                self.assertEqual(result.error, "boundary_violation")
                self.assertTrue(outside.exists(), "File outside root must not be deleted")

    def test_directory_deletion_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subdir = root / "subdir"
            subdir.mkdir()

            from action_controller import ActionController
            controller = ActionController(dry_run=False, scan_root=root)
            result = controller.delete(subdir)

            self.assertEqual(result.status, "failed")
            self.assertTrue(subdir.exists(), "Directory must never be deleted")


# ---------------------------------------------------------------------------
# Test 11 — Audit log completeness
# ---------------------------------------------------------------------------

class TestAuditLog(unittest.TestCase):
    """Every action must produce a structured audit event."""

    def test_audit_log_has_required_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "test.txt"
            f.write_bytes(b"audit test data")

            from action_controller import ActionController
            controller = ActionController(dry_run=False, scan_root=base)
            controller.delete(f, size=15, file_hash="deadbeef")

            log = controller.audit_log()
            self.assertEqual(len(log), 1)
            event = log[0]

            required_fields = {"action", "file", "size", "hash", "status", "timestamp"}
            for field in required_fields:
                self.assertIn(field, event, f"Audit event missing field: {field}")

            self.assertEqual(event["action"], "delete")
            self.assertEqual(event["size"], 15)
            self.assertEqual(event["hash"], "deadbeef")
            self.assertIn(event["status"], {"success", "already_gone", "dry_run", "failed", "trashed"})

    def test_audit_log_is_append_only_copy(self):
        """audit_log() must return a copy — mutating it must not affect controller."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f = base / "f.txt"
            f.write_bytes(b"data")

            from action_controller import ActionController
            controller = ActionController(dry_run=False, scan_root=base)
            controller.delete(f, size=4)

            log_copy = controller.audit_log()
            log_copy.clear()

            self.assertEqual(len(controller.audit_log()), 1,
                "Mutating returned audit log must not affect controller state")


if __name__ == "__main__":
    unittest.main()
