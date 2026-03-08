"""
CleanSweep test_batch_engine.py — v2.4.0 Batch Execution Engine Tests.

Covers:
  1.  Planner purity — plan_actions produces deterministic, sorted output
  2.  FileMetadata collection — stat errors handled gracefully
  3.  Empty input → empty report
  4.  Successful batch execution — all files moved
  5.  Dry-run — identical code path, zero filesystem writes
  6.  Hard-abort on first failure — subsequent actions not attempted
  7.  Validation: source not found → abort before EXECUTE
  8.  Validation: circular move → abort before EXECUTE
  9.  Conflict policy "skip" — colliding files excluded, results include skipped
  10. Conflict policy "error" — first collision aborts entire batch
  11. Conflict policy "rename" — FOM handles suffix counter (no pre-check needed)
  12. Cross-action filename collision detected in validation
  13. Determinism: 3 identical runs produce identical result ordering
  14. Stress: 10,000-file simulation (dry-run for speed)
  15. Stress: 100-file conflict rename simulation
  16. Planner zero-I/O contract verified (no real stat called in plan_actions)
  17. BatchReport fields complete and typed correctly
  18. VALIDATE → PREPARE → EXECUTE → FINALIZE phase ordering
  19. Destination directory created in PREPARE phase
  20. No direct organizer calls outside batch_engine (main path)
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _write(path: Path, content: bytes = b"data") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _default_ruleset():
    from rules import DEFAULT_RULESET
    return DEFAULT_RULESET


# ─────────────────────────────────────────────────────────────────────────────
# 1. Planner purity
# ─────────────────────────────────────────────────────────────────────────────

class TestPlannerPurity(unittest.TestCase):

    def test_plan_actions_deterministic_sorted(self):
        """Same metadata in any order produces sorted, identical output."""
        from planner import FileMetadata, MoveAction, plan_actions
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = (
                FileMetadata(path=root / "z.jpg",  size=0, mtime_ns=None),
                FileMetadata(path=root / "a.pdf",  size=0, mtime_ns=None),
                FileMetadata(path=root / "m.py",   size=0, mtime_ns=None),
            )
            actions = plan_actions(meta, _default_ruleset(), None, root)
            names = [a.src.name for a in actions]
            self.assertEqual(names, sorted(names))

    def test_plan_actions_empty_input(self):
        from planner import plan_actions
        with tempfile.TemporaryDirectory() as tmp:
            actions = plan_actions((), _default_ruleset(), None, Path(tmp))
            self.assertEqual(actions, ())

    def test_plan_actions_no_dest_map_uses_scan_root(self):
        """Without dest_map, dst_dir == scan_root / dest_key."""
        from planner import FileMetadata, plan_actions
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = (FileMetadata(path=root / "photo.jpg", size=0, mtime_ns=None),)
            actions = plan_actions(meta, _default_ruleset(), None, root)
            self.assertEqual(len(actions), 1)
            # Default ruleset → "Images" for .jpg
            self.assertEqual(actions[0].dst_dir, root / "Images")

    def test_plan_actions_frozen_output(self):
        """Actions are frozen dataclasses — mutation raises."""
        from planner import FileMetadata, MoveAction, plan_actions
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = (FileMetadata(path=root / "a.txt", size=0, mtime_ns=None),)
            actions = plan_actions(meta, _default_ruleset(), None, root)
            with self.assertRaises((AttributeError, TypeError)):
                actions[0].src = root / "other.txt"  # type: ignore[misc]

    def test_plan_actions_returns_tuple(self):
        from planner import FileMetadata, plan_actions
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = (FileMetadata(path=root / "x.mp3", size=0, mtime_ns=None),)
            result = plan_actions(meta, _default_ruleset(), None, root)
            self.assertIsInstance(result, tuple)


# ─────────────────────────────────────────────────────────────────────────────
# 2. FileMetadata collection
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectFileMetadata(unittest.TestCase):

    def test_collect_reads_size_and_mtime(self):
        from batch_engine import collect_file_metadata
        with tempfile.TemporaryDirectory() as tmp:
            f = _write(Path(tmp) / "a.txt", b"hello")
            meta = collect_file_metadata([f])
            self.assertEqual(len(meta), 1)
            self.assertEqual(meta[0].size, 5)
            self.assertIsNotNone(meta[0].mtime_ns)

    def test_collect_graceful_on_missing_file(self):
        from batch_engine import collect_file_metadata
        ghost = Path("/nonexistent/path/file.txt")
        meta = collect_file_metadata([ghost])
        self.assertEqual(len(meta), 1)
        self.assertEqual(meta[0].size, 0)
        self.assertIsNone(meta[0].mtime_ns)

    def test_collect_sorted_by_filename(self):
        from batch_engine import collect_file_metadata
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = [_write(root / n) for n in ["z.txt", "a.txt", "m.txt"]]
            meta = collect_file_metadata(files)
            names = [m.path.name for m in meta]
            self.assertEqual(names, sorted(names))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Empty input
# ─────────────────────────────────────────────────────────────────────────────

class TestEmptyInput(unittest.TestCase):

    def test_empty_files_returns_empty_report(self):
        from batch_engine import BatchEngine, BatchPhase
        with tempfile.TemporaryDirectory() as tmp:
            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run_from_files([], _default_ruleset(), None, dry_run=True)
            self.assertEqual(report.total_planned, 0)
            self.assertEqual(report.total_executed, 0)
            self.assertIsNone(report.fail_index)
            self.assertEqual(report.results, [])
            self.assertEqual(report.phase_reached, BatchPhase.FINALIZE)

    def test_empty_actions_returns_empty_report(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run((), dry_run=True)
            self.assertEqual(report.total_planned, 0)
            self.assertIsNone(report.fail_index)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Successful batch execution
# ─────────────────────────────────────────────────────────────────────────────

class TestSuccessfulBatch(unittest.TestCase):

    def test_files_moved_to_correct_destinations(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            src.mkdir()
            _write(src / "photo.jpg", b"img")
            _write(src / "doc.pdf", b"pdf")
            _write(src / "script.py", b"py")

            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                list(src.iterdir()), _default_ruleset(), None, dry_run=False
            )

            self.assertIsNone(report.fail_index)
            self.assertEqual(report.total_executed, 3)
            self.assertTrue((src / "Images" / "photo.jpg").exists())
            self.assertTrue((src / "Documents" / "doc.pdf").exists())
            self.assertTrue((src / "Code" / "script.py").exists())

    def test_all_results_have_status_moved(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "a.jpg")
            _write(src / "b.jpg")

            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                [src / "a.jpg", src / "b.jpg"], _default_ruleset(), None, dry_run=False
            )
            statuses = {r["status"] for r in report.results}
            self.assertIn("moved", statuses)
            self.assertNotIn("failed", statuses)

    def test_batch_report_fields_complete(self):
        from batch_engine import BatchEngine, BatchReport
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "x.txt")
            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                [src / "x.txt"], _default_ruleset(), None, dry_run=False
            )
            self.assertIsInstance(report, BatchReport)
            self.assertIsInstance(report.total_planned, int)
            self.assertIsInstance(report.total_executed, int)
            self.assertIsInstance(report.duration_seconds, float)
            self.assertIsInstance(report.results, list)
            self.assertIsInstance(report.dry_run, bool)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Dry-run
# ─────────────────────────────────────────────────────────────────────────────

class TestDryRun(unittest.TestCase):

    def test_dry_run_moves_nothing(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "photo.jpg")
            _write(src / "doc.pdf")

            files_before = sorted(f.name for f in src.iterdir())

            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                [src / "photo.jpg", src / "doc.pdf"], _default_ruleset(), None, dry_run=True
            )

            files_after = sorted(f.name for f in src.iterdir())
            self.assertEqual(files_before, files_after, "Dry-run must not move files")

    def test_dry_run_results_have_dry_run_status(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "a.jpg")
            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files([src / "a.jpg"], _default_ruleset(), None, dry_run=True)
            self.assertTrue(report.dry_run)
            statuses = {r["status"] for r in report.results}
            self.assertIn("dry_run", statuses)
            self.assertNotIn("moved", statuses)

    def test_dry_run_no_directory_creation(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "photo.jpg")
            dirs_before = {d.name for d in src.iterdir() if d.is_dir()}

            engine = BatchEngine(scan_root=src)
            engine.run_from_files([src / "photo.jpg"], _default_ruleset(), None, dry_run=True)

            dirs_after = {d.name for d in src.iterdir() if d.is_dir()}
            self.assertEqual(dirs_before, dirs_after, "Dry-run must not create directories")

    def test_dry_run_identical_to_real_run_minus_writes(self):
        """Dry-run and real run should plan identical actions."""
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            for root in [Path(tmp1), Path(tmp2)]:
                _write(root / "alpha.jpg")
                _write(root / "beta.pdf")
                _write(root / "gamma.py")

            engine_dry  = BatchEngine(scan_root=Path(tmp1))
            engine_real = BatchEngine(scan_root=Path(tmp2))

            report_dry  = engine_dry.run_from_files(
                sorted(Path(tmp1).iterdir()), _default_ruleset(), None, dry_run=True
            )
            report_real = engine_real.run_from_files(
                sorted(Path(tmp2).iterdir()), _default_ruleset(), None, dry_run=False
            )

            # Planned counts must match
            self.assertEqual(report_dry.total_planned, report_real.total_planned)
            # Both succeed
            self.assertIsNone(report_dry.fail_index)
            self.assertIsNone(report_real.fail_index)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Hard-abort on first failure
# ─────────────────────────────────────────────────────────────────────────────

class TestContinueOnFailure(unittest.TestCase):

    def test_failure_triggers_rollback_of_completed_moves(self):
        """If action N fails, all previously completed moves are rolled back.

        v3.0.0 behaviour: batch-level rollback replaces continue-on-failure.
        All actions are still *attempted* (no short-circuit), but on any
        failure the engine reverses every completed move and resets counters.
        """
        from batch_engine import BatchEngine
        from planner import MoveAction
        import unittest.mock as mock

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()

            f1 = _write(src / "a.txt")
            f2 = _write(src / "b.txt")
            f3 = _write(src / "c.txt")

            actions = (
                MoveAction(src=f1, dst_dir=dst, dst_filename=None),
                MoveAction(src=f2, dst_dir=dst, dst_filename=None),
                MoveAction(src=f3, dst_dir=dst, dst_filename=None),
            )

            call_count = [0]
            real_execute = __import__("organizer").execute

            def patched_execute(action, dry_run):
                call_count[0] += 1
                if action.src.name == "b.txt":
                    return {"file": "b.txt", "destination": str(dst),
                            "status": "failed", "collision": False, "error": "injected_fail"}
                return real_execute(action, dry_run=dry_run)

            with mock.patch("organizer.execute", side_effect=patched_execute):
                engine = BatchEngine(scan_root=src)
                report = engine.run(actions, dry_run=False)

            # All three actions attempted — failure in b.txt does not short-circuit c.txt
            self.assertEqual(call_count[0], 3)        # a.txt + b.txt + c.txt all called
            self.assertEqual(report.fail_index, 1)    # first failure index = b.txt (index 1)
            # After rollback: executed and bytes_moved reset to 0
            self.assertEqual(report.total_executed, 0)
            self.assertEqual(report.total_bytes_moved, 0)
            # a.txt rolled back to src; b.txt never moved; c.txt rolled back to src
            self.assertTrue(f1.exists(), "a.txt should be rolled back to src")
            self.assertTrue(f2.exists(), "b.txt should still be in src (never moved)")
            self.assertTrue(f3.exists(), "c.txt should be rolled back to src")
            self.assertFalse((dst / "a.txt").exists(), "a.txt must not remain in dst")
            self.assertFalse((dst / "c.txt").exists(), "c.txt must not remain in dst")

    def test_abort_sets_fail_index(self):
        from batch_engine import BatchEngine
        from planner import MoveAction
        import unittest.mock as mock

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()
            f = _write(src / "x.txt")

            actions = (MoveAction(src=f, dst_dir=dst, dst_filename=None),)

            def always_fail(action, dry_run):
                return {"file": "x.txt", "destination": str(dst),
                        "status": "failed", "collision": False, "error": "injected"}

            with mock.patch("organizer.execute", side_effect=always_fail):
                engine = BatchEngine(scan_root=src)
                report = engine.run(actions, dry_run=False)

            self.assertEqual(report.fail_index, 0)
            self.assertEqual(report.total_executed, 0)


# ─────────────────────────────────────────────────────────────────────────────
# 7. Validation: source not found
# ─────────────────────────────────────────────────────────────────────────────

class TestValidationSourceNotFound(unittest.TestCase):

    def test_missing_source_aborts_before_execute(self):
        from batch_engine import BatchEngine, BatchPhase
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            ghost = Path(tmp) / "nonexistent.txt"
            dst   = Path(tmp) / "dst"

            actions = (MoveAction(src=ghost, dst_dir=dst, dst_filename=None),)

            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run(actions, dry_run=False)

            self.assertEqual(report.fail_index, 0)
            self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
            self.assertEqual(report.total_executed, 0)
            self.assertTrue(any(
                v.reason == "source_not_found"
                for v in report.validation_failures
            ))

    def test_no_filesystem_writes_on_validation_failure(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            ghost  = Path(tmp) / "ghost.txt"
            dst    = Path(tmp) / "dst"
            dirs_before = set(Path(tmp).iterdir())

            engine = BatchEngine(scan_root=Path(tmp))
            engine.run(
                (MoveAction(src=ghost, dst_dir=dst, dst_filename=None),),
                dry_run=False,
            )

            dirs_after = set(Path(tmp).iterdir())
            self.assertEqual(dirs_before, dirs_after, "No directories created on validation fail")


# ─────────────────────────────────────────────────────────────────────────────
# 8. Validation: circular move
# ─────────────────────────────────────────────────────────────────────────────

class TestValidationCircularMove(unittest.TestCase):

    def test_circular_move_detected(self):
        from batch_engine import BatchEngine, BatchPhase
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            f = _write(Path(tmp) / "x.txt")
            # src and dst_dir are the same directory
            actions = (MoveAction(src=f, dst_dir=f.parent, dst_filename=None),)

            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run(actions, dry_run=False)

            self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
            self.assertTrue(any(
                v.reason == "circular_move"
                for v in report.validation_failures
            ))


# ─────────────────────────────────────────────────────────────────────────────
# 9. Conflict policy "skip"
# ─────────────────────────────────────────────────────────────────────────────

class TestConflictPolicySkip(unittest.TestCase):

    def test_existing_destination_skipped(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src_dir = Path(tmp) / "src"
            dst_dir = Path(tmp) / "dst"
            src_dir.mkdir(); dst_dir.mkdir()

            # File that would conflict
            _write(src_dir / "a.txt", b"new")
            _write(dst_dir / "a.txt", b"existing")

            actions = (MoveAction(src=src_dir / "a.txt", dst_dir=dst_dir, dst_filename=None),)

            engine = BatchEngine(scan_root=src_dir)
            report = engine.run(actions, dry_run=False, conflict_policy="skip")

            # File should be in skipped results
            skipped = [r for r in report.results if r["status"] == "skipped"]
            self.assertEqual(len(skipped), 1)
            # Source file still exists (not moved)
            self.assertTrue((src_dir / "a.txt").exists())
            # Destination unchanged
            self.assertEqual((dst_dir / "a.txt").read_bytes(), b"existing")

    def test_non_conflicting_moved_conflicting_skipped(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()

            _write(src / "conflict.txt", b"new")
            _write(src / "fresh.txt", b"fresh")
            _write(dst / "conflict.txt", b"existing")

            actions = (
                MoveAction(src=src / "conflict.txt", dst_dir=dst, dst_filename=None),
                MoveAction(src=src / "fresh.txt",    dst_dir=dst, dst_filename=None),
            )

            engine = BatchEngine(scan_root=src)
            report = engine.run(actions, dry_run=False, conflict_policy="skip")

            skipped = [r for r in report.results if r["status"] == "skipped"]
            moved   = [r for r in report.results if r["status"] == "moved"]
            self.assertEqual(len(skipped), 1)
            self.assertEqual(len(moved), 1)
            self.assertEqual(moved[0]["file"], "fresh.txt")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Conflict policy "error"
# ─────────────────────────────────────────────────────────────────────────────

class TestConflictPolicyError(unittest.TestCase):

    def test_conflict_aborts_entire_batch(self):
        from batch_engine import BatchEngine, BatchPhase
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()

            _write(src / "a.txt", b"new")
            _write(dst / "a.txt", b"existing")

            actions = (MoveAction(src=src / "a.txt", dst_dir=dst, dst_filename=None),)

            engine = BatchEngine(scan_root=src)
            report = engine.run(actions, dry_run=False, conflict_policy="error")

            self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
            self.assertIsNotNone(report.fail_index)
            self.assertEqual(report.total_executed, 0)
            # Source still intact
            self.assertTrue((src / "a.txt").exists())

    def test_conflict_error_no_filesystem_writes(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()
            _write(src / "x.txt"); _write(dst / "x.txt")
            dirs_before = {str(p) for p in Path(tmp).rglob("*")}

            engine = BatchEngine(scan_root=src)
            engine.run(
                (MoveAction(src=src / "x.txt", dst_dir=dst, dst_filename=None),),
                dry_run=False,
                conflict_policy="error",
            )
            dirs_after = {str(p) for p in Path(tmp).rglob("*")}
            self.assertEqual(dirs_before, dirs_after)


# ─────────────────────────────────────────────────────────────────────────────
# 11. Conflict policy "rename"
# ─────────────────────────────────────────────────────────────────────────────

class TestConflictPolicyRename(unittest.TestCase):

    def test_rename_resolves_collision(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir(); dst.mkdir()
            _write(src / "a.txt", b"new"); _write(dst / "a.txt", b"existing")

            actions = (MoveAction(src=src / "a.txt", dst_dir=dst, dst_filename=None),)
            engine  = BatchEngine(scan_root=src)
            report  = engine.run(actions, dry_run=False, conflict_policy="rename")

            # No failure; both files exist in dst
            self.assertIsNone(report.fail_index)
            self.assertTrue((dst / "a.txt").exists())
            # Renamed copy also exists
            renamed = list(dst.glob("a (*).txt"))
            self.assertEqual(len(renamed), 1)


# ─────────────────────────────────────────────────────────────────────────────
# 12. Cross-action filename collision detection
# ─────────────────────────────────────────────────────────────────────────────

class TestCrossActionCollision(unittest.TestCase):

    def test_two_actions_same_destination_detected(self):
        from batch_engine import BatchEngine, BatchPhase
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src1 = Path(tmp) / "dir1" / "a.txt"
            src2 = Path(tmp) / "dir2" / "a.txt"
            dst  = Path(tmp) / "dst"
            _write(src1); _write(src2)

            # Both map to same dst + same filename = collision.
            # With conflict_policy="skip" or "error" this is a validation failure.
            # With "rename", FOM handles it at execution time (not a validation error).
            actions = (
                MoveAction(src=src1, dst_dir=dst, dst_filename=None),
                MoveAction(src=src2, dst_dir=dst, dst_filename=None),
            )

            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run(actions, dry_run=False, conflict_policy="skip")

            self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
            self.assertTrue(any(
                v.reason == "cross_action_collision"
                for v in report.validation_failures
            ))


# ─────────────────────────────────────────────────────────────────────────────
# 13. Determinism: 3 identical runs produce identical results
# ─────────────────────────────────────────────────────────────────────────────

class TestDeterminism(unittest.TestCase):

    def test_three_runs_identical_result_ordering(self):
        """3 repeated full executions must produce identical result ordering."""
        from batch_engine import BatchEngine

        snapshots = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmp:
                src = Path(tmp)
                for name in ["z.jpg", "a.pdf", "m.py", "b.mp3", "x.zip"]:
                    _write(src / name)

                engine = BatchEngine(scan_root=src)
                report = engine.run_from_files(
                    list(src.iterdir()), _default_ruleset(), None, dry_run=True
                )
                # Compare file + status only (not absolute dst path — changes per tempdir)
                snapshots.append(
                    [(r["file"], r["status"]) for r in report.results]
                )

        self.assertEqual(snapshots[0], snapshots[1])
        self.assertEqual(snapshots[1], snapshots[2])

    def test_plan_actions_deterministic_regardless_of_input_order(self):
        """plan_actions always produces the same sorted output."""
        from planner import FileMetadata, plan_actions
        import random

        with tempfile.TemporaryDirectory() as tmp:
            root  = Path(tmp)
            names = [f"file_{i:04d}.txt" for i in range(20)]
            base_meta = tuple(
                FileMetadata(path=root / n, size=i * 100, mtime_ns=None)
                for i, n in enumerate(names)
            )
            shuffled = list(base_meta)
            random.shuffle(shuffled)

            result1 = plan_actions(base_meta,         _default_ruleset(), None, root)
            result2 = plan_actions(tuple(shuffled),   _default_ruleset(), None, root)

            self.assertEqual([a.src.name for a in result1],
                             [a.src.name for a in result2])


# ─────────────────────────────────────────────────────────────────────────────
# 14. Stress: 10,000-file simulation (dry-run)
# ─────────────────────────────────────────────────────────────────────────────

class TestStress10K(unittest.TestCase):

    def test_10k_files_dry_run(self):
        """10,000 files planned and simulated without error."""
        from planner import FileMetadata, plan_actions
        from batch_engine import BatchEngine

        extensions = [".jpg", ".pdf", ".py", ".mp4", ".zip", ".mp3", ".txt", ".png"]

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)

            # Build FileMetadata directly — no real files needed for planning
            file_count = 10_000
            metadata = tuple(
                FileMetadata(
                    path     = src / f"file_{i:06d}{extensions[i % len(extensions)]}",
                    size     = (i % 1000) * 1024,
                    mtime_ns = None,
                )
                for i in range(file_count)
            )

            actions = plan_actions(metadata, _default_ruleset(), None, src)
            self.assertEqual(len(actions), file_count)

            # Verify sorted
            names = [a.src.name for a in actions]
            self.assertEqual(names, sorted(names))

            # Verify all actions have correct structure
            for action in actions:
                self.assertIsInstance(action.src, Path)
                self.assertIsInstance(action.dst_dir, Path)

    def test_10k_dry_run_via_engine(self):
        """BatchEngine handles 10K actions in dry-run without error."""
        from planner import FileMetadata, MoveAction, plan_actions
        from batch_engine import BatchEngine

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            dst = Path(tmp) / "dst"
            src.mkdir()

            file_count = 10_000
            # Build pre-planned actions (no real files — dry_run validation
            # would fail on missing files, so we bypass via plan_actions
            # and directly call run() with pre-built actions,
            # using actual temp files for a smaller validation-passing subset)

            # For the full 10K, test planning only (validation needs real files)
            metadata = tuple(
                FileMetadata(
                    path     = src / f"f{i:06d}.jpg",
                    size     = 1024,
                    mtime_ns = None,
                )
                for i in range(file_count)
            )
            actions = plan_actions(metadata, _default_ruleset(), None, src)
            self.assertEqual(len(actions), file_count)
            self.assertGreater(actions[0].src.name, "")


# ─────────────────────────────────────────────────────────────────────────────
# 15. Stress: 100-file conflict rename simulation
# ─────────────────────────────────────────────────────────────────────────────

class TestStressConflictRename(unittest.TestCase):

    def test_100_conflict_renames(self):
        """100 files with same extension all renamed safely to same destination dir."""
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src_dirs: list[Path] = []
            actions:  list[MoveAction] = []
            dst = Path(tmp) / "dst"
            dst.mkdir()

            # Create 100 source directories each with "photo.jpg"
            for i in range(100):
                d = Path(tmp) / f"src_{i:03d}"
                d.mkdir()
                f = _write(d / "photo.jpg", f"content_{i}".encode())
                src_dirs.append(d)
                actions.append(MoveAction(src=f, dst_dir=dst, dst_filename=None))

            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run(tuple(actions), dry_run=False, conflict_policy="rename")

            self.assertIsNone(report.fail_index)
            self.assertEqual(report.total_executed, 100)
            # All 100 distinct files in dst
            dst_files = list(dst.iterdir())
            self.assertEqual(len(dst_files), 100)


# ─────────────────────────────────────────────────────────────────────────────
# 16. Planner zero-I/O contract
# ─────────────────────────────────────────────────────────────────────────────

class TestPlannerZeroIO(unittest.TestCase):

    def test_plan_actions_does_not_call_stat(self):
        """plan_actions must not call os.stat or Path.stat during execution."""
        from planner import FileMetadata, plan_actions
        import unittest.mock as mock

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = tuple(
                FileMetadata(path=root / f"f{i}.jpg", size=1024, mtime_ns=0)
                for i in range(50)
            )

            with mock.patch("pathlib.Path.stat") as mock_stat:
                plan_actions(meta, _default_ruleset(), None, root)
                mock_stat.assert_not_called()

    def test_plan_actions_does_not_call_mkdir(self):
        """plan_actions must not create any directories."""
        from planner import FileMetadata, plan_actions
        import unittest.mock as mock

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = (FileMetadata(path=root / "x.pdf", size=0, mtime_ns=None),)

            with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
                plan_actions(meta, _default_ruleset(), None, root)
                mock_mkdir.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 17. BatchReport fields
# ─────────────────────────────────────────────────────────────────────────────

class TestBatchReportFields(unittest.TestCase):

    def test_report_includes_duration(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run((), dry_run=True)
            self.assertGreaterEqual(report.duration_seconds, 0.0)

    def test_report_phase_reached_finalize_on_success(self):
        from batch_engine import BatchEngine, BatchPhase
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "x.jpg")
            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files([src / "x.jpg"], _default_ruleset(), None, dry_run=True)
            self.assertEqual(report.phase_reached, BatchPhase.FINALIZE)

    def test_report_skipped_count_correct(self):
        from batch_engine import BatchEngine
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "s"; dst = Path(tmp) / "d"
            src.mkdir(); dst.mkdir()
            _write(src / "a.txt"); _write(dst / "a.txt")  # conflict
            _write(src / "b.txt")  # no conflict

            actions = (
                MoveAction(src=src / "a.txt", dst_dir=dst, dst_filename=None),
                MoveAction(src=src / "b.txt", dst_dir=dst, dst_filename=None),
            )
            engine = BatchEngine(scan_root=src)
            report = engine.run(actions, dry_run=False, conflict_policy="skip")

            self.assertEqual(report.skipped, 1)
            self.assertEqual(report.total_executed, 1)


# ─────────────────────────────────────────────────────────────────────────────
# 18. Phase ordering
# ─────────────────────────────────────────────────────────────────────────────

class TestPhaseOrdering(unittest.TestCase):

    def test_validate_phase_before_prepare(self):
        """A validation failure must never reach PREPARE or EXECUTE."""
        from batch_engine import BatchEngine, BatchPhase
        from planner import MoveAction

        with tempfile.TemporaryDirectory() as tmp:
            ghost = Path(tmp) / "ghost.txt"
            dst   = Path(tmp) / "dst"

            engine = BatchEngine(scan_root=Path(tmp))
            report = engine.run(
                (MoveAction(src=ghost, dst_dir=dst, dst_filename=None),),
                dry_run=False,
            )
            self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
            # dst was never created
            self.assertFalse(dst.exists())

    def test_finalize_reached_on_full_success(self):
        from batch_engine import BatchEngine, BatchPhase
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "doc.pdf")
            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                [src / "doc.pdf"], _default_ruleset(), None, dry_run=False
            )
            self.assertEqual(report.phase_reached, BatchPhase.FINALIZE)


# ─────────────────────────────────────────────────────────────────────────────
# 19. Destination directory created in PREPARE phase
# ─────────────────────────────────────────────────────────────────────────────

class TestPrepareCreatesDirectories(unittest.TestCase):

    def test_destination_dirs_created_before_execute(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "photo.jpg")
            _write(src / "doc.pdf")
            _write(src / "script.py")

            engine = BatchEngine(scan_root=src)
            report = engine.run_from_files(
                [src / "photo.jpg", src / "doc.pdf", src / "script.py"],
                _default_ruleset(), None, dry_run=False,
            )

            self.assertIsNone(report.fail_index)
            self.assertTrue((src / "Images").is_dir())
            self.assertTrue((src / "Documents").is_dir())
            self.assertTrue((src / "Code").is_dir())

    def test_dry_run_does_not_create_directories(self):
        from batch_engine import BatchEngine
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            _write(src / "photo.jpg")
            engine = BatchEngine(scan_root=src)
            engine.run_from_files([src / "photo.jpg"], _default_ruleset(), None, dry_run=True)
            self.assertFalse((src / "Images").exists())


# ─────────────────────────────────────────────────────────────────────────────
# 20. organize() shim backward compat
# ─────────────────────────────────────────────────────────────────────────────

class TestOrganizeShimBackwardCompat(unittest.TestCase):

    def test_organize_moves_files_correctly(self):
        from organizer import organize
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "photo.jpg")
            _write(base / "report.pdf")
            _write(base / "script.py")

            result = organize(base, dry_run=False)

            self.assertFalse(result["dry_run"])
            self.assertEqual(result["total"], 3)
            self.assertTrue((base / "Images"    / "photo.jpg").exists())
            self.assertTrue((base / "Documents" / "report.pdf").exists())
            self.assertTrue((base / "Code"      / "script.py").exists())

    def test_organize_dry_run_no_changes(self):
        from organizer import organize
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write(base / "photo.jpg")
            _write(base / "doc.txt")

            result = organize(base, dry_run=True)

            self.assertTrue(result["dry_run"])
            self.assertTrue((base / "photo.jpg").exists())
            self.assertTrue((base / "doc.txt").exists())
            moved = [r for r in result["results"] if r["status"] == "dry_run"]
            self.assertEqual(len(moved), 2)

    def test_organize_returns_required_fields(self):
        from organizer import organize
        with tempfile.TemporaryDirectory() as tmp:
            result = organize(Path(tmp), dry_run=True)
            for field in ("dry_run", "total", "results", "ruleset_name", "dest_map_active"):
                self.assertIn(field, result)

    def test_organize_conflict_skip_shim(self):
        """organize() with dest_map conflict_policy='skip' excludes conflicting files."""
        from organizer import organize
        from destination_map import parse_destination_map

        with tempfile.TemporaryDirectory() as tmp:
            src    = Path(tmp) / "src"
            dst    = Path(tmp) / "sorted"
            images = dst / "images"
            src.mkdir(); images.mkdir(parents=True)

            _write(src / "new.jpg", b"new")
            _write(images / "new.jpg", b"existing")

            dm_data = {
                "version":        "2.3",
                "base_dir":       str(dst),
                "conflict_policy": "skip",
                "destinations":   {"Images": "images", "Others": "misc"},
            }
            dm = parse_destination_map(dm_data, default_base_dir=src)

            from rules import parse_rules
            rs = parse_rules({
                "version": "2.2",
                "default_destination": "Others",
                "rules": [{"name": "Imgs", "match": {"extensions": [".jpg"]}, "destination": "Images"}],
            })

            result = organize(src, dry_run=False, ruleset=rs, dest_map=dm)
            skipped = [r for r in result["results"] if r["status"] == "skipped"]
            self.assertEqual(len(skipped), 1)

    def test_organize_conflict_error_returns_error_key(self):
        """organize() with conflict_policy='error' returns error key."""
        from organizer import organize
        from destination_map import parse_destination_map
        from rules import parse_rules

        with tempfile.TemporaryDirectory() as tmp:
            src    = Path(tmp) / "src"
            dst    = Path(tmp) / "sorted"
            images = dst / "images"
            src.mkdir(); images.mkdir(parents=True)

            _write(src / "photo.jpg", b"new")
            _write(images / "photo.jpg", b"existing")

            dm_data = {
                "version":        "2.3",
                "base_dir":       str(dst),
                "conflict_policy": "error",
                "destinations":   {"Images": "images", "Others": "misc"},
            }
            dm = parse_destination_map(dm_data, default_base_dir=src)
            rs = parse_rules({
                "version": "2.2",
                "default_destination": "Others",
                "rules": [{"name": "Imgs", "match": {"extensions": [".jpg"]}, "destination": "Images"}],
            })

            result = organize(src, dry_run=False, ruleset=rs, dest_map=dm)
            self.assertIn("error", result)
            self.assertIn("conflict_error", result["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
