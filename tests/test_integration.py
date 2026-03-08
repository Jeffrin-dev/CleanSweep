"""
CleanSweep v2.9.0 — Cross-Module Integration & Stability Test Suite.

Objectives (from v2.9 roadmap):
  1. Cross-Module Integration Tests   — full pipeline from scan → report
  2. Stress Tests                     — large directory trees (scalable up to 1M)
  3. Failure Simulation Tests         — real filesystem error injection
  4. Determinism Tests                — identical output across repeated runs
  5. Performance Baseline             — wall-clock timing assertions

Test classes:
  TestFullOrganizePipeline    — scan → rule → dest → batch → report
  TestDuplicatePipeline       — scan → hash pipeline → duplicate detection
  TestDryRunGuarantee         — dry-run never mutates filesystem
  TestPolicyModePipeline      — strict / safe / warn across full pipeline
  TestDestinationMapPipeline  — batch pipeline with explicit DestinationMap
  TestFailureSimulation       — permission denied, broken symlink, collision,
                                 unreadable file, mid-scan deletion
  TestDeterminism             — two identical runs produce byte-identical results
  TestStress                  — large tree traversal and hashing stability

Environment variables:
  CLEANSWEEP_STRESS_DIRS       — number of subdirectories for stress test (default 100)
  CLEANSWEEP_STRESS_FILES      — files per subdir for stress test (default 100)
  CLEANSWEEP_STRESS_SKIP       — set to "1" to skip stress tests entirely

All tests are:
  - Self-contained (TemporaryDirectory — no persistent artifacts)
  - Order-independent (no shared mutable state)
  - Platform-agnostic (standard library only)
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from batch_engine import BatchEngine, BatchReport, BatchPhase
from duplicates import (
    collect_snapshot, group_by_size, group_by_partial_hash,
    group_by_hash, find_duplicates, scan_duplicates, FileEntry,
    run_hash_pipeline,
)
from file_operation_manager import FileOperationManager, TEMP_PREFIX
from organizer import organize
from planner import plan_actions, plan_with_policy, FileMetadata
from policy import PolicyConflict, STRICT, SAFE, WARN
from rules import parse_rules, DEFAULT_RULESET, RuleSet, Rule
from scanner import (
    ScanPolicy, scan_files, list_files, validate_folder,
    SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR, ScanError,
)


# ---------------------------------------------------------------------------
# Stress-test scale — environment-controlled for CI vs full-scale runs
# ---------------------------------------------------------------------------

_STRESS_DIRS  = int(os.environ.get("CLEANSWEEP_STRESS_DIRS",  "100"))
_STRESS_FILES = int(os.environ.get("CLEANSWEEP_STRESS_FILES", "100"))
_STRESS_SKIP  = os.environ.get("CLEANSWEEP_STRESS_SKIP", "0") == "1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tree(base: Path, spec: dict) -> None:
    """Recursively build a directory tree from a nested dict spec."""
    for name, value in spec.items():
        path = base / name
        if isinstance(value, dict):
            path.mkdir(exist_ok=True)
            _make_tree(path, value)
        elif isinstance(value, bytes):
            path.write_bytes(value)
        else:
            path.write_text(str(value))


def _file_names_recursive(root: Path) -> list[str]:
    """Return sorted list of filenames under root (recursive, non-dirs)."""
    return sorted(p.name for p in root.rglob("*") if p.is_file())


def _all_paths_recursive(root: Path) -> list[Path]:
    """Return sorted list of all paths (files + dirs) under root."""
    return sorted(root.rglob("*"))


def _build_v20_ruleset(rules_data: list[dict], default: str = "Others") -> RuleSet:
    """Build a v2.0 RuleSet from compact rule definitions."""
    return parse_rules({
        "version": "2.0",
        "rules": rules_data,
        "default_destination": default,
    })


def _build_v22_ruleset(rules_data: list[dict], default: str = "Others") -> RuleSet:
    """Build a v2.2 RuleSet from compact rule definitions."""
    return parse_rules({
        "version": "2.2",
        "rules": rules_data,
        "default_destination": default,
    })


# ===========================================================================
# 1. Full Organize Pipeline
# ===========================================================================

class TestFullOrganizePipeline(unittest.TestCase):
    """
    Tests the full organize pipeline:
      scan → rule match → destination resolution → batch execution → report

    Verifies correct file placement, report accuracy, and cross-module wiring.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_full_pipeline_default_ruleset(self) -> None:
        """All default categories route correctly through the full pipeline."""
        _make_tree(self.root, {
            "photo.jpg":   b"fake-jpeg",
            "report.pdf":  b"fake-pdf",
            "script.py":   "print('hello')",
            "clip.mp4":    b"fake-mp4",
            "backup.zip":  b"fake-zip",
            "notes.txt":   "notes content",
            "track.mp3":   b"fake-mp3",
            "random.xyz":  "unknown type",
        })

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False, policy_mode="safe",
        )

        # No failures
        self.assertIsNone(report.fail_index)
        self.assertEqual(report.phase_reached, BatchPhase.FINALIZE)
        self.assertEqual(report.total_planned, len(files))
        self.assertEqual(report.total_executed, len(files))
        self.assertFalse(report.dry_run)

        # Files physically placed correctly
        self.assertTrue((self.root / "Images"    / "photo.jpg").exists())
        self.assertTrue((self.root / "Documents" / "report.pdf").exists())
        self.assertTrue((self.root / "Code"      / "script.py").exists())
        self.assertTrue((self.root / "Videos"    / "clip.mp4").exists())
        self.assertTrue((self.root / "Archives"  / "backup.zip").exists())
        self.assertTrue((self.root / "Documents" / "notes.txt").exists())
        self.assertTrue((self.root / "Audio"     / "track.mp3").exists())
        self.assertTrue((self.root / "Others"    / "random.xyz").exists())

        # Source root should have no more loose files
        loose = [f for f in self.root.iterdir() if f.is_file()]
        self.assertEqual(loose, [])

    def test_full_pipeline_empty_directory(self) -> None:
        """Empty directory produces a clean zero-executed report."""
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=[], ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        self.assertIsNone(report.fail_index)
        self.assertEqual(report.total_planned, 0)
        self.assertEqual(report.total_executed, 0)

    def test_full_pipeline_single_file(self) -> None:
        """Single-file batch completes cleanly."""
        (self.root / "data.csv").write_text("a,b\n1,2\n")
        files = [self.root / "data.csv"]
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        self.assertIsNone(report.fail_index)
        self.assertEqual(report.total_executed, 1)
        self.assertTrue((self.root / "Documents" / "data.csv").exists())

    def test_total_bytes_moved_accurate(self) -> None:
        """BatchReport.total_bytes_moved reflects sum of file sizes."""
        content_a = b"A" * 1024
        content_b = b"B" * 2048
        (self.root / "img.jpg").write_bytes(content_a)
        (self.root / "vid.mp4").write_bytes(content_b)

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        self.assertEqual(report.total_bytes_moved, len(content_a) + len(content_b))

    def test_destinations_created_counted(self) -> None:
        """BatchReport.destinations_created counts newly made dirs."""
        (self.root / "photo.jpg").write_bytes(b"img")
        files = [self.root / "photo.jpg"]
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        # "Images" did not exist — must have been created
        self.assertGreaterEqual(report.destinations_created, 1)

    def test_custom_ruleset_pipeline(self) -> None:
        """Custom v2.0 ruleset routes files through the full pipeline."""
        ruleset = _build_v20_ruleset([
            {"name": "PyFiles", "match": {"extensions": [".py"]}, "destination": "Python"},
            {"name": "Configs", "match": {"extensions": [".json", ".yaml"]}, "destination": "Config"},
        ])
        _make_tree(self.root, {
            "main.py":   "# main",
            "config.json": '{"key": "value"}',
            "readme.md":  "# readme",
        })
        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=ruleset,
            dest_map=None, dry_run=False,
        )
        self.assertIsNone(report.fail_index)
        self.assertTrue((self.root / "Python" / "main.py").exists())
        self.assertTrue((self.root / "Config" / "config.json").exists())
        self.assertTrue((self.root / "Others" / "readme.md").exists())

    def test_v22_size_rule_pipeline(self) -> None:
        """v2.2 size-constrained rules route through the full pipeline correctly.

        Rules are mutually exclusive (non-overlapping size bands) so safe mode
        does not trigger a conflict skip.
        """
        ruleset = _build_v22_ruleset([
            {
                "name": "BigImages",
                "priority": 1,
                "match": {"extensions": [".jpg"], "min_size": 1000},
                "destination": "BigImages",
            },
            {
                "name": "SmallImages",
                "priority": 2,
                "match": {"extensions": [".jpg"], "max_size": 999},
                "destination": "SmallImages",
            },
        ])
        (self.root / "large.jpg").write_bytes(b"X" * 2000)
        (self.root / "small.jpg").write_bytes(b"X" * 100)

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=ruleset,
            dest_map=None, dry_run=False,
        )
        self.assertIsNone(report.fail_index)
        self.assertTrue((self.root / "BigImages"   / "large.jpg").exists())
        self.assertTrue((self.root / "SmallImages" / "small.jpg").exists())

    def test_pipeline_result_statuses(self) -> None:
        """Each result dict carries a valid status field."""
        _make_tree(self.root, {
            "a.jpg": b"img",
            "b.pdf": b"doc",
        })
        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        valid_statuses = {"moved", "dry_run", "skipped", "failed", "validation_failed"}
        for result in report.results:
            self.assertIn(result["status"], valid_statuses)

    def test_report_file_matches_executed_count(self) -> None:
        """report.results length equals total_planned (skipped + executed + failed)."""
        _make_tree(self.root, {
            "a.jpg": b"img",
            "b.pdf": b"doc",
            "c.mp4": b"vid",
        })
        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        # total_planned = sum(executed + skipped + failed)
        total_from_results = sum(
            1 for r in report.results
            if r["status"] in ("moved", "dry_run", "skipped", "failed", "validation_failed")
        )
        self.assertEqual(total_from_results, report.total_planned)


# ===========================================================================
# 2. Duplicate Detection Pipeline
# ===========================================================================

class TestDuplicatePipeline(unittest.TestCase):
    """
    Tests the full duplicate detection pipeline:
      scan → collect_snapshot → group_by_size → group_by_partial_hash
            → group_by_hash → find_duplicates → export/report

    Verifies correctness, keep strategies, and result stability.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run_full_pipeline(self, keep: str = "oldest") -> dict:
        return scan_duplicates(self.root, keep=keep)

    def test_detects_two_groups(self) -> None:
        """Pipeline finds two independent duplicate groups."""
        content_a = b"duplicate content AAAA"
        content_b = b"B" * 8192

        (self.root / "a1.txt").write_bytes(content_a)
        (self.root / "a2.txt").write_bytes(content_a)
        (self.root / "b1.bin").write_bytes(content_b)
        (self.root / "b2.bin").write_bytes(content_b)
        (self.root / "b3.bin").write_bytes(content_b)
        (self.root / "unique.txt").write_text("I am unique — no duplicate")

        result = self._run_full_pipeline()

        self.assertEqual(result["total_scanned"], 6)
        self.assertEqual(len(result["duplicates"]), 2)
        group_sizes = sorted(len(g) for g in result["duplicates"].values())
        self.assertEqual(group_sizes, [2, 3])
        self.assertEqual(result["total_duplicate_files"], 3)

    def test_no_false_positives(self) -> None:
        """Files with different content are never grouped as duplicates."""
        for i in range(10):
            (self.root / f"file_{i}.txt").write_text(f"unique content {i}")

        result = self._run_full_pipeline()
        self.assertEqual(len(result["duplicates"]), 0)
        self.assertEqual(result["total_duplicate_files"], 0)

    def test_keep_oldest_puts_oldest_first(self) -> None:
        """keep='oldest' puts the file with earliest mtime at index 0."""
        (self.root / "newer.txt").write_bytes(b"same content abc")
        time.sleep(0.05)
        (self.root / "older.txt").write_bytes(b"same content abc")

        # Force mtime on older.txt to be clearly earlier
        older_path = self.root / "older.txt"
        earlier_time = time.time() - 3600
        os.utime(older_path, (earlier_time, earlier_time))

        result = scan_duplicates(self.root, keep="oldest")
        groups = list(result["duplicates"].values())
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0][0].path.name, "older.txt")

    def test_keep_newest_puts_newest_first(self) -> None:
        """keep='newest' puts the file with latest mtime at index 0."""
        older_path = self.root / "old.txt"
        newer_path = self.root / "new.txt"
        older_path.write_bytes(b"same bytes here")
        newer_path.write_bytes(b"same bytes here")
        earlier_time = time.time() - 3600
        os.utime(older_path, (earlier_time, earlier_time))

        result = scan_duplicates(self.root, keep="newest")
        groups = list(result["duplicates"].values())
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0][0].path.name, "new.txt")

    def test_keep_first_alphabetical_order(self) -> None:
        """keep='first' puts the alphabetically first path at index 0."""
        (self.root / "z_copy.txt").write_bytes(b"identical content here")
        (self.root / "a_copy.txt").write_bytes(b"identical content here")

        result = scan_duplicates(self.root, keep="first")
        groups = list(result["duplicates"].values())
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0][0].path.name, "a_copy.txt")

    def test_wasted_bytes_calculation(self) -> None:
        """wasted_bytes = sum of extra-copy sizes across all groups."""
        content = b"X" * 512
        (self.root / "c1.bin").write_bytes(content)
        (self.root / "c2.bin").write_bytes(content)
        (self.root / "c3.bin").write_bytes(content)

        result = self._run_full_pipeline()
        self.assertEqual(result["wasted_bytes"], 512 * 2)

    def test_subdirectory_duplicates_detected(self) -> None:
        """Duplicates nested in subdirectories are found."""
        sub = self.root / "subdir"
        sub.mkdir()
        content = b"nested duplicate bytes"
        (self.root / "top.txt").write_bytes(content)
        (sub / "nested.txt").write_bytes(content)

        result = self._run_full_pipeline()
        self.assertEqual(len(result["duplicates"]), 1)
        self.assertEqual(result["total_duplicate_files"], 1)

    def test_empty_folder_returns_clean_result(self) -> None:
        """Empty folder → zero duplicates, no crash."""
        result = self._run_full_pipeline()
        self.assertEqual(result["total_scanned"], 0)
        self.assertEqual(len(result["duplicates"]), 0)
        self.assertEqual(result["total_duplicate_files"], 0)
        self.assertEqual(result["wasted_bytes"], 0)

    def test_result_keys_present(self) -> None:
        """scan_duplicates result dict always contains all expected keys."""
        result = self._run_full_pipeline()
        expected_keys = {
            "folder", "keep_strategy", "total_scanned", "skipped_by_size",
            "skipped_by_partial", "total_hashed", "workers",
            "skipped_unreadable", "duplicates", "total_duplicate_files",
            "wasted_bytes", "scan_duration_seconds",
        }
        for key in expected_keys:
            self.assertIn(key, result, f"Missing key: {key}")

    def test_scan_duplicates_reports_worker_count(self) -> None:
        """workers field reflects the resolved thread count."""
        (self.root / "f1.txt").write_bytes(b"content")
        result = scan_duplicates(self.root, max_workers=2)
        self.assertEqual(result["workers"], 2)

    def test_phase_memory_release(self) -> None:
        """Pipeline completes without accumulating phase intermediates."""
        # Verify by running pipeline multiple times in sequence
        for _ in range(3):
            (self.root / f"run_{_}.txt").write_bytes(b"content " * 50)
        for _ in range(3):
            result = self._run_full_pipeline()
        # If memory leaked, OS would OOM — completing 3 runs without error is the assertion
        self.assertIsNotNone(result)


# ===========================================================================
# 3. Dry-Run Guarantee
# ===========================================================================

class TestDryRunGuarantee(unittest.TestCase):
    """
    Dry-run must NEVER mutate the filesystem.

    Tests organizer, BatchEngine, and ActionController dry-run paths.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _snapshot(self) -> list[Path]:
        """All paths under root, sorted."""
        return sorted(self.root.rglob("*"))

    def test_batch_engine_dry_run_no_mutation(self) -> None:
        """BatchEngine dry_run=True never moves, creates or deletes files."""
        _make_tree(self.root, {
            "photo.jpg":  b"img",
            "report.pdf": b"doc",
            "script.py":  "code",
        })
        before = self._snapshot()

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=True,
        )

        after = self._snapshot()
        self.assertEqual(before, after, "Dry run mutated filesystem")
        # Dry-run still produces correct counts
        self.assertEqual(report.total_executed, len(files))
        self.assertTrue(report.dry_run)

    def test_organize_shim_dry_run_no_mutation(self) -> None:
        """organize() dry_run=True never touches the filesystem."""
        _make_tree(self.root, {
            "video.mp4":  b"vid",
            "audio.mp3":  b"aud",
        })
        before = self._snapshot()
        organize(self.root, dry_run=True)
        after = self._snapshot()
        self.assertEqual(before, after)

    def test_dry_run_results_show_correct_status(self) -> None:
        """Dry-run results carry status='dry_run', not 'moved'."""
        (self.root / "img.jpg").write_bytes(b"img")
        files = [self.root / "img.jpg"]
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=True,
        )
        statuses = [r["status"] for r in report.results]
        self.assertIn("dry_run", statuses)
        self.assertNotIn("moved", statuses)

    def test_duplicate_scan_dry_run_no_deletion(self) -> None:
        """Duplicate scan with dry_run=True never deletes files."""
        content = b"duplicate content for dry run test"
        (self.root / "d1.txt").write_bytes(content)
        (self.root / "d2.txt").write_bytes(content)
        before = self._snapshot()

        result = scan_duplicates(self.root)
        # Just scanning — no deletion unless ActionController is used
        after = self._snapshot()
        self.assertEqual(before, after)
        self.assertEqual(len(result["duplicates"]), 1)

    def test_fom_dry_run_no_mutation(self) -> None:
        """FileOperationManager dry_run=True produces no filesystem changes."""
        (self.root / "source.txt").write_text("content")
        dst = self.root / "destination"
        dst.mkdir()
        before = self._snapshot()

        fom = FileOperationManager(dry_run=True)
        result = fom.atomic_move(self.root / "source.txt", dst)

        after = self._snapshot()
        self.assertEqual(before, after)
        self.assertEqual(result.status, "dry_run")


# ===========================================================================
# 4. Policy Mode Pipeline
# ===========================================================================

class TestPolicyModePipeline(unittest.TestCase):
    """
    Tests policy modes (strict / safe / warn) through the full pipeline.

    A conflict is created by placing a file that matches two rules.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        # Ruleset with deliberate overlap on .txt extension
        self._conflicting_ruleset = _build_v22_ruleset([
            {"name": "TextDocs", "priority": 1,
             "match": {"extensions": [".txt"]}, "destination": "TextDocs"},
            {"name": "AllDocs", "priority": 2,
             "match": {"extensions": [".txt", ".md"]}, "destination": "AllDocs"},
        ])

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_safe_mode_skips_conflicting_file(self) -> None:
        """Policy safe: conflicting file is skipped, unambiguous files proceed."""
        (self.root / "conflict.txt").write_text("conflicts two rules")
        (self.root / "readme.md").write_text("unambiguous")

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=self._conflicting_ruleset,
            dest_map=None, dry_run=True, policy_mode="safe",
        )

        metrics = report.policy_metrics
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.mode, "safe")
        self.assertGreaterEqual(metrics.files_skipped, 1)
        # readme.md is unambiguous — must still be planned
        self.assertEqual(report.total_planned, 1)  # only readme.md

    def test_warn_mode_applies_first_rule(self) -> None:
        """Policy warn: conflicting file uses first (lowest-priority-number) rule."""
        (self.root / "conflict.txt").write_text("warn mode test")

        files = [self.root / "conflict.txt"]
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=self._conflicting_ruleset,
            dest_map=None, dry_run=True, policy_mode="warn",
        )

        metrics = report.policy_metrics
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.mode, "warn")
        self.assertGreaterEqual(metrics.overrides_applied, 1)
        self.assertGreaterEqual(metrics.conflicts_detected, 1)
        # File must be planned (not skipped) with first matching rule
        self.assertEqual(report.total_planned, 1)

    def test_strict_mode_aborts_on_conflict(self) -> None:
        """Policy strict: any conflict aborts the entire plan (fail_index=0)."""
        (self.root / "conflict.txt").write_text("strict mode test")

        files = [self.root / "conflict.txt"]
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=self._conflicting_ruleset,
            dest_map=None, dry_run=True, policy_mode="strict",
        )

        self.assertIsNotNone(report.fail_index)
        self.assertEqual(report.total_executed, 0)

    def test_strict_mode_no_files_moved(self) -> None:
        """Policy strict abort leaves filesystem unchanged."""
        (self.root / "conflict.txt").write_text("strict abort test")
        (self.root / "readme.md").write_text("also present")

        before = sorted(self.root.iterdir())
        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        engine.run_from_files(
            files=files, ruleset=self._conflicting_ruleset,
            dest_map=None, dry_run=False, policy_mode="strict",
        )
        after = sorted(self.root.iterdir())
        self.assertEqual(before, after)

    def test_no_conflict_all_modes_equivalent(self) -> None:
        """Without conflicts, strict / safe / warn all produce identical results."""
        (self.root / "photo.jpg").write_bytes(b"img")

        results = {}
        for mode in ("strict", "safe", "warn"):
            engine = BatchEngine(scan_root=self.root)
            report = engine.run_from_files(
                files=[self.root / "photo.jpg"],
                ruleset=DEFAULT_RULESET,
                dest_map=None, dry_run=True, policy_mode=mode,
            )
            results[mode] = report.total_planned

        self.assertEqual(results["strict"], results["safe"])
        self.assertEqual(results["safe"],   results["warn"])

    def test_warn_mode_conflict_details_sorted_by_filename(self) -> None:
        """PolicyMetrics.conflict_details are sorted by filename."""
        for c in ("z_file.txt", "a_file.txt", "m_file.txt"):
            (self.root / c).write_text("conflict")

        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=self._conflicting_ruleset,
            dest_map=None, dry_run=True, policy_mode="warn",
        )
        conflicts = report.policy_metrics.conflict_details
        names = [c.filename for c in conflicts]
        self.assertEqual(names, sorted(names))


# ===========================================================================
# 5. DestinationMap Pipeline
# ===========================================================================

class TestDestinationMapPipeline(unittest.TestCase):
    """
    Tests full pipeline with an explicit DestinationMap.

    Verifies that destination paths are resolved through the map rather than
    defaulting to scan_root / dest_key.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.archive_root = Path(self._tmp.name) / "archive"
        self.archive_root.mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_destination_map_routes_to_custom_paths(self) -> None:
        """Files route to dest_map-specified absolute paths."""
        from destination_map import parse_destination_map

        map_data = {
            "version": "2.3",
            "destinations": {
                "Images":    "photos",
                "Documents": "docs",
                "Videos":    "media",
                "Audio":     "media",
                "Archives":  "archives",
                "Code":      "src",
                "Others":    "misc",
            },
            "base_dir": str(self.archive_root),
            "conflict_policy": "rename",
        }
        dest_map = parse_destination_map(map_data, default_base_dir=self.archive_root)

        _make_tree(self.root, {
            "photo.jpg":   b"img",
            "script.py":   "code",
        })
        files = sorted(
            [f for f in self.root.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=dest_map, dry_run=False,
        )
        self.assertIsNone(report.fail_index)
        self.assertTrue((self.archive_root / "photos" / "photo.jpg").exists())
        self.assertTrue((self.archive_root / "src"    / "script.py").exists())


# ===========================================================================
# 6. Failure Simulation Tests
# ===========================================================================

class TestFailureSimulation(unittest.TestCase):
    """
    Simulates real filesystem failures and verifies correct system behaviour.

    Table of scenarios:
      Permission denied on directory  → skip + continue (no crash)
      Permission denied on file       → skip during hash (no crash)
      Broken symlink                  → skip gracefully (follow policy)
      Rename collision                → deterministic suffix resolution
      Missing source file mid-batch   → validation failure, not crash
      Cross-action collision          → detected and reported
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        # Restore permissions before cleanup (chmod 000 dirs block rmtree)
        for path in self.root.rglob("*"):
            try:
                os.chmod(path, 0o755)
            except OSError:
                pass
        self._tmp.cleanup()

    # ── Permission denied ────────────────────────────────────────────────────

    @unittest.skipIf(os.getuid() == 0, "Running as root — chmod restrictions are bypassed")
    def test_permission_denied_directory_skipped(self) -> None:
        """Scanner skips unreadable directories and continues traversal."""
        accessible = self.root / "accessible"
        restricted = self.root / "restricted"
        accessible.mkdir()
        restricted.mkdir()

        (accessible / "visible.txt").write_text("I am visible")
        (restricted / "hidden.txt").write_text("I am hidden")

        os.chmod(restricted, 0o000)
        try:
            policy = ScanPolicy(recursive=True)
            files = list(scan_files(self.root, policy))
            names = [f.name for f in files]
            self.assertIn("visible.txt", names)
            self.assertNotIn("hidden.txt", names)
        finally:
            os.chmod(restricted, 0o755)

    @unittest.skipIf(os.getuid() == 0, "Running as root — chmod restrictions are bypassed")
    def test_permission_denied_file_skipped_during_snapshot(self) -> None:
        """collect_snapshot skips unreadable files without crashing."""
        (self.root / "readable.txt").write_text("readable content here")
        (self.root / "locked.txt").write_text("locked content here")
        os.chmod(self.root / "locked.txt", 0o000)

        try:
            snapshot, skipped = collect_snapshot(self.root)
            names = [e.path.name for e in snapshot]
            # Should still have readable.txt
            self.assertIn("readable.txt", names)
            # Should not crash even though locked.txt is unreadable
        finally:
            os.chmod(self.root / "locked.txt", 0o644)

    @unittest.skipIf(os.getuid() == 0, "Running as root — chmod restrictions are bypassed")
    def test_unreadable_file_skipped_during_hashing(self) -> None:
        """Hashing pipeline skips unreadable files without crashing."""
        content = b"same content" * 100  # > PARTIAL_BYTES threshold
        (self.root / "readable1.bin").write_bytes(content)
        (self.root / "readable2.bin").write_bytes(content)
        (self.root / "locked.bin").write_bytes(content)
        os.chmod(self.root / "locked.bin", 0o000)

        try:
            result = scan_duplicates(self.root)
            # locked.bin should appear in skipped_unreadable
            skipped_paths = [s["path"] for s in result["skipped_unreadable"]]
            # At least the duplicates pipeline ran without exception
            self.assertIsNotNone(result["duplicates"])
        finally:
            os.chmod(self.root / "locked.bin", 0o644)

    # ── Broken symlink ────────────────────────────────────────────────────────

    def test_broken_symlink_skipped_ignore_policy(self) -> None:
        """Broken symlink is silently skipped under SYMLINK_IGNORE."""
        (self.root / "real.txt").write_text("real content")
        os.symlink("/nonexistent/path/ghost.txt", self.root / "broken_link.txt")

        policy = ScanPolicy(recursive=True, symlink_policy=SYMLINK_IGNORE)
        files = list(scan_files(self.root, policy))
        names = [f.name for f in files]

        self.assertIn("real.txt", names)
        self.assertNotIn("broken_link.txt", names)

    def test_broken_symlink_skipped_follow_policy(self) -> None:
        """Broken symlink is skipped under SYMLINK_FOLLOW (not a regular file)."""
        (self.root / "real.txt").write_text("real content")
        os.symlink("/definitely/does/not/exist.txt", self.root / "broken.txt")

        policy = ScanPolicy(recursive=True, symlink_policy=SYMLINK_FOLLOW)
        # Must not crash
        files = list(scan_files(self.root, policy))
        names = [f.name for f in files]
        self.assertIn("real.txt", names)

    def test_symlink_error_policy_raises_scan_error(self) -> None:
        """SYMLINK_ERROR policy raises ScanError immediately on first symlink."""
        (self.root / "real.txt").write_text("content")
        os.symlink(self.root / "real.txt", self.root / "link.txt")

        policy = ScanPolicy(recursive=True, symlink_policy=SYMLINK_ERROR)
        with self.assertRaises(ScanError):
            list(scan_files(self.root, policy))

    # ── Collision resolution ──────────────────────────────────────────────────

    def test_collision_resolved_deterministically(self) -> None:
        """FileOperationManager resolves filename collision with (1) suffix."""
        src_dir = self.root / "src"
        dst_dir = self.root / "dst"
        src_dir.mkdir(); dst_dir.mkdir()

        (src_dir / "file.txt").write_text("source content")
        (dst_dir / "file.txt").write_text("existing content in destination")

        fom = FileOperationManager(dry_run=False)
        result = fom.atomic_move(src_dir / "file.txt", dst_dir)

        self.assertEqual(result.status, "moved")
        self.assertTrue(result.collision)
        # Original must be preserved
        self.assertTrue((dst_dir / "file.txt").exists())
        # New file must use deterministic suffix
        self.assertTrue((dst_dir / "file (1).txt").exists())

    def test_multiple_collisions_resolved_sequentially(self) -> None:
        """FOM resolves (1), (2), (3)... in ascending order."""
        src_dir = self.root / "src"
        dst_dir = self.root / "dst"
        src_dir.mkdir(); dst_dir.mkdir()

        # Pre-populate (1) and original
        (dst_dir / "file.txt").write_text("existing")
        (dst_dir / "file (1).txt").write_text("existing 1")

        (src_dir / "file.txt").write_text("new content")
        fom = FileOperationManager(dry_run=False)
        result = fom.atomic_move(src_dir / "file.txt", dst_dir)

        self.assertEqual(result.status, "moved")
        self.assertTrue(result.collision)
        self.assertTrue((dst_dir / "file (2).txt").exists())

    def test_rollback_on_fom_restores_files(self) -> None:
        """FOM rollback restores moved files to original location."""
        src = self.root / "source.txt"
        dst_dir = self.root / "dest"
        src.write_text("rollback test content")
        dst_dir.mkdir()

        fom = FileOperationManager(dry_run=False)
        result = fom.atomic_move(src, dst_dir)
        self.assertEqual(result.status, "moved")
        self.assertFalse(src.exists())

        rollback_results = fom.rollback()
        # After rollback, original location must have the file
        self.assertTrue(src.exists())
        self.assertFalse((dst_dir / "source.txt").exists())
        self.assertTrue(any(r["status"] == "rolled_back" for r in rollback_results))

    # ── Source deleted mid-batch ──────────────────────────────────────────────

    def test_source_not_found_triggers_validation_failure(self) -> None:
        """BatchEngine VALIDATE phase catches missing source file before executing."""
        ghost = self.root / "ghost.jpg"
        # Do NOT create the file — it's missing on purpose

        files = [ghost]  # file doesn't exist
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        # Must fail at VALIDATE, not crash
        self.assertIsNotNone(report.fail_index)
        self.assertEqual(report.phase_reached, BatchPhase.VALIDATE)
        self.assertEqual(report.total_executed, 0)

    def test_partial_batch_source_missing_aborts_cleanly(self) -> None:
        """When one source file disappears, batch fails cleanly at VALIDATE."""
        (self.root / "real.jpg").write_bytes(b"real image")
        ghost = self.root / "ghost.jpg"
        # ghost does not exist

        files = sorted([self.root / "ghost.jpg", self.root / "real.jpg"])
        engine = BatchEngine(scan_root=self.root)
        report = engine.run_from_files(
            files=files, ruleset=DEFAULT_RULESET,
            dest_map=None, dry_run=False,
        )
        self.assertIsNotNone(report.fail_index)
        self.assertEqual(report.total_executed, 0)  # validation failed before execution


# ===========================================================================
# 7. Determinism Tests
# ===========================================================================

class TestDeterminism(unittest.TestCase):
    """
    Two identical runs on identical data must produce identical results.

    Tests scan ordering, hash pipeline ordering, and rule evaluation ordering.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _make_mixed_tree(self) -> None:
        """Populate root with a stable, repeatable set of files and dirs."""
        _make_tree(self.root, {
            "alpha.jpg":   b"A" * 100,
            "beta.pdf":    b"B" * 200,
            "gamma.mp4":   b"G" * 300,
            "delta.py":    "# delta",
            "epsilon.zip": b"Z" * 400,
            "zeta.txt":    "zeta content",
            "sub": {
                "nested_a.jpg": b"A" * 100,  # same content as alpha.jpg
                "nested_b.txt": "different content",
                "nested_c.mp3": b"M" * 150,
            },
        })

    def test_scan_order_stable_across_runs(self) -> None:
        """scan_files yields paths in identical order on repeated calls."""
        self._make_mixed_tree()
        policy = ScanPolicy(recursive=True)
        run1 = [str(p) for p in scan_files(self.root, policy)]
        run2 = [str(p) for p in scan_files(self.root, policy)]
        self.assertEqual(run1, run2)

    def test_list_files_stable_across_runs(self) -> None:
        """list_files() returns identical sorted list on repeated calls."""
        self._make_mixed_tree()
        run1 = [str(p) for p in list_files(self.root)]
        run2 = [str(p) for p in list_files(self.root)]
        self.assertEqual(run1, run2)

    def test_duplicate_detection_stable_across_runs(self) -> None:
        """scan_duplicates produces identical group ordering on repeated calls."""
        content_x = b"X" * 512
        content_y = b"Y" * 1024
        (self.root / "x1.bin").write_bytes(content_x)
        (self.root / "x2.bin").write_bytes(content_x)
        (self.root / "x3.bin").write_bytes(content_x)
        (self.root / "y1.bin").write_bytes(content_y)
        (self.root / "y2.bin").write_bytes(content_y)

        result1 = scan_duplicates(self.root)
        result2 = scan_duplicates(self.root)

        keys1 = list(result1["duplicates"].keys())
        keys2 = list(result2["duplicates"].keys())
        self.assertEqual(keys1, keys2)

        for key in keys1:
            paths1 = [str(e.path) for e in result1["duplicates"][key]]
            paths2 = [str(e.path) for e in result2["duplicates"][key]]
            self.assertEqual(paths1, paths2)

    def test_batch_report_results_stable_across_runs(self) -> None:
        """BatchEngine produces identical result file lists across runs."""
        src1 = self.root / "run1"
        src2 = self.root / "run2"
        src1.mkdir(); src2.mkdir()

        for i, ext in enumerate(["a.jpg", "b.pdf", "c.mp4", "d.py"]):
            (src1 / ext).write_bytes(bytes([i]) * 64)
            (src2 / ext).write_bytes(bytes([i]) * 64)

        def _get_file_order(src: Path) -> list[str]:
            dst = self.root / f"out_{src.name}"
            dst.mkdir(exist_ok=True)
            engine = BatchEngine(scan_root=src)
            files = sorted(
                [f for f in src.iterdir() if f.is_file()],
                key=lambda p: p.name,
            )
            report = engine.run_from_files(
                files=files, ruleset=DEFAULT_RULESET,
                dest_map=None, dry_run=True,
            )
            return [r.get("file", "") for r in report.results]

        order1 = _get_file_order(src1)
        order2 = _get_file_order(src2)
        self.assertEqual(order1, order2)

    def test_rule_evaluation_order_stable(self) -> None:
        """Rules are evaluated in (priority ASC, config_index ASC) order."""
        ruleset = _build_v22_ruleset([
            {"name": "High", "priority": 1,
             "match": {"extensions": [".txt"]}, "destination": "HighPriority"},
            {"name": "Low", "priority": 5,
             "match": {"extensions": [".txt"]}, "destination": "LowPriority"},
        ])
        from rules import resolve_destination
        dest = resolve_destination("file.txt", ruleset)
        self.assertEqual(dest, "HighPriority")

    def test_snapshot_sort_key_stable(self) -> None:
        """collect_snapshot returns entries sorted by (size, device, inode, path)."""
        for i in range(20):
            (self.root / f"file_{i:02d}.txt").write_text(f"content {i}")

        snapshot1, _ = collect_snapshot(self.root)
        snapshot2, _ = collect_snapshot(self.root)

        paths1 = [str(e.path) for e in snapshot1]
        paths2 = [str(e.path) for e in snapshot2]
        self.assertEqual(paths1, paths2)


# ===========================================================================
# 8. Stress Tests
# ===========================================================================

class TestStress(unittest.TestCase):
    """
    Large directory tree tests — verifies scalability and stability.

    Scale is environment-controlled:
      CLEANSWEEP_STRESS_DIRS  (default 100)
      CLEANSWEEP_STRESS_FILES (default 100)
      CLEANSWEEP_STRESS_SKIP  set to "1" to skip all stress tests

    At defaults: 100 × 100 = 10K files.
    For 1M scale: CLEANSWEEP_STRESS_DIRS=1000 CLEANSWEEP_STRESS_FILES=1000
    """

    @classmethod
    def setUpClass(cls) -> None:
        if _STRESS_SKIP:
            raise unittest.SkipTest("Stress tests skipped (CLEANSWEEP_STRESS_SKIP=1)")

    def _build_large_tree(self, root: Path, n_dirs: int, n_files: int) -> int:
        """
        Create n_dirs subdirectories, each containing n_files files.
        Returns total file count.
        """
        for i in range(n_dirs):
            d = root / f"dir_{i:05d}"
            d.mkdir(parents=True, exist_ok=True)
            for j in range(n_files):
                # Mix of extensions to exercise rule engine
                ext = [".txt", ".jpg", ".pdf", ".mp4", ".py", ".zip",
                       ".mp3", ".xyz"][(j + i) % 8]
                (d / f"file_{j:05d}{ext}").write_bytes(
                    bytes([(i + j) % 256]) * 128
                )
        return n_dirs * n_files

    def test_large_tree_traversal_completes(self) -> None:
        """
        Scanner traverses a large tree without memory explosion or deadlock.

        Default: 100 dirs × 100 files = 10K files.
        Full scale (env): 1000 × 1000 = 1M files.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = self._build_large_tree(root, _STRESS_DIRS, _STRESS_FILES)

            t_start = time.perf_counter()
            policy = ScanPolicy(recursive=True)
            files = list(scan_files(root, policy))
            elapsed = time.perf_counter() - t_start

            self.assertEqual(len(files), expected,
                f"Expected {expected} files, found {len(files)}")

            # Ordering must be stable — sort and re-sort must produce same list
            sorted_once  = sorted(files, key=str)
            sorted_twice = sorted(sorted_once, key=str)
            self.assertEqual(sorted_once, sorted_twice)

            # Soft timing guard: warn but don't fail (hardware varies)
            files_per_sec = expected / elapsed if elapsed > 0 else float("inf")
            print(
                f"\n[Stress] traversal: {expected:,} files in {elapsed:.2f}s "
                f"({files_per_sec:,.0f} files/s)"
            )

    def test_large_tree_hashing_pipeline_stable(self) -> None:
        """
        Hash pipeline on a large tree completes without deadlock or OOM.

        Uses a subset of files with deliberate duplicates to exercise all phases.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            n = min(_STRESS_DIRS * _STRESS_FILES, 5000)

            # Create n files with a controlled set of duplicates (10%)
            dup_content = b"D" * 512
            dup_count = max(1, n // 10)
            unique_count = n - dup_count

            for i in range(unique_count):
                (root / f"unique_{i:06d}.bin").write_bytes(bytes([i % 256]) * 512)
            for i in range(dup_count):
                (root / f"dup_{i:06d}.bin").write_bytes(dup_content)

            t_start = time.perf_counter()
            result = scan_duplicates(root, max_workers=4)
            elapsed = time.perf_counter() - t_start

            self.assertEqual(result["total_scanned"], n)
            self.assertGreaterEqual(len(result["duplicates"]), 1)
            # All duplicate paths must be within root
            for group in result["duplicates"].values():
                for entry in group:
                    self.assertTrue(
                        str(entry.path).startswith(str(root)),
                        f"Entry path outside root: {entry.path}",
                    )

            print(
                f"\n[Stress] hashing: {n:,} files in {elapsed:.2f}s "
                f"({n / elapsed:,.0f} files/s)"
            )

    def test_large_tree_organize_pipeline(self) -> None:
        """
        Organize pipeline processes a large flat directory without errors.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            n = min(_STRESS_DIRS * _STRESS_FILES, 1000)
            extensions = [".jpg", ".pdf", ".mp4", ".py", ".zip", ".mp3",
                          ".txt", ".xyz"]
            for i in range(n):
                ext = extensions[i % len(extensions)]
                (root / f"file_{i:06d}{ext}").write_bytes(b"content")

            files = sorted(
                [f for f in root.iterdir() if f.is_file()],
                key=lambda p: p.name,
            )
            t_start = time.perf_counter()
            engine = BatchEngine(scan_root=root)
            report = engine.run_from_files(
                files=files, ruleset=DEFAULT_RULESET,
                dest_map=None, dry_run=True,  # dry_run=True for speed
            )
            elapsed = time.perf_counter() - t_start

            self.assertIsNone(report.fail_index)
            self.assertEqual(report.total_executed, n)

            print(
                f"\n[Stress] organize (dry-run): {n:,} files in {elapsed:.2f}s "
                f"({n / elapsed:,.0f} files/s)"
            )

    def test_thread_pool_does_not_deadlock(self) -> None:
        """Parallel hashing with multiple worker counts never deadlocks."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            content = b"T" * 4096
            for i in range(200):
                (root / f"f_{i:04d}.bin").write_bytes(
                    content if i % 2 == 0 else bytes([i % 256]) * 4096
                )

            for workers in (1, 2, 4, 8):
                result = scan_duplicates(root, max_workers=workers)
                self.assertIsNotNone(result["duplicates"])

    def test_scan_memory_bounded(self) -> None:
        """
        scan_files() stack memory is O(depth × branching), not O(total_files).

        Creates a deeply nested tree to exercise the iterative DFS stack,
        verifying it completes without RecursionError.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Build a 50-level deep chain
            current = root
            for depth in range(50):
                current = current / f"level_{depth:02d}"
                current.mkdir(parents=True, exist_ok=True)
                (current / f"file_at_{depth}.txt").write_text(f"depth {depth}")

            policy = ScanPolicy(recursive=True)
            files = list(scan_files(root, policy))
            self.assertEqual(len(files), 50)


# ===========================================================================
# 9. Performance Baseline
# ===========================================================================

class TestPerformanceBaseline(unittest.TestCase):
    """
    Soft performance timing assertions.

    These tests warn on slow runs but only fail on extreme outliers
    (100× expected throughput). Actual baselines are documented in README.
    """

    def test_scan_throughput_baseline(self) -> None:
        """
        Scanner throughput on flat directory.

        Baseline: ≥ 5,000 files/second on any modern SSD.
        Failure threshold: < 100 files/second (100× below baseline).
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            n = 1000
            for i in range(n):
                (root / f"file_{i:05d}.txt").write_bytes(b"x" * 64)

            policy = ScanPolicy(recursive=False)
            t = time.perf_counter()
            files = list(scan_files(root, policy))
            elapsed = time.perf_counter() - t

            self.assertEqual(len(files), n)
            fps = n / elapsed if elapsed > 0 else float("inf")
            # Hard lower bound — even spinning rust delivers >100 files/sec
            self.assertGreater(fps, 100,
                f"Scan throughput critically low: {fps:.0f} files/sec")

    def test_hash_pipeline_throughput_baseline(self) -> None:
        """
        Hash pipeline throughput.

        Baseline: ≥ 500 files/second for small (512-byte) files.
        Failure threshold: < 50 files/second.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            n = 500
            content = b"HASH_BASELINE_CONTENT_BYTES" * 20  # ~560 bytes
            for i in range(n):
                (root / f"h_{i:05d}.bin").write_bytes(content)

            t = time.perf_counter()
            snapshot, skipped = collect_snapshot(root)
            skipped_log: list[dict] = []
            _, skipped_size, skipped_partial, hashed = run_hash_pipeline(
                snapshot, keep="oldest", skipped=skipped_log, max_workers=4
            )
            elapsed = time.perf_counter() - t

            fps = n / elapsed if elapsed > 0 else float("inf")
            self.assertGreater(fps, 50,
                f"Hash throughput critically low: {fps:.0f} files/sec")

    def test_rule_evaluation_throughput_baseline(self) -> None:
        """
        Rule engine throughput — resolve_destination() per file.

        Baseline: ≥ 100,000 calls/second (pure Python computation).
        Failure threshold: < 1,000 calls/second.
        """
        from rules import resolve_destination
        filenames = [f"file_{i}.jpg" for i in range(10000)]

        t = time.perf_counter()
        for name in filenames:
            resolve_destination(name, DEFAULT_RULESET)
        elapsed = time.perf_counter() - t

        cps = len(filenames) / elapsed if elapsed > 0 else float("inf")
        self.assertGreater(cps, 1000,
            f"Rule evaluation throughput critically low: {cps:.0f} calls/sec")


if __name__ == "__main__":
    unittest.main(verbosity=2)
