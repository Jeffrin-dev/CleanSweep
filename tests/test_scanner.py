"""
CleanSweep v2.1.0 — Scanner / Traversal Engine Tests.

Covers:
  1.  ScanPolicy construction and validation
  2.  Non-recursive (top-level only)
  3.  max_depth limiting (0, 1, N, None)
  4.  recursive=False vs max_depth=0 equivalence
  5.  Symlink policy: ignore
  6.  Symlink policy: follow (no cycle)
  7.  Symlink policy: follow with cycle detection
  8.  Symlink policy: error
  9.  Glob exclusion patterns — files
  10. Glob exclusion patterns — directories
  11. Case-insensitive exclusion matching
  12. Multiple exclusion patterns
  13. Extension filtering
  14. Folder name filtering
  15. Min file size filtering
  16. Stable ordering — deterministic across runs
  17. Per-directory sorted order
  18. Generator nature — scan_files is an iterator, not a list
  19. Large tree — does not exhaust memory
  20. Backward compat — list_files() signature unchanged
  21. list_files() returns sorted absolute paths
  22. ScanPolicy is frozen / immutable
  23. Temp file exclusion (TEMP_PREFIX)
  24. Special files (devices, sockets) excluded
  25. Empty directory yields nothing
  26. Unreadable directory — scan continues, doesn't crash
  27. Broken symlink handling per policy
  28. collect_snapshot with ScanPolicy
  29. collect_snapshot cycle detection
  30. config.py new fields
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scanner import (
    ScanPolicy, ScanError, scan_files, list_files,
    _matches_exclusion,
    SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR,
    validate_folder,
)
from file_operation_manager import TEMP_PREFIX


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tree(base: Path, spec: dict) -> None:
    """
    Recursively create a directory tree from a spec dict.
    Keys that are strings → create file with that content.
    Keys that are dicts   → create subdirectory and recurse.
    """
    for name, value in spec.items():
        path = base / name
        if isinstance(value, dict):
            path.mkdir(exist_ok=True)
            _make_tree(path, value)
        else:
            path.write_text(str(value))


def _names(paths) -> list[str]:
    """Extract sorted filenames from a list/iterator of paths."""
    return sorted(p.name for p in paths)


# ===========================================================================
# 1. ScanPolicy construction and validation
# ===========================================================================

class TestScanPolicyConstruction(unittest.TestCase):

    def test_defaults(self):
        p = ScanPolicy()
        self.assertTrue(p.recursive)
        self.assertIsNone(p.max_depth)
        self.assertEqual(p.symlink_policy, SYMLINK_IGNORE)
        self.assertEqual(p.exclude_patterns, ())
        self.assertEqual(p.min_file_size, 0)
        self.assertEqual(p.ignore_extensions, ())
        self.assertEqual(p.ignore_folders, ())

    def test_invalid_symlink_policy_raises(self):
        with self.assertRaises(ValueError) as ctx:
            ScanPolicy(symlink_policy="jump")
        self.assertIn("jump", str(ctx.exception))
        self.assertIn("symlink_policy", str(ctx.exception))

    def test_negative_max_depth_raises(self):
        with self.assertRaises(ValueError):
            ScanPolicy(max_depth=-1)

    def test_negative_min_file_size_raises(self):
        with self.assertRaises(ValueError):
            ScanPolicy(min_file_size=-1)

    def test_valid_symlink_policies(self):
        for p in (SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR):
            sp = ScanPolicy(symlink_policy=p)
            self.assertEqual(sp.symlink_policy, p)

    def test_zero_max_depth_valid(self):
        sp = ScanPolicy(max_depth=0)
        self.assertEqual(sp.max_depth, 0)

    def test_zero_min_file_size_valid(self):
        sp = ScanPolicy(min_file_size=0)
        self.assertEqual(sp.min_file_size, 0)


# ===========================================================================
# 2. ScanPolicy is frozen (immutable)
# ===========================================================================

class TestScanPolicyImmutability(unittest.TestCase):

    def test_cannot_mutate(self):
        sp = ScanPolicy()
        with self.assertRaises((AttributeError, TypeError)):
            sp.recursive = False  # type: ignore

    def test_exclude_patterns_tuple(self):
        sp = ScanPolicy(exclude_patterns=("*.log", "*.tmp"))
        self.assertIsInstance(sp.exclude_patterns, tuple)


# ===========================================================================
# 3. Non-recursive — top-level only
# ===========================================================================

class TestNonRecursive(unittest.TestCase):

    def test_recursive_false_top_level_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "root.txt": "r",
                "sub": {"deep.txt": "d"},
            })
            policy = ScanPolicy(recursive=False)
            result = list(scan_files(base, policy))
            names = _names(result)
            self.assertIn("root.txt", names)
            self.assertNotIn("deep.txt", names)

    def test_recursive_false_no_subdirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "a.txt": "a",
                "b.txt": "b",
                "sub": {"c.txt": "c", "nested": {"d.txt": "d"}},
            })
            policy = ScanPolicy(recursive=False)
            result = _names(scan_files(base, policy))
            self.assertEqual(result, ["a.txt", "b.txt"])


# ===========================================================================
# 4. max_depth limiting
# ===========================================================================

class TestMaxDepth(unittest.TestCase):

    def _make_deep_tree(self, base: Path) -> None:
        _make_tree(base, {
            "depth0.txt": "0",
            "level1": {
                "depth1.txt": "1",
                "level2": {
                    "depth2.txt": "2",
                    "level3": {"depth3.txt": "3"},
                },
            },
        })

    def test_max_depth_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_deep_tree(base)
            names = _names(scan_files(base, ScanPolicy(max_depth=0)))
            self.assertIn("depth0.txt", names)
            self.assertNotIn("depth1.txt", names)
            self.assertNotIn("depth2.txt", names)

    def test_max_depth_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_deep_tree(base)
            names = _names(scan_files(base, ScanPolicy(max_depth=1)))
            self.assertIn("depth0.txt", names)
            self.assertIn("depth1.txt", names)
            self.assertNotIn("depth2.txt", names)
            self.assertNotIn("depth3.txt", names)

    def test_max_depth_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_deep_tree(base)
            names = _names(scan_files(base, ScanPolicy(max_depth=2)))
            self.assertIn("depth0.txt", names)
            self.assertIn("depth1.txt", names)
            self.assertIn("depth2.txt", names)
            self.assertNotIn("depth3.txt", names)

    def test_max_depth_none_unlimited(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_deep_tree(base)
            names = _names(scan_files(base, ScanPolicy(max_depth=None)))
            self.assertIn("depth0.txt", names)
            self.assertIn("depth3.txt", names)

    def test_recursive_false_equiv_max_depth_zero(self):
        """recursive=False must yield same files as max_depth=0."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_deep_tree(base)
            no_rec  = _names(scan_files(base, ScanPolicy(recursive=False)))
            depth_0 = _names(scan_files(base, ScanPolicy(max_depth=0)))
            self.assertEqual(no_rec, depth_0)


# ===========================================================================
# 5. Symlink policy: ignore
# ===========================================================================

class TestSymlinkIgnore(unittest.TestCase):

    def test_file_symlink_not_yielded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("content")
            link = base / "link.txt"
            link.symlink_to(real)
            policy = ScanPolicy(symlink_policy=SYMLINK_IGNORE)
            names = _names(scan_files(base, policy))
            self.assertIn("real.txt", names)
            self.assertNotIn("link.txt", names)

    def test_dir_symlink_not_descended(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real_dir = base / "realdir"
            real_dir.mkdir()
            (real_dir / "hidden.txt").write_text("h")
            link_dir = base / "linkdir"
            link_dir.symlink_to(real_dir)
            policy = ScanPolicy(symlink_policy=SYMLINK_IGNORE)
            names = _names(scan_files(base, policy))
            self.assertIn("hidden.txt", names)   # from realdir
            self.assertEqual(names.count("hidden.txt"), 1)  # not via linkdir

    def test_broken_symlink_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "real.txt").write_text("x")
            broken = base / "broken.txt"
            broken.symlink_to(base / "nonexistent")
            policy = ScanPolicy(symlink_policy=SYMLINK_IGNORE)
            names = _names(scan_files(base, policy))
            self.assertIn("real.txt", names)
            self.assertNotIn("broken.txt", names)


# ===========================================================================
# 6. Symlink policy: follow (no cycle)
# ===========================================================================

class TestSymlinkFollow(unittest.TestCase):

    def test_file_symlink_yielded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("content")
            link = base / "link.txt"
            link.symlink_to(real)
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            names = _names(scan_files(base, policy))
            self.assertIn("real.txt", names)
            self.assertIn("link.txt", names)

    def test_dir_symlink_followed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real_dir = base / "realdir"
            real_dir.mkdir()
            (real_dir / "file.txt").write_text("f")
            link_dir = base / "linkdir"
            link_dir.symlink_to(real_dir)
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            names = _names(scan_files(base, policy))
            # file.txt should appear twice (once from realdir, once from linkdir)
            self.assertEqual(names.count("file.txt"), 2)


# ===========================================================================
# 7. Symlink policy: follow with cycle detection
# ===========================================================================

class TestSymlinkCycleDetection(unittest.TestCase):

    def test_cycle_does_not_hang(self):
        """A symlink loop must not cause infinite traversal."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("data")
            # Create a loop: sub/loop → base
            sub = base / "sub"
            sub.mkdir()
            loop = sub / "loop"
            loop.symlink_to(base)  # points back to root — cycle
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            # Must complete and not hang or raise RecursionError
            result = list(scan_files(base, policy))
            self.assertIsInstance(result, list)

    def test_cycle_file_not_yielded_infinitely(self):
        """Each file appears at most a bounded number of times with a cycle."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("data")
            sub = base / "sub"
            sub.mkdir()
            (sub / "subfile.txt").write_text("sub")
            loop = sub / "loop"
            loop.symlink_to(base)
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            result = list(scan_files(base, policy))
            names = [p.name for p in result]
            # Even with a cycle symlink present, files must not multiply unboundedly
            # The cycle is detected and broken; each real path appears at most once per
            # non-cyclic traversal path
            self.assertLess(names.count("file.txt"), 5,
                            f"file.txt appeared too many times: {names.count('file.txt')}")

    def test_self_referential_symlink(self):
        """A symlink pointing to its own parent directory must not loop."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "real.txt").write_text("x")
            # Symlink inside base pointing back to base
            self_link = base / "self_link"
            self_link.symlink_to(base)
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            result = list(scan_files(base, policy))
            self.assertIsInstance(result, list)


# ===========================================================================
# 8. Symlink policy: error
# ===========================================================================

class TestSymlinkError(unittest.TestCase):

    def test_symlink_raises_scan_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("x")
            link = base / "link.txt"
            link.symlink_to(real)
            policy = ScanPolicy(symlink_policy=SYMLINK_ERROR)
            with self.assertRaises(ScanError) as ctx:
                list(scan_files(base, policy))
            self.assertIn("link.txt", str(ctx.exception))

    def test_no_symlink_no_error(self):
        """If no symlinks present, policy=error must not raise."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            (base / "other.txt").write_text("y")
            policy = ScanPolicy(symlink_policy=SYMLINK_ERROR)
            result = list(scan_files(base, policy))
            self.assertEqual(len(result), 2)

    def test_scan_error_names_symlink_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            link = base / "mylink.txt"
            link.symlink_to(base / "nonexistent")
            policy = ScanPolicy(symlink_policy=SYMLINK_ERROR)
            with self.assertRaises(ScanError) as ctx:
                list(scan_files(base, policy))
            self.assertIn("mylink.txt", str(ctx.exception))


# ===========================================================================
# 9. Glob exclusion — files
# ===========================================================================

class TestGlobExclusionFiles(unittest.TestCase):

    def test_single_pattern_excludes_matching_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "report.log").write_text("log")
            (base / "data.csv").write_text("csv")
            (base / "notes.txt").write_text("txt")
            policy = ScanPolicy(exclude_patterns=("*.log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("report.log", names)
            self.assertIn("data.csv", names)
            self.assertIn("notes.txt", names)

    def test_multiple_patterns_exclude_all_matching(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.tmp").write_text("")
            (base / "b.log").write_text("")
            (base / "c.py").write_text("")
            policy = ScanPolicy(exclude_patterns=("*.tmp", "*.log"))
            names = _names(scan_files(base, policy))
            self.assertNotIn("a.tmp", names)
            self.assertNotIn("b.log", names)
            self.assertIn("c.py", names)

    def test_no_patterns_excludes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for name in ("a.txt", "b.log", "c.tmp"):
                (base / name).write_text("x")
            policy = ScanPolicy(exclude_patterns=())
            names = _names(scan_files(base, policy))
            self.assertEqual(len(names), 3)

    def test_exact_name_pattern(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / ".DS_Store").write_text("")
            (base / "main.py").write_text("")
            policy = ScanPolicy(exclude_patterns=(".ds_store",))
            names = _names(scan_files(base, policy))
            self.assertNotIn(".DS_Store", names)
            self.assertIn("main.py", names)


# ===========================================================================
# 10. Glob exclusion — directories
# ===========================================================================

class TestGlobExclusionDirs(unittest.TestCase):

    def test_excluded_dir_not_traversed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "keep.txt": "k",
                "node_modules": {"package.json": "{}", "index.js": "js"},
                ".git": {"config": "cfg"},
            })
            policy = ScanPolicy(exclude_patterns=("node_modules", ".git"))
            names = _names(scan_files(base, policy))
            self.assertIn("keep.txt", names)
            self.assertNotIn("package.json", names)
            self.assertNotIn("index.js", names)
            self.assertNotIn("config", names)

    def test_glob_star_excludes_matching_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "src": {"main.py": ""},
                "__pycache__": {"cached.pyc": ""},
                "test__pycache__extra": {"other.pyc": ""},
            })
            policy = ScanPolicy(exclude_patterns=("__pycache__",))
            names = _names(scan_files(base, policy))
            self.assertIn("main.py", names)
            self.assertNotIn("cached.pyc", names)


# ===========================================================================
# 11. Case-insensitive exclusion
# ===========================================================================

class TestCaseInsensitiveExclusion(unittest.TestCase):

    def test_uppercase_file_excluded_by_lowercase_pattern(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "REPORT.LOG").write_text("")
            (base / "data.csv").write_text("")
            policy = ScanPolicy(exclude_patterns=("*.log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("REPORT.LOG", names)
            self.assertIn("data.csv", names)

    def test_uppercase_pattern_matches_lowercase_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "report.log").write_text("")
            policy = ScanPolicy(exclude_patterns=("*.LOG",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("report.log", names)

    def test_mixed_case_both_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "Report.Log").write_text("")
            (base / "keep.txt").write_text("")
            policy = ScanPolicy(exclude_patterns=("*.log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("Report.Log", names)
            self.assertIn("keep.txt", names)


# ===========================================================================
# 12. _matches_exclusion unit tests (pure function)
# ===========================================================================

class TestMatchesExclusion(unittest.TestCase):

    def test_no_patterns_never_matches(self):
        self.assertFalse(_matches_exclusion("anything.txt", ()))

    def test_star_log_matches_log_files(self):
        self.assertTrue(_matches_exclusion("report.log", ("*.log",)))

    def test_exact_match(self):
        self.assertTrue(_matches_exclusion("node_modules", ("node_modules",)))

    def test_case_insensitive(self):
        self.assertTrue(_matches_exclusion("REPORT.LOG", ("*.log",)))
        self.assertTrue(_matches_exclusion("report.log", ("*.LOG",)))

    def test_no_match_returns_false(self):
        self.assertFalse(_matches_exclusion("main.py", ("*.log", "*.tmp")))

    def test_multiple_patterns_any_match(self):
        self.assertTrue(_matches_exclusion("a.tmp", ("*.log", "*.tmp")))
        self.assertTrue(_matches_exclusion("b.log", ("*.log", "*.tmp")))
        self.assertFalse(_matches_exclusion("c.py",  ("*.log", "*.tmp")))


# ===========================================================================
# 13. Extension filtering
# ===========================================================================

class TestExtensionFiltering(unittest.TestCase):

    def test_ignored_extension_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.log").write_text("")
            (base / "file.txt").write_text("")
            policy = ScanPolicy(ignore_extensions=(".log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("file.log", names)
            self.assertIn("file.txt", names)

    def test_extension_without_dot_normalized(self):
        """ignore_extensions=("log",) must work the same as (".log",)."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.log").write_text("")
            (base / "b.txt").write_text("")
            policy = ScanPolicy(ignore_extensions=("log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("a.log", names)
            self.assertIn("b.txt", names)

    def test_extension_case_insensitive(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "FILE.LOG").write_text("")
            (base / "keep.txt").write_text("")
            policy = ScanPolicy(ignore_extensions=(".log",))
            names = _names(scan_files(base, policy))
            self.assertNotIn("FILE.LOG", names)
            self.assertIn("keep.txt", names)


# ===========================================================================
# 14. Folder filtering (ignore_folders)
# ===========================================================================

class TestFolderFiltering(unittest.TestCase):

    def test_ignored_folder_not_traversed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "keep.txt": "k",
                ".git": {"config": "git"},
                "node_modules": {"dep.js": "js"},
            })
            policy = ScanPolicy(ignore_folders=(".git", "node_modules"))
            names = _names(scan_files(base, policy))
            self.assertIn("keep.txt", names)
            self.assertNotIn("config", names)
            self.assertNotIn("dep.js", names)


# ===========================================================================
# 15. Min file size filtering
# ===========================================================================

class TestMinFileSizeFiltering(unittest.TestCase):

    def test_small_files_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "small.txt").write_bytes(b"x" * 10)
            (base / "large.txt").write_bytes(b"x" * 1000)
            policy = ScanPolicy(min_file_size=500)
            names = _names(scan_files(base, policy))
            self.assertIn("large.txt", names)
            self.assertNotIn("small.txt", names)

    def test_zero_min_size_includes_empty_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "empty.txt").write_bytes(b"")
            (base / "nonempty.txt").write_bytes(b"x")
            policy = ScanPolicy(min_file_size=0)
            names = _names(scan_files(base, policy))
            self.assertIn("empty.txt", names)
            self.assertIn("nonempty.txt", names)

    def test_exact_size_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "exact.txt").write_bytes(b"x" * 100)
            (base / "below.txt").write_bytes(b"x" * 99)
            policy = ScanPolicy(min_file_size=100)
            names = _names(scan_files(base, policy))
            self.assertIn("exact.txt", names)
            self.assertNotIn("below.txt", names)


# ===========================================================================
# 16. Stable ordering — deterministic across runs
# ===========================================================================

class TestDeterministicOrdering(unittest.TestCase):

    def test_same_output_across_100_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "z_file.txt": "z",
                "a_file.txt": "a",
                "m_file.txt": "m",
                "sub": {"z_sub.txt": "zs", "a_sub.txt": "as"},
            })
            policy = ScanPolicy()
            first_run = list(scan_files(base, policy))
            for _ in range(99):
                run = list(scan_files(base, policy))
                self.assertEqual(
                    [str(p) for p in first_run],
                    [str(p) for p in run],
                    "scan_files produced different order on repeated call"
                )

    def test_list_files_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for name in ("z.txt", "a.txt", "m.txt"):
                (base / name).write_text("x")
            result = list_files(base)
            self.assertEqual(result, sorted(result))


# ===========================================================================
# 17. Per-directory sorted order during traversal
# ===========================================================================

class TestPerDirectorySortedOrder(unittest.TestCase):

    def test_files_within_dir_yielded_alphabetically(self):
        """Files in a single directory must be yielded in alphabetical order."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Create files in reverse alpha order to force ordering test
            for name in ("z.txt", "m.txt", "a.txt"):
                (base / name).write_text("x")
            policy = ScanPolicy(recursive=False)
            result = [p.name for p in scan_files(base, policy)]
            self.assertEqual(result, sorted(result))

    def test_subdirs_processed_in_sorted_order(self):
        """Subdirectories must be processed in alphabetical order."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _make_tree(base, {
                "z_dir": {"z_file.txt": "z"},
                "a_dir": {"a_file.txt": "a"},
                "m_dir": {"m_file.txt": "m"},
            })
            policy = ScanPolicy(recursive=True, max_depth=1)
            result = [p.name for p in scan_files(base, policy)]
            # Must be sorted overall
            self.assertEqual(result, sorted(result))


# ===========================================================================
# 18. Generator nature — scan_files is an iterator
# ===========================================================================

class TestGeneratorNature(unittest.TestCase):

    def test_scan_files_is_iterator(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            policy = ScanPolicy()
            import types
            gen = scan_files(base, policy)
            self.assertIsInstance(gen, types.GeneratorType)

    def test_can_stop_early(self):
        """Consuming only the first item must not traverse the whole tree."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for i in range(100):
                (base / f"file_{i:03d}.txt").write_text("x")
            policy = ScanPolicy()
            gen = scan_files(base, policy)
            first = next(gen)
            self.assertIsInstance(first, Path)
            # Generator is now paused — no error, no full traversal forced

    def test_empty_generator_for_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            policy = ScanPolicy()
            result = list(scan_files(base, policy))
            self.assertEqual(result, [])


# ===========================================================================
# 19. Large tree — memory safety (stack-based, not recursive)
# ===========================================================================

class TestLargeTreeMemorySafety(unittest.TestCase):

    def test_wide_directory_no_crash(self):
        """500 files in one directory must scan without memory issues."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for i in range(500):
                (base / f"file_{i:04d}.txt").write_bytes(b"x")
            policy = ScanPolicy()
            result = list(scan_files(base, policy))
            self.assertEqual(len(result), 500)

    def test_deep_tree_no_recursion_error(self):
        """50-level deep tree must not raise RecursionError."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            current = base
            for i in range(50):
                current = current / f"level_{i:02d}"
                current.mkdir()
            (current / "leaf.txt").write_text("leaf")
            policy = ScanPolicy(recursive=True, max_depth=None)
            result = list(scan_files(base, policy))
            names = [p.name for p in result]
            self.assertIn("leaf.txt", names)

    def test_scan_files_never_holds_full_path_list(self):
        """scan_files must be a generator — no list accumulation."""
        import inspect
        from scanner import scan_files as sf
        # Verify it's a generator function
        self.assertTrue(inspect.isgeneratorfunction(sf))


# ===========================================================================
# 20. Backward compat — list_files() signature unchanged
# ===========================================================================

class TestListFilesBackwardCompat(unittest.TestCase):

    def test_all_original_params_work(self):
        """Calling list_files with original v1.x params must work unchanged."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            (base / "file.log").write_text("x")
            # Original signature
            result = list_files(
                folder=base,
                ignore_extensions=[".log"],
                ignore_folders=[],
                min_file_size=0,
                follow_symlinks=False,
                max_depth=None,
            )
            names = [p.name for p in result]
            self.assertIn("file.txt", names)
            self.assertNotIn("file.log", names)

    def test_new_params_extend_behavior(self):
        """New v2.1 params must work alongside old ones."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "keep.txt").write_text("x")
            (base / "exclude.tmp").write_text("x")
            result = list_files(
                folder=base,
                exclude_patterns=["*.tmp"],
                symlink_policy=SYMLINK_IGNORE,
            )
            names = [p.name for p in result]
            self.assertIn("keep.txt", names)
            self.assertNotIn("exclude.tmp", names)

    def test_follow_symlinks_true_maps_to_follow_policy(self):
        """follow_symlinks=True without explicit symlink_policy → SYMLINK_FOLLOW."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("x")
            link = base / "link.txt"
            link.symlink_to(real)
            result = list_files(base, follow_symlinks=True)
            names = [p.name for p in result]
            self.assertIn("link.txt", names)

    def test_follow_symlinks_false_maps_to_ignore_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("x")
            link = base / "link.txt"
            link.symlink_to(real)
            result = list_files(base, follow_symlinks=False)
            names = [p.name for p in result]
            self.assertNotIn("link.txt", names)


# ===========================================================================
# 21. list_files() returns sorted absolute paths
# ===========================================================================

class TestListFilesOutput(unittest.TestCase):

    def test_returns_sorted_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for name in ("z.txt", "a.txt", "m.txt"):
                (base / name).write_text("x")
            result = list_files(base)
            self.assertEqual(result, sorted(result))

    def test_returns_absolute_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            result = list_files(base)
            for p in result:
                self.assertTrue(p.is_absolute(), f"Expected absolute, got: {p}")

    def test_returns_list_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "f.txt").write_text("x")
            result = list_files(base)
            self.assertIsInstance(result, list)


# ===========================================================================
# 22. Temp file exclusion
# ===========================================================================

class TestTempFileExclusion(unittest.TestCase):

    def test_temp_prefix_files_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "normal.txt").write_text("x")
            (base / f"{TEMP_PREFIX}atomic_move.txt").write_text("x")
            policy = ScanPolicy()
            names = _names(scan_files(base, policy))
            self.assertIn("normal.txt", names)
            self.assertFalse(
                any(n.startswith(TEMP_PREFIX) for n in names),
                f"Temp file found in output: {names}"
            )


# ===========================================================================
# 23. Empty directory
# ===========================================================================

class TestEmptyDirectory(unittest.TestCase):

    def test_empty_dir_yields_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Create empty subdirs
            (base / "empty_sub").mkdir()
            policy = ScanPolicy()
            result = list(scan_files(base, policy))
            self.assertEqual(result, [])


# ===========================================================================
# 24. Unreadable directory — scan continues
# ===========================================================================

class TestUnreadableDirectory(unittest.TestCase):

    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_unreadable_dir_scan_continues(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            locked = base / "locked"
            locked.mkdir()
            (locked / "hidden.txt").write_text("h")
            (base / "visible.txt").write_text("v")
            os.chmod(locked, 0o000)
            try:
                policy = ScanPolicy()
                result = list(scan_files(base, policy))
                names = [p.name for p in result]
                self.assertIn("visible.txt", names)
                self.assertNotIn("hidden.txt", names)
            finally:
                os.chmod(locked, 0o755)


# ===========================================================================
# 25. collect_snapshot with ScanPolicy
# ===========================================================================

class TestCollectSnapshotWithPolicy(unittest.TestCase):

    def test_policy_glob_exclusion(self):
        from duplicates import collect_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "keep.txt").write_text("k")
            (base / "exclude.log").write_text("e")
            policy = ScanPolicy(exclude_patterns=("*.log",))
            snapshot, _ = collect_snapshot(base, policy=policy)
            names = [e.path.name for e in snapshot]
            self.assertIn("keep.txt", names)
            self.assertNotIn("exclude.log", names)

    def test_policy_recursive_false(self):
        from duplicates import collect_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "root.txt").write_text("r")
            sub = base / "sub"
            sub.mkdir()
            (sub / "deep.txt").write_text("d")
            policy = ScanPolicy(recursive=False)
            snapshot, _ = collect_snapshot(base, policy=policy)
            names = [e.path.name for e in snapshot]
            self.assertIn("root.txt", names)
            self.assertNotIn("deep.txt", names)

    def test_policy_symlink_error(self):
        from duplicates import collect_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real.txt"
            real.write_text("x")
            (base / "link.txt").symlink_to(real)
            policy = ScanPolicy(symlink_policy=SYMLINK_ERROR)
            with self.assertRaises(ScanError):
                collect_snapshot(base, policy=policy)

    def test_legacy_params_still_work(self):
        """collect_snapshot with no policy must work as before."""
        from duplicates import collect_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            snapshot, _ = collect_snapshot(base, follow_symlinks=False, max_depth=None)
            self.assertEqual(len(snapshot), 1)


# ===========================================================================
# 26. collect_snapshot cycle detection
# ===========================================================================

class TestCollectSnapshotCycleDetection(unittest.TestCase):

    def test_cycle_does_not_hang(self):
        from duplicates import collect_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("data")
            sub = base / "sub"
            sub.mkdir()
            (sub / "loop").symlink_to(base)
            policy = ScanPolicy(symlink_policy=SYMLINK_FOLLOW)
            # Must complete without hanging
            snapshot, skipped = collect_snapshot(base, policy=policy)
            self.assertIsInstance(snapshot, list)
            # Cycle must have been recorded in skipped
            skip_reasons = [s["error"] for s in skipped]
            self.assertTrue(
                any("cycle" in r for r in skip_reasons),
                f"Expected cycle_detected in skipped, got: {skip_reasons}"
            )


# ===========================================================================
# 27. config.py new fields
# ===========================================================================

class TestConfigNewFields(unittest.TestCase):
    """Tests for config.py v2.1.0 scan block schema."""

    # Minimal valid config — used as base for all tests
    _BASE = {"version": "2.7", "scan": {"recursive": True, "symlink_policy": "ignore"}}

    def _cfg(self, **scan_overrides) -> dict:
        """Return a valid config dict with optional scan block overrides."""
        import copy
        d = copy.deepcopy(self._BASE)
        d["scan"].update(scan_overrides)
        return d

    def test_scan_block_required(self):
        """parse_config({"version":"2.7"}) must raise — 'scan' block is required."""
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7"})

    def test_scan_block_required_error_message(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError) as ctx:
            parse_config({"version": "2.7"})
        self.assertIn("scan", str(ctx.exception).lower())

    def test_recursive_required_in_scan(self):
        """'recursive' is required within scan block."""
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {"symlink_policy": "ignore"}})

    def test_symlink_policy_required_in_scan(self):
        """'symlink_policy' is required within scan block."""
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {"recursive": True}})

    def test_symlink_policy_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": "follow"}})
        self.assertEqual(cfg.symlink_policy, "follow")

    def test_all_three_symlink_policies_accepted(self):
        from config import parse_config
        for policy in ("ignore", "follow", "error"):
            cfg = parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": policy}})
            self.assertEqual(cfg.symlink_policy, policy)

    def test_invalid_symlink_policy_raises(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": "jump"}})

    def test_recursive_true_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": "ignore"}})
        self.assertTrue(cfg.recursive)

    def test_recursive_false_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {"recursive": False, "symlink_policy": "ignore"}})
        self.assertFalse(cfg.recursive)

    def test_recursive_must_be_bool(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {"recursive": "yes", "symlink_policy": "ignore"}})

    def test_exclude_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {
            "recursive": True, "symlink_policy": "ignore",
            "exclude": ["*.log", "*.tmp"]
        }})
        self.assertEqual(cfg.exclude, ("*.log", "*.tmp"))

    def test_exclude_non_string_raises(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {
                "recursive": True, "symlink_policy": "ignore",
                "exclude": ["*.log", 42]
            }})

    def test_max_depth_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {
            "recursive": True, "symlink_policy": "ignore", "max_depth": 3
        }})
        self.assertEqual(cfg.max_depth, 3)

    def test_max_depth_null_parsed(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {
            "recursive": True, "symlink_policy": "ignore", "max_depth": None
        }})
        self.assertIsNone(cfg.max_depth)

    def test_negative_max_depth_raises(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {
                "recursive": True, "symlink_policy": "ignore", "max_depth": -1
            }})

    def test_unknown_scan_key_raises(self):
        from config import parse_config, ConfigError
        with self.assertRaises(ConfigError):
            parse_config({"version": "2.7", "scan": {
                "recursive": True, "symlink_policy": "ignore",
                "unknown_key": True
            }})

    def test_follow_symlinks_property_true(self):
        """AppConfig.follow_symlinks is True when symlink_policy='follow'."""
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": "follow"}})
        self.assertTrue(cfg.follow_symlinks)

    def test_follow_symlinks_property_false(self):
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {"recursive": True, "symlink_policy": "ignore"}})
        self.assertFalse(cfg.follow_symlinks)

    def test_exclude_patterns_compat_alias(self):
        """exclude_patterns property is a backward-compat alias for exclude."""
        from config import parse_config
        cfg = parse_config({"version": "2.7", "scan": {
            "recursive": True, "symlink_policy": "ignore",
            "exclude": ["*.log"]
        }})
        self.assertEqual(cfg.exclude_patterns, cfg.exclude)


if __name__ == "__main__":
    unittest.main()
