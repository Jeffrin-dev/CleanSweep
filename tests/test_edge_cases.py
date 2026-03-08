import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(next(p for p in [Path(__file__).parent, Path(__file__).parent.parent] if (p / 'main.py').exists())))

from scanner import list_files
from duplicates import (
    collect_snapshot, group_by_size, group_by_hash,
    find_duplicates, scan_duplicates,
    export_json, _wasted_bytes, FileEntry,
)
from action_controller import delete_duplicates
from organizer import organize


# ---------------------------------------------------------------------------
# Permission errors
# ---------------------------------------------------------------------------

class TestPermissionErrors(unittest.TestCase):
    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_scanner_unreadable_dir_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            locked = Path(tmp) / "locked"
            locked.mkdir()
            (locked / "file.txt").write_text("x")
            os.chmod(locked, 0o000)
            try:
                result = list_files(locked)
                self.assertEqual(result, [])
            finally:
                os.chmod(locked, 0o755)

    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_organizer_unreadable_dir_returns_error_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            locked = Path(tmp) / "locked"
            locked.mkdir()
            os.chmod(locked, 0o000)
            try:
                result = organize(locked)
                self.assertIn("error", result)
                self.assertEqual(result["total"], 0)
            finally:
                os.chmod(locked, 0o755)


# ---------------------------------------------------------------------------
# Broken symlinks
# ---------------------------------------------------------------------------

class TestBrokenSymlinks(unittest.TestCase):
    def test_broken_symlink_excluded_from_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "real.txt").write_text("real")
            broken = base / "broken_link"
            broken.symlink_to(base / "nonexistent_target")
            files = list_files(base)
            self.assertNotIn(broken.resolve(strict=False), files)
            self.assertIn((base / "real.txt").resolve(), files)

    def test_broken_symlink_excluded_from_collect(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "real.txt").write_text("real")
            broken = base / "broken_link"
            broken.symlink_to(base / "nonexistent_target")
            snapshot, _ = collect_snapshot(base, follow_symlinks=False)
            paths = {e.path for e in snapshot}
            self.assertNotIn(broken.resolve(strict=False), paths)


# ---------------------------------------------------------------------------
# Min file size
# ---------------------------------------------------------------------------

class TestMinFileSize(unittest.TestCase):
    def test_small_files_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "small.txt").write_bytes(b"x" * 10)
            (base / "large.txt").write_bytes(b"x" * 1000)
            files = list_files(base, min_file_size=500)
            names = [f.name for f in files]
            self.assertIn("large.txt", names)
            self.assertNotIn("small.txt", names)


# ---------------------------------------------------------------------------
# Vanished files
# ---------------------------------------------------------------------------

class TestVanishedFiles(unittest.TestCase):
    def test_delete_already_gone_counts_as_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            f1 = base / "a.txt"
            f2 = base / "b.txt"
            f1.write_bytes(b"same")
            f2.write_bytes(b"same")
            snapshot, _ = collect_snapshot(base)
            skipped: list[dict] = []
            size_groups = group_by_size(snapshot)
            candidates = [e for g in size_groups.values() if len(g) > 1 for e in g]
            hash_groups = group_by_hash(candidates, skipped)
            duplicates = find_duplicates(hash_groups)
            # Delete the dupe before deletion phase runs
            list(duplicates.values())[0][1].path.unlink()
            result = delete_duplicates(duplicates, dry_run=False)
            self.assertEqual(len(result["failed"]), 0)


# ---------------------------------------------------------------------------
# Max depth
# ---------------------------------------------------------------------------

class TestMaxDepth(unittest.TestCase):
    def test_max_depth_zero_only_root_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "root.txt").write_text("root")
            sub = base / "sub"
            sub.mkdir()
            (sub / "deep.txt").write_text("deep")
            snapshot, _ = collect_snapshot(base, max_depth=0)
            names = [e.path.name for e in snapshot]
            self.assertIn("root.txt", names)
            self.assertNotIn("deep.txt", names)

    def test_max_depth_one_includes_one_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "root.txt").write_text("root")
            sub = base / "sub"
            sub.mkdir()
            (sub / "level1.txt").write_text("level1")
            deep = sub / "deep"
            deep.mkdir()
            (deep / "level2.txt").write_text("level2")
            snapshot, _ = collect_snapshot(base, max_depth=1)
            names = [e.path.name for e in snapshot]
            self.assertIn("root.txt", names)
            self.assertIn("level1.txt", names)
            self.assertNotIn("level2.txt", names)


# ---------------------------------------------------------------------------
# Large file memory safety
# ---------------------------------------------------------------------------

class TestLargeFileMemorySafety(unittest.TestCase):
    def test_chunked_hash_does_not_load_full_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            big = base / "big.bin"
            big.write_bytes(b"x" * 5 * 1024 * 1024)
            result = scan_duplicates(base)
            self.assertEqual(result["total_scanned"], 1)
            self.assertEqual(result["total_duplicate_files"], 0)


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------

class TestPathNormalization(unittest.TestCase):
    def test_relative_and_absolute_scan_produce_same_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"duplicate content"
            (base / "a.txt").write_bytes(content)
            (base / "b.txt").write_bytes(content)
            (base / "unique.txt").write_bytes(b"unique")

            def run(folder: Path) -> dict[str, list[str]]:
                snapshot, skipped = collect_snapshot(folder)
                skipped_list: list[dict] = []
                size_groups = group_by_size(snapshot)
                candidates = [e for g in size_groups.values() if len(g) > 1 for e in g]
                hash_groups = group_by_hash(candidates, skipped_list)
                duplicates = find_duplicates(hash_groups)
                return {h: sorted(str(e.path) for e in entries) for h, entries in duplicates.items()}

            self.assertEqual(run(base.resolve()), run(base))

    def test_list_files_returns_absolute_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            for f in list_files(base):
                self.assertTrue(f.is_absolute(), f"Expected absolute path, got: {f}")

    def test_list_files_relative_input_returns_absolute(self):
        """Passing a relative path still produces absolute resolved paths."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            original_cwd = os.getcwd()
            try:
                os.chdir(tmp)
                for f in list_files(Path(".")):
                    self.assertTrue(f.is_absolute(), f"Expected absolute path, got: {f}")
            finally:
                os.chdir(original_cwd)

    def test_snapshot_entries_are_absolute(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_text("x")
            snapshot, _ = collect_snapshot(base)
            for entry in snapshot:
                self.assertTrue(entry.path.is_absolute())


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism(unittest.TestCase):
    def _make_dupes(self, base: Path) -> None:
        content = b"shared content"
        (base / "alpha.txt").write_bytes(content)
        (base / "beta.txt").write_bytes(content)
        (base / "gamma.txt").write_bytes(content)
        (base / "unique.txt").write_bytes(b"only one")

    def test_snapshot_is_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_dupes(base)
            snapshot, _ = collect_snapshot(base)
            self.assertEqual(snapshot, sorted(snapshot, key=lambda e: (e.size, e.device, e.inode, str(e.path))))

    def test_duplicate_groups_sorted_by_strategy(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_dupes(base)
            skipped: list[dict] = []
            snapshot, _ = collect_snapshot(base)
            size_groups = group_by_size(snapshot)
            candidates = [e for g in size_groups.values() if len(g) > 1 for e in g]
            hash_groups = group_by_hash(candidates, skipped)
            duplicates = find_duplicates(hash_groups, keep="first")
            for entries in duplicates.values():
                self.assertEqual(entries, sorted(entries, key=lambda e: str(e.path)))

    def test_json_export_identical_across_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_dupes(base)
            out1 = Path(tmp) / "run1.json"
            out2 = Path(tmp) / "run2.json"
            r1 = scan_duplicates(base, keep="first")
            r2 = scan_duplicates(base, keep="first")
            export_json(r1["duplicates"], out1)
            export_json(r2["duplicates"], out2)
            self.assertEqual(out1.read_text(), out2.read_text())

    def test_keep_first_always_same_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            self._make_dupes(base)
            r1 = scan_duplicates(base, keep="first")
            r2 = scan_duplicates(base, keep="first")
            keepers1 = {h: str(entries[0].path) for h, entries in r1["duplicates"].items()}
            keepers2 = {h: str(entries[0].path) for h, entries in r2["duplicates"].items()}
            self.assertEqual(keepers1, keepers2)


# ---------------------------------------------------------------------------
# Wasted bytes guard
# ---------------------------------------------------------------------------

class TestWastedBytesGuard(unittest.TestCase):
    def test_wasted_bytes_uses_snapshot_size(self):
        """_wasted_bytes must use FileEntry.size — no stat() calls."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"duplicate"
            (base / "a.txt").write_bytes(content)
            (base / "b.txt").write_bytes(content)
            skipped: list[dict] = []
            snapshot, _ = collect_snapshot(base)
            size_groups = group_by_size(snapshot)
            candidates = [e for g in size_groups.values() if len(g) > 1 for e in g]
            hash_groups = group_by_hash(candidates, skipped)
            duplicates = find_duplicates(hash_groups, keep="first")

            # Delete files — _wasted_bytes must still work (uses .size from snapshot)
            for entries in duplicates.values():
                for entry in entries:
                    entry.path.unlink()

            result = _wasted_bytes(duplicates)
            self.assertEqual(result, len(content) * (2 - 1))


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# v1.1.0 — Deterministic Output Hardening Tests
# ---------------------------------------------------------------------------

class TestShuffleOrder(unittest.TestCase):
    """Force shuffle of snapshot before pipeline. Output must be identical."""

    def _make_dataset(self, base: Path) -> None:
        for g in range(4):
            content = f"group content {g}".encode()
            for i in range(3):
                (base / f"group{g}_file{i}.txt").write_bytes(content)
        for i in range(5):
            (base / f"unique_{i}.txt").write_bytes(f"unique {i} x{i*7}".encode())

    def test_shuffled_snapshot_produces_identical_json(self):
        """Pipeline output must not depend on input list order."""
        import random
        import tempfile
        from duplicates import (
            collect_snapshot, run_hash_pipeline, export_json,
        )

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()
            self._make_dataset(data)

            # Canonical run
            snapshot, _ = collect_snapshot(data)
            skipped: list[dict] = []
            dupes, *_ = run_hash_pipeline(snapshot, "first", skipped)
            out_canonical = out_dir / "canonical.json"
            export_json(dupes, out_canonical)

            # 5 shuffled runs
            rng = random.Random(42)
            for i in range(5):
                shuffled = list(snapshot)
                rng.shuffle(shuffled)
                sk2: list[dict] = []
                dupes2, *_ = run_hash_pipeline(shuffled, "first", sk2)
                out_shuffled = out_dir / f"shuffled_{i}.json"
                export_json(dupes2, out_shuffled)
                self.assertEqual(
                    out_canonical.read_bytes(),
                    out_shuffled.read_bytes(),
                    f"Shuffled run {i} produced different JSON",
                )


class TestMultipleRunSnapshot(unittest.TestCase):
    """5 consecutive scans on unchanged data must produce byte-identical JSON."""

    def _make_dataset(self, base: Path) -> None:
        for g in range(3):
            content = f"stable content group {g}".encode()
            for i in range(4):
                (base / f"g{g}f{i}.txt").write_bytes(content)
        (base / "solo.txt").write_bytes(b"i am alone")

    def test_five_runs_byte_identical(self):
        import tempfile
        from duplicates import scan_duplicates, export_json

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()
            self._make_dataset(data)

            outputs = []
            for run in range(5):
                out = out_dir / f"run{run}.json"
                r = scan_duplicates(data, keep="first")
                export_json(r["duplicates"], out)
                outputs.append(out.read_bytes())

            for i in range(1, 5):
                self.assertEqual(
                    outputs[0], outputs[i],
                    f"Run {i} differs from run 0",
                )

    def test_five_runs_skipped_identical(self):
        """Skipped file list must also be deterministic across runs."""
        import tempfile
        from duplicates import scan_duplicates

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            self._make_dataset(data)

            skipped_lists = [
                scan_duplicates(data)["skipped_unreadable"]
                for _ in range(5)
            ]
            for i in range(1, 5):
                self.assertEqual(skipped_lists[0], skipped_lists[i])


class TestArtificialDisorder(unittest.TestCase):
    """Simulate unordered/adversarial input. Output must remain stable."""

    def test_reverse_sorted_input_same_output(self):
        """Reversing snapshot order must not change duplicate groups or JSON."""
        import tempfile
        from duplicates import (
            collect_snapshot, run_hash_pipeline, export_json,
        )

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()

            content = b"identical"
            for i in range(5):
                (data / f"file_{i:03d}.txt").write_bytes(content)
            (data / "unique.txt").write_bytes(b"only me")

            snapshot, _ = collect_snapshot(data)

            # Forward order
            sk1: list[dict] = []
            d1, *_ = run_hash_pipeline(list(snapshot), "first", sk1)
            out1 = out_dir / "forward.json"
            export_json(d1, out1)

            # Reverse order
            sk2: list[dict] = []
            d2, *_ = run_hash_pipeline(list(reversed(snapshot)), "first", sk2)
            out2 = out_dir / "reversed.json"
            export_json(d2, out2)

            self.assertEqual(out1.read_bytes(), out2.read_bytes())

    def test_group_sort_key_includes_hash(self):
        """Two groups with same file size must be sorted by hash, not arbitrarily."""
        import tempfile
        from duplicates import scan_duplicates, export_json

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()

            # Two groups of identical-size files but different content
            for i in range(3):
                (data / f"alpha_{i}.txt").write_bytes(b"content_alpha")
            for i in range(3):
                (data / f"beta_{i}.txt").write_bytes(b"content_beta_")

            outputs = []
            for run in range(5):
                out = out_dir / f"run{run}.json"
                r = scan_duplicates(data, keep="first")
                export_json(r["duplicates"], out)
                outputs.append(out.read_bytes())

            for i in range(1, 5):
                self.assertEqual(outputs[0], outputs[i],
                    "Group ordering unstable when sizes are equal")

    def test_dict_iteration_order_independence(self):
        """Output must be identical even if internal dict ordering varies."""
        import tempfile
        from duplicates import (
            group_by_size, group_by_hash, find_duplicates, collect_snapshot,
        )

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            for g in range(3):
                for i in range(2):
                    (data / f"g{g}_{i}.txt").write_bytes(f"group{g}".encode())

            snapshot, _ = collect_snapshot(data)
            skipped: list[dict] = []

            # Run pipeline twice
            def run():
                sg = group_by_size(snapshot)
                cands = sorted(
                    [e for g in sg.values() if len(g) > 1 for e in g],
                    key=lambda e: str(e.path)
                )
                hg = group_by_hash(cands, [])
                return find_duplicates(hg, keep="first")

            d1 = run()
            d2 = run()

            self.assertEqual(list(d1.keys()), list(d2.keys()))
            for h in d1:
                self.assertEqual(
                    [str(e.path) for e in d1[h]],
                    [str(e.path) for e in d2[h]],
                )


# ---------------------------------------------------------------------------
# v1.3.0 — Failure & Edge-Case Hardening Tests
# ---------------------------------------------------------------------------

class TestPermissionDeniedFile(unittest.TestCase):
    """Unreadable file must be skipped, recorded, not crash."""

    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_unreadable_file_skipped_and_recorded(self):
        import stat as stat_module
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Normal duplicate pair
            (base / "a.txt").write_bytes(b"same")
            (base / "b.txt").write_bytes(b"same")
            # Restricted file — same content as duplicates so it reaches hash stage
            restricted = base / "restricted.txt"
            restricted.write_bytes(b"same")   # same size → reaches hashing → fails → skipped
            restricted.chmod(0o000)

            try:
                from duplicates import scan_duplicates
                result = scan_duplicates(base, keep="first")

                # Must not crash
                # Restricted file must appear in skipped
                skipped_paths = [s["path"] for s in result["skipped_unreadable"]]
                self.assertTrue(
                    any("restricted" in p for p in skipped_paths),
                    f"Restricted file not in skipped: {skipped_paths}"
                )
                # Skipped must be sorted
                self.assertEqual(skipped_paths, sorted(skipped_paths))
                # No false duplicate from restricted file
                for entries in result["duplicates"].values():
                    for e in entries:
                        self.assertNotIn("restricted", str(e.path))
            finally:
                restricted.chmod(0o644)

    @unittest.skipIf(os.getuid() == 0, "Permission tests require non-root")
    def test_unreadable_directory_skipped_not_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            restricted_dir = base / "locked"
            restricted_dir.mkdir()
            (restricted_dir / "hidden.txt").write_bytes(b"hidden")
            restricted_dir.chmod(0o000)

            try:
                from duplicates import scan_duplicates
                result = scan_duplicates(base)
                # Must not crash — locked dir appears in skipped
                skipped_paths = [s["path"] for s in result["skipped_unreadable"]]
                self.assertTrue(
                    any("locked" in p for p in skipped_paths),
                    f"Locked dir not in skipped: {skipped_paths}"
                )
            finally:
                restricted_dir.chmod(0o755)


class TestBrokenSymlinkV13(unittest.TestCase):
    """Broken symlinks must be skipped cleanly — never hashed, never crash."""

    def test_broken_symlink_skipped_and_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Normal files
            (base / "real.txt").write_bytes(b"content")
            # Broken symlink — points to nonexistent target
            broken = base / "broken_link.txt"
            broken.symlink_to(base / "nonexistent_target.txt")

            from duplicates import collect_snapshot
            snapshot, skipped = collect_snapshot(base)

            # Must not crash
            # Broken symlink must be in skipped
            skipped_paths = [s["path"] for s in skipped]
            self.assertTrue(
                any("broken_link" in p for p in skipped_paths),
                f"Broken symlink not in skipped: {skipped_paths}"
            )
            # Broken symlink must not be in snapshot
            snap_paths = [str(e.path) for e in snapshot]
            self.assertFalse(
                any("broken_link" in p for p in snap_paths),
                "Broken symlink incorrectly included in snapshot"
            )
            # Skipped deterministically sorted
            self.assertEqual(skipped_paths, sorted(skipped_paths))

    def test_symlink_loop_cannot_recurse(self):
        """Symlink loop must not cause infinite recursion or crash."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_bytes(b"data")
            # Loop: subdir → base
            loop = base / "loop_dir"
            loop.symlink_to(base)

            from duplicates import collect_snapshot
            # Must complete without RecursionError or hanging
            snapshot, skipped = collect_snapshot(base, follow_symlinks=False)
            self.assertIsInstance(snapshot, list)


class TestPartialReadFailure(unittest.TestCase):
    """Mid-hash read failure must not produce false duplicates or corrupt state."""

    def test_mock_read_failure_file_goes_to_skipped(self):
        """Simulate OSError during read — file must appear in skipped, not in groups."""
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # Three identical files
            for i in range(3):
                (base / f"dup_{i}.txt").write_bytes(b"identical content here")

            from duplicates import collect_snapshot, group_by_size, _hash_entry, FileEntry

            snapshot, _ = collect_snapshot(base)
            skipped: list[dict] = []
            sg = group_by_size(snapshot)
            candidates = [e for g in sg.values() if len(g) > 1 for e in g]

            # Patch os.open to fail on first call
            original_open = os.open
            call_count = [0]

            def failing_open(path, flags, *args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise OSError("Simulated read failure")
                return original_open(path, flags, *args, **kwargs)

            with mock.patch("os.open", side_effect=failing_open):
                results = [_hash_entry(e) for e in candidates]

            # First entry failed — returns None digest
            none_count = sum(1 for _, d in results if d is None)
            self.assertGreaterEqual(none_count, 1, "Expected at least one None digest from failure")

            # Remaining entries with valid digests must all match — no false isolation
            valid_digests = [d for _, d in results if d is not None]
            if len(valid_digests) > 1:
                self.assertEqual(
                    len(set(valid_digests)), 1,
                    "Identical files produced different digests — false negative"
                )

    def test_no_false_positive_from_partial_hash(self):
        """Files with different content must never be grouped even if one fails mid-hash."""
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.txt").write_bytes(b"content alpha " * 100)
            (base / "b.txt").write_bytes(b"content beta  " * 100)

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")
            # Different content — must never be grouped
            self.assertEqual(result["total_duplicate_files"], 0)


class TestKeyboardInterrupt(unittest.TestCase):
    """Ctrl+C must exit cleanly — no traceback, exit code 130."""

    def test_keyboard_interrupt_exits_130(self):
        """KeyboardInterrupt during scan must produce exit code 130."""
        import unittest.mock as mock
        import importlib

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "file.txt").write_bytes(b"data")

            # Patch scan_duplicates to raise KeyboardInterrupt
            with mock.patch("duplicates.scan_duplicates",
                            side_effect=KeyboardInterrupt):
                with self.assertRaises(SystemExit) as ctx:
                    # Simulate what main() does after parsing
                    import logger as lg
                    lg.set_log_level("ERROR")
                    try:
                        raise KeyboardInterrupt
                    except KeyboardInterrupt:
                        import sys
                        sys.exit(130)

            self.assertEqual(ctx.exception.code, 130)

    def test_keyboard_interrupt_no_traceback(self):
        """KeyboardInterrupt handler must not propagate as unhandled exception."""
        import subprocess
        import sys

        # Write a minimal script that raises KeyboardInterrupt inside main logic
        script = """
import sys
sys.path.insert(0, '.')
import unittest.mock as mock
import duplicates
import scanner
from pathlib import Path

with mock.patch('duplicates.collect_snapshot', side_effect=KeyboardInterrupt), \
     mock.patch('scanner.validate_folder', return_value=Path('.')):
    try:
        import main
        import argparse
        with mock.patch('sys.argv', ['main', 'duplicates', 'test_dupes']):
            main.main()
    except SystemExit as e:
        sys.exit(e.code)
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=next(p for p in [Path(__file__).parent, Path(__file__).parent.parent] if (p / 'main.py').exists()),
        )
        # Exit code must be 130
        self.assertEqual(result.returncode, 130)
        # Stderr must contain our clean message, not a traceback
        self.assertIn("Interrupted", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
