import unittest
import tempfile
from pathlib import Path
from duplicates import (
    collect_snapshot, group_by_size, group_by_hash,
    find_duplicates, FileEntry,
)


class TestFindDuplicates(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = Path(self.temp_dir.name)

        # Duplicate group 1
        content_a = b"duplicate content A"
        (base / "a1.txt").write_bytes(content_a)
        (base / "a2.txt").write_bytes(content_a)

        # Duplicate group 2 (larger file)
        content_b = b"x" * 10000
        (base / "b1.bin").write_bytes(content_b)
        (base / "b2.bin").write_bytes(content_b)
        (base / "b3.bin").write_bytes(content_b)

        # Unique file
        (base / "unique.txt").write_text("I am unique")

        self.base = base

    def tearDown(self):
        self.temp_dir.cleanup()

    def _run_pipeline(self) -> dict[str, list[FileEntry]]:
        snapshot, skipped = collect_snapshot(self.base)
        skipped_list: list[dict] = []
        size_groups = group_by_size(snapshot)
        candidates = [
            e for group in size_groups.values()
            if len(group) > 1
            for e in group
        ]
        hash_groups = group_by_hash(candidates, skipped_list)
        return find_duplicates(hash_groups)

    def test_detects_duplicates(self):
        duplicates = self._run_pipeline()
        self.assertEqual(len(duplicates), 2)
        group_sizes = sorted(len(g) for g in duplicates.values())
        self.assertEqual(group_sizes, [2, 3])

    def test_no_false_positive(self):
        duplicates = self._run_pipeline()
        all_dup_paths = {e.path for group in duplicates.values() for e in group}
        self.assertNotIn((self.base / "unique.txt").resolve(), all_dup_paths)

    def test_empty_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshot, _ = collect_snapshot(Path(tmp))
            duplicates = find_duplicates(group_by_hash([], []))
            self.assertEqual(duplicates, {})

    def test_all_unique(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "only.txt").write_text("unique")
            snapshot, _ = collect_snapshot(Path(tmp))
            skipped: list[dict] = []
            size_groups = group_by_size(snapshot)
            candidates = [e for g in size_groups.values() if len(g) > 1 for e in g]
            duplicates = find_duplicates(group_by_hash(candidates, skipped))
            self.assertEqual(duplicates, {})
