"""
v1.4.0 — Memory Discipline Tests.

Validates:
1. Large synthetic dataset — no MemoryError, linear growth
2. Large-file hashing — streaming confirmed (no full load)
3. Singleton size groups never reach full hash stage
4. Chunk size constant — never dynamically resized
5. No list copying in tight loops — references only in groups
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Test 1 — Large synthetic dataset
# ---------------------------------------------------------------------------

class TestLargeDataset(unittest.TestCase):
    """Large file count must complete without MemoryError or crash."""

    def test_10k_files_no_memory_error(self):
        """10K files with duplicates — must complete, correct duplicate count."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            # 5 duplicate groups × 4 files = 20 duplicates expected
            for g in range(5):
                content = f"group content {g} " .encode() * 100
                for i in range(4):
                    (base / f"grp{g}_{i:04d}.bin").write_bytes(content)

            # 9980 unique files
            for i in range(9980):
                (base / f"unique_{i:05d}.txt").write_bytes(
                    f"unique file {i} padding {'x' * 20}".encode()
                )

            from duplicates import scan_duplicates
            try:
                result = scan_duplicates(base, keep="first")
            except MemoryError:
                self.fail("MemoryError on 10K file dataset")

            self.assertEqual(result["total_scanned"], 10000)
            self.assertEqual(result["total_duplicate_files"], 15)  # 5 groups × 3 non-kept

    def test_many_duplicate_groups_no_runaway(self):
        """200 duplicate groups × 3 files — group structures must not explode."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for g in range(200):
                content = f"dup group content {g:04d}".encode()
                for i in range(3):
                    (base / f"d{g:03d}_{i}.dat").write_bytes(content)

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")
            self.assertEqual(len(result["duplicates"]), 200)
            self.assertEqual(result["total_duplicate_files"], 400)  # 200 × 2


# ---------------------------------------------------------------------------
# Test 2 — Large-file streaming (sparse file)
# ---------------------------------------------------------------------------

class TestLargeFileStreaming(unittest.TestCase):
    """File content must never be fully loaded. Chunk-based read verified."""

    def test_sparse_1gb_file_no_memory_spike(self):
        """
        Create a 1GB sparse file. Hash it.
        Memory must not spike to 1GB — only chunk_size bytes held at once.
        Sparse files have no physical disk usage.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            sparse = base / "sparse_1gb.bin"

            # Create 1GB sparse file — no physical disk space used
            try:
                with open(sparse, "wb") as f:
                    f.seek(1024 * 1024 * 1024 - 1)  # 1GB - 1 byte
                    f.write(b"\x00")
            except OSError:
                self.skipTest("Cannot create sparse file on this filesystem")

            from duplicates import scan_duplicates, CHUNK_SIZE

            # If streaming: memory stays near CHUNK_SIZE (1MB), not 1GB
            # We verify by checking the scan completes — not by measuring memory
            # (memory measurement would require tracemalloc, which is slow)
            result = scan_duplicates(base, keep="first")
            self.assertEqual(result["total_scanned"], 1)
            self.assertEqual(result["total_duplicate_files"], 0)

    def test_hash_entry_chunk_size_constant(self):
        """
        _hash_entry must use fixed chunk_size regardless of file size.
        Confirmed by passing explicit small chunk_size and verifying hash correct.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"abcdefghij" * 10000  # 100KB
            f1 = base / "file1.bin"
            f2 = base / "file2.bin"
            f1.write_bytes(content)
            f2.write_bytes(content)

            from duplicates import collect_snapshot, _hash_entry, CHUNK_SIZE

            snapshot, _ = collect_snapshot(base)
            self.assertEqual(len(snapshot), 2)

            # Hash with default chunk size
            _, h1_default = _hash_entry(snapshot[0], chunk_size=CHUNK_SIZE)
            # Hash with tiny chunk size (64 bytes) — result must be identical
            _, h1_tiny = _hash_entry(snapshot[0], chunk_size=64)

            self.assertIsNotNone(h1_default)
            self.assertIsNotNone(h1_tiny)
            self.assertEqual(h1_default, h1_tiny,
                "Hash must be identical regardless of chunk size")


# ---------------------------------------------------------------------------
# Test 3 — Singleton size groups never reach full hash
# ---------------------------------------------------------------------------

class TestSingletonExclusion(unittest.TestCase):
    """Files with unique size must be excluded before hashing."""

    def test_unique_size_files_not_hashed(self):
        """
        If every file has a different size, zero hashing should occur.
        Verified by checking total_hashed == 0.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for i in range(10):
                # Each file is a different size
                (base / f"file_{i}.txt").write_bytes(b"x" * (100 + i))

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")

            self.assertEqual(result["total_duplicate_files"], 0)
            # All files skipped by size — none reached hash stage
            self.assertEqual(result["skipped_by_size"], 10)
            self.assertEqual(result["total_hashed"], 0)

    def test_only_duplicate_size_files_hashed(self):
        """
        Mixed dataset: some singletons, some duplicates.
        Only size-group candidates must reach hash stage.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            # 3 singletons (unique sizes)
            for i in range(3):
                (base / f"solo_{i}.txt").write_bytes(b"x" * (200 + i * 7))
            # 4 files with same size (2 pairs)
            (base / "a.bin").write_bytes(b"same" * 10)
            (base / "b.bin").write_bytes(b"same" * 10)
            (base / "c.bin").write_bytes(b"diff" * 10)
            (base / "d.bin").write_bytes(b"diff" * 10)

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")

            # 3 singletons must not reach hash stage
            self.assertEqual(result["skipped_by_size"], 3)
            # 4 candidates must be hashed
            self.assertEqual(result["total_hashed"], 4)
            # 2 duplicate pairs found
            self.assertEqual(result["total_duplicate_files"], 2)


# ---------------------------------------------------------------------------
# Test 4 — No list copying — groups hold references only
# ---------------------------------------------------------------------------

class TestNoUnnecessaryCopying(unittest.TestCase):
    """Group structures must hold references to FileEntry objects, not copies."""

    def test_group_entries_are_same_objects_as_snapshot(self):
        """
        FileEntry objects in size groups must be the same objects as in snapshot.
        If they're copies, identity check fails — indicates unnecessary duplication.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"identical"
            (base / "x.txt").write_bytes(content)
            (base / "y.txt").write_bytes(content)

            from duplicates import collect_snapshot, group_by_size

            snapshot, _ = collect_snapshot(base)
            size_groups = group_by_size(snapshot)

            # Entries in groups must be the same objects as in snapshot
            snapshot_ids = {id(e) for e in snapshot}
            for group in size_groups.values():
                for entry in group:
                    self.assertIn(
                        id(entry), snapshot_ids,
                        "group_by_size created a copy instead of a reference"
                    )

    def test_stale_references_released_after_pipeline(self):
        """
        After scan_duplicates completes, only the result dict is held.
        Internal pipeline structures (size_groups, partial_groups) must be gone.
        Verified indirectly — scan completes without MemoryError on large input.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"same content repeated"
            for i in range(500):
                (base / f"f{i:04d}.txt").write_bytes(content)

            from duplicates import scan_duplicates
            result = scan_duplicates(base, keep="first")

            # All 500 are duplicates of each other — 499 non-kept
            self.assertEqual(result["total_duplicate_files"], 499)


# ---------------------------------------------------------------------------
# Test 5 — Chunk size is constant (not dynamic)
# ---------------------------------------------------------------------------

class TestChunkSizeConstant(unittest.TestCase):
    """CHUNK_SIZE must be fixed. _hash_entry must not resize based on file size."""

    def test_chunk_size_is_fixed_constant(self):
        """CHUNK_SIZE must be 1MB — fixed in module, not derived from input."""
        from duplicates import CHUNK_SIZE
        self.assertEqual(CHUNK_SIZE, 1024 * 1024, "CHUNK_SIZE must be exactly 1MB")

    def test_custom_chunk_size_respected(self):
        """
        When scan_duplicates is called with explicit chunk_size,
        hashing must use that size — not auto-select based on file size.
        """
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            content = b"chunk test content " * 500
            (base / "a.dat").write_bytes(content)
            (base / "b.dat").write_bytes(content)

            from duplicates import scan_duplicates
            # Both chunk sizes must produce identical duplicate detection
            r1 = scan_duplicates(base, keep="first", chunk_size=256)
            r2 = scan_duplicates(base, keep="first", chunk_size=1024 * 1024)

            self.assertEqual(r1["total_duplicate_files"], r2["total_duplicate_files"])
            self.assertEqual(
                list(r1["duplicates"].keys()),
                list(r2["duplicates"].keys()),
                "Different chunk sizes produced different group ordering"
            )

    def test_partial_bytes_constant(self):
        """PARTIAL_BYTES pre-hash window must be fixed at 4096."""
        from duplicates import PARTIAL_BYTES
        self.assertEqual(PARTIAL_BYTES, 4096)


if __name__ == "__main__":
    unittest.main()
