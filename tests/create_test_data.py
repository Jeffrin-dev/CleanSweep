"""
CleanSweep create_test_data.py — Test Data Generator.

Functions:
  create_test_dupes(base)            — Duplicate detection dataset
  create_test_config(base)           — Config + ignore-rules dataset
  create_test_dry(base)              — Organizer dry-run dataset
  create_stress_tree(base, ...)      — Large-scale stress test dataset (v2.9)
  create_duplicate_stress(base, ...) — Large-scale duplicate stress dataset (v2.9)

v2.9 additions:
  create_stress_tree() supports generation of 1M+ files for performance baseline
  testing. Used by test_integration.py TestStress class and standalone benchmarks.
"""

from pathlib import Path
import os
import time


def create_test_dupes(base: Path = Path("test_dupes")) -> None:
    """Duplicate detection test data — known duplicate groups."""
    base.mkdir(parents=True, exist_ok=True)
    (base / "subdir").mkdir(exist_ok=True)

    # Duplicate group — 3 copies of same content
    dup_content = b"duplicate file content"
    (base / "a.txt").write_bytes(dup_content)
    (base / "b.txt").write_bytes(dup_content)
    (base / "subdir" / "c.txt").write_bytes(dup_content)

    # Unique files
    (base / "d.txt").write_bytes(b"unique content alpha")
    (base / "subdir" / "e.txt").write_bytes(b"unique content beta")

    print(f"Created: {base.resolve()}")


def create_test_config(base: Path = Path("test_config")) -> None:
    """Config + ignore-rules test data."""
    dirs = [base, base / "docs", base / "node_modules", base / ".git"]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    (base / "README.md").write_text("readme content\n")
    (base / "report.pdf").write_text("important document\n")
    (base / "data.xlsx").write_text("spreadsheet data\n")
    (base / "unique.txt").write_text("unique file\n")

    duplicate_content = "same content\n"
    (base / "original.txt").write_text(duplicate_content)
    (base / "copy.txt").write_text(duplicate_content)
    (base / "docs" / "another_copy.txt").write_text(duplicate_content)

    (base / "debug.log").write_text("debug output\n")
    (base / "session.tmp").write_text("temp file\n")
    (base / "node_modules" / "package.js").write_text("dependency\n")
    (base / ".git" / "HEAD").write_text("git object\n")

    print(f"Created: {base.resolve()}")


def create_test_dry(base: Path = Path("test_dry")) -> None:
    """Organizer dry-run test data — mixed file types."""
    base.mkdir(parents=True, exist_ok=True)

    (base / "photo.jpg").write_bytes(b"fake jpeg")
    (base / "report.pdf").write_bytes(b"fake pdf")
    (base / "script.py").write_text("print('hello')\n")
    (base / "notes.txt").write_text("some notes\n")
    (base / "data.csv").write_text("a,b,c\n1,2,3\n")
    (base / "video.mp4").write_bytes(b"fake mp4")
    (base / "archive.zip").write_bytes(b"fake zip")

    print(f"Created: {base.resolve()}")


# ---------------------------------------------------------------------------
# v2.9 — Large-scale stress test generators
# ---------------------------------------------------------------------------

_STRESS_EXTENSIONS = [
    ".jpg", ".jpeg", ".png",          # Images
    ".pdf", ".txt", ".md", ".docx",   # Documents
    ".mp4", ".mkv", ".avi",           # Videos
    ".mp3", ".wav", ".flac",          # Audio
    ".zip", ".tar", ".gz",            # Archives
    ".py", ".js", ".json", ".yaml",   # Code
    ".bin", ".dat", ".xyz",           # Others
]


def create_stress_tree(
    base:            Path = Path("stress_tree"),
    n_dirs:          int  = 1000,
    n_files_per_dir: int  = 1000,
    file_size_bytes: int  = 128,
    verbose:         bool = True,
) -> int:
    """
    Generate a large directory tree for traversal and organize stress testing.

    Structure:
      base/
        dir_00000/
          file_00000.jpg
          file_00001.pdf
          ...
          file_{n_files_per_dir-1}.xyz
        dir_00001/
          ...
        ...
        dir_{n_dirs-1}/

    File contents: a single repeated byte — minimal disk I/O, maximum file count.
    Extensions rotate through _STRESS_EXTENSIONS so the rule engine sees all types.

    Args:
      base:            output directory (created if absent, must be empty)
      n_dirs:          number of subdirectories (1000 → 1000 × 1000 = 1M)
      n_files_per_dir: files per subdirectory
      file_size_bytes: bytes per file (default 128 — minimal disk use)
      verbose:         print progress every 10% of dirs created

    Returns:
      Total number of files created.

    Disk space estimate:
      n_dirs × n_files_per_dir × file_size_bytes bytes
      e.g. 1000 × 1000 × 128 = 128 MB for 1M files
    """
    base.mkdir(parents=True, exist_ok=True)

    total_files    = n_dirs * n_files_per_dir
    n_exts         = len(_STRESS_EXTENSIONS)
    progress_every = max(1, n_dirs // 10)
    t_start        = time.perf_counter()
    files_created  = 0

    if verbose:
        print(
            f"[stress_tree] Generating {n_dirs:,} dirs × "
            f"{n_files_per_dir:,} files = {total_files:,} files "
            f"({file_size_bytes} bytes each) → {base.resolve()}"
        )

    for d_idx in range(n_dirs):
        d = base / f"dir_{d_idx:06d}"
        d.mkdir(exist_ok=True)

        for f_idx in range(n_files_per_dir):
            ext  = _STRESS_EXTENSIONS[(d_idx + f_idx) % n_exts]
            byte = bytes([(d_idx + f_idx) % 256])
            (d / f"file_{f_idx:06d}{ext}").write_bytes(byte * file_size_bytes)
            files_created += 1

        if verbose and (d_idx + 1) % progress_every == 0:
            elapsed  = time.perf_counter() - t_start
            rate     = files_created / elapsed if elapsed > 0 else 0
            pct      = 100 * (d_idx + 1) / n_dirs
            eta_secs = (total_files - files_created) / rate if rate > 0 else 0
            print(
                f"  {pct:5.1f}%  {files_created:>10,} files  "
                f"{rate:,.0f} files/s  ETA {eta_secs:.0f}s"
            )

    elapsed = time.perf_counter() - t_start
    rate    = files_created / elapsed if elapsed > 0 else 0

    if verbose:
        print(
            f"[stress_tree] Done: {files_created:,} files in {elapsed:.1f}s "
            f"({rate:,.0f} files/s)"
        )
    return files_created


def create_duplicate_stress(
    base:              Path = Path("stress_dupes"),
    n_unique:          int  = 50000,
    n_dup_groups:      int  = 1000,
    n_copies_per_group: int  = 5,
    file_size_bytes:   int  = 4096,
    verbose:           bool = True,
) -> dict:
    """
    Generate a large dataset for duplicate detection stress testing.

    Creates a mix of unique files and known duplicate groups to exercise
    all three hashing pipeline phases under load.

    Args:
      base:               output directory
      n_unique:           number of unique (non-duplicate) files
      n_dup_groups:       number of distinct duplicate groups
      n_copies_per_group: copies per duplicate group (>= 2)
      file_size_bytes:    bytes per file (default 4096 — exercises chunked hashing)
      verbose:            print progress summary

    Returns:
      dict with keys: total_files, unique_count, duplicate_groups,
                      expected_duplicate_files, expected_wasted_bytes

    Total files = n_unique + (n_dup_groups × n_copies_per_group)
    Expected duplicates found = n_dup_groups groups, each of size n_copies_per_group
    """
    base.mkdir(parents=True, exist_ok=True)

    total = n_unique + n_dup_groups * n_copies_per_group
    t_start = time.perf_counter()

    if verbose:
        print(
            f"[dup_stress] Generating {n_unique:,} unique + "
            f"{n_dup_groups:,} groups × {n_copies_per_group} = "
            f"{total:,} total files → {base.resolve()}"
        )

    # Unique files — each gets distinct content
    for i in range(n_unique):
        content = bytes([(i * 7 + 13) % 256]) * file_size_bytes
        (base / f"unique_{i:08d}.bin").write_bytes(content)

    # Duplicate groups — identical content within each group
    for g in range(n_dup_groups):
        content = bytes([(g * 31 + 97) % 256]) * file_size_bytes
        for c in range(n_copies_per_group):
            (base / f"dup_g{g:06d}_c{c:03d}.bin").write_bytes(content)

    elapsed = time.perf_counter() - t_start
    rate    = total / elapsed if elapsed > 0 else 0

    if verbose:
        print(
            f"[dup_stress] Done: {total:,} files in {elapsed:.1f}s "
            f"({rate:,.0f} files/s)"
        )

    return {
        "total_files":             total,
        "unique_count":            n_unique,
        "duplicate_groups":        n_dup_groups,
        "expected_duplicate_files": n_dup_groups * (n_copies_per_group - 1),
        "expected_wasted_bytes":   n_dup_groups * (n_copies_per_group - 1) * file_size_bytes,
    }


if __name__ == "__main__":
    import sys

    if "--stress" in sys.argv:
        # Large-scale stress mode — creates 1M files
        # Usage: python3 create_test_data.py --stress [--dirs N] [--files N]
        args = sys.argv[1:]
        n_dirs  = int(args[args.index("--dirs")  + 1]) if "--dirs"  in args else 1000
        n_files = int(args[args.index("--files") + 1]) if "--files" in args else 1000
        create_stress_tree(n_dirs=n_dirs, n_files_per_dir=n_files)
        create_duplicate_stress()
    elif "--dup-stress" in sys.argv:
        create_duplicate_stress()
    else:
        create_test_dupes()
        create_test_config()
        create_test_dry()
        print("\nAll standard test data ready.")
        print("Try:")
        print("  python3 main.py scan test_dupes")
        print("  python3 main.py duplicates test_dupes")
        print("  python3 main.py organize test_dry --dry-run")
        print()
        print("For 1M-file stress test:")
        print("  python3 create_test_data.py --stress")
        print("  python3 create_test_data.py --stress --dirs 100 --files 100")
