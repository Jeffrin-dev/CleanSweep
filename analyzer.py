"""
CleanSweep v2.6.0 — Analyzer.

v2.6.0 (performance pass):
  summarize() now accepts either list[Path] or list[FileEntry].
  When FileEntry objects are supplied their pre-fetched .size field is used
  directly — zero extra stat() calls.  Path objects fall back to the
  original stat()-per-file behaviour for backward compatibility.

Architecture contract (permanent):
  No filesystem mutation.  No printing.  Returns data only.
"""

from __future__ import annotations

from pathlib import Path


def summarize(files: list) -> dict:
    """
    Return {"count": N, "total_bytes": T} for the given file collection.

    Accepts:
      list[Path]      — stat() called per file (legacy behaviour)
      list[FileEntry] — .size used directly; zero additional syscalls

    FileEntry detection is duck-typed: any object with a numeric .size
    attribute is treated as a FileEntry.  This avoids a circular import
    while preserving strict module boundaries.
    """
    if not files:
        return {"count": 0, "total_bytes": 0}

    total_bytes = 0
    first = files[0]

    if hasattr(first, "size"):
        # FileEntry path — pre-fetched metadata, no I/O
        for entry in files:
            total_bytes += entry.size
    else:
        # Path path — stat() per file (file may vanish after scan; skip silently)
        for f in files:
            try:
                total_bytes += f.stat().st_size
            except OSError:
                pass

    return {"count": len(files), "total_bytes": total_bytes}
