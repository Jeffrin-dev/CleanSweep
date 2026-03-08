from __future__ import annotations

import hashlib
import json
import os
import stat as stat_mod
import threading
import time
import logger
import timer
from collections import defaultdict
import fnmatch
from file_operation_manager import TEMP_PREFIX
from scanner import (
    ScanPolicy, ScanError,
    SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR,
    _matches_exclusion,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_SIZE    = 1024 * 1024           # 1 MB read window — fixed, never varies
PARTIAL_BYTES = 4096                  # bytes for partial pre-hash

# ---------------------------------------------------------------------------
# Worker count — I/O-bound heuristic (locked permanently)
#
# Python's recommended formula for I/O-bound thread pools:
#   min(32, cpu_count + 4)
# Rationale: threads block on disk I/O, so more than cpu_count is beneficial.
# Hard cap at 32 prevents runaway thread creation on high-core machines.
# This default is used when no explicit workers= value is provided.
# ---------------------------------------------------------------------------

_CPU_COUNT = os.cpu_count() or 1
DEFAULT_WORKERS = min(32, _CPU_COUNT + 4)
MAX_WORKERS_HARD_CAP = _CPU_COUNT * 4  # absolute ceiling regardless of user input

# Queue depth: 4× worker count — bounded submission, prevents future accumulation
_QUEUE_DEPTH_MULTIPLIER = 4


# Canonical sort key — used at every sort site, never varies
_ENTRY_KEY = lambda e: (e.size, e.device, e.inode, str(e.path))


# ---------------------------------------------------------------------------
# FileEntry — immutable metadata snapshot (~370 bytes per file)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FileEntry:
    """
    Memory footprint: ~370 bytes per file.
    At 100K files → ~37 MB. At 1M files → ~370 MB.
    No file contents stored — metadata only.
    """
    path:     Path
    size:     int
    inode:    int
    mtime_ns: int
    device:   int


# ---------------------------------------------------------------------------
# Keep strategies
# ---------------------------------------------------------------------------

KEEP_STRATEGIES: dict = {
    "oldest": lambda e: (e.mtime_ns,   str(e.path)),
    "newest": lambda e: (-e.mtime_ns,  str(e.path)),
    "first":  lambda e: str(e.path),
}


# ---------------------------------------------------------------------------
# Hash cache — safe intra-run reuse for small files
# ---------------------------------------------------------------------------

class _HashCache:
    """
    Thread-safe cache mapping (path, size, inode, mtime_ns) → hexdigest.

    Reuse contract (v2.6.0, permanent):
      A cached digest is valid only when size + inode + mtime_ns are unchanged.
      Safe reuse case: files with size <= PARTIAL_BYTES are fully read during
      the partial-hash phase, so their partial digest equals their full digest.
      The full-hash phase may consume the cache instead of re-reading the file.

    Thread-safety: all public methods acquire _lock.  In practice the pipeline
    calls get/put exclusively from the main thread (futures drain loop), so the
    lock is defensive rather than load-bearing.  It costs nothing and guards
    against future refactors.

    Scope: one instance per run_hash_pipeline() call; freed immediately after
    Phase 3 completes.  No cross-run persistence.
    """

    __slots__ = ("_lock", "_data")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[tuple[str, int, int, int], str] = {}

    def _key(self, entry: "FileEntry") -> tuple[str, int, int, int]:
        return (str(entry.path), entry.size, entry.inode, entry.mtime_ns)

    def get(self, entry: "FileEntry") -> "str | None":
        """Return cached digest, or None if absent."""
        with self._lock:
            return self._data.get(self._key(entry))

    def put(self, entry: "FileEntry", digest: str) -> None:
        """Store digest.  Overwrites any existing entry for the same key."""
        with self._lock:
            self._data[self._key(entry)] = digest


# ---------------------------------------------------------------------------
# Centralized skip recorder
# ---------------------------------------------------------------------------

def _record_skip(
    skipped: list[dict],
    path: Path | str,
    reason: str,
    log_level: str = "warn",
) -> None:
    """Single point for all skip decisions. Never raises.

    §13: Exceptions are narrowed to expected types only — never a blanket swallow.
    logger._emit() itself swallows all internal failures (§12), so log calls are safe.
    """
    try:
        path_str = path.as_posix() if isinstance(path, Path) else str(path)
        skipped.append({"path": path_str, "error": reason})
        if log_level == "warn":
            logger.log_warn(f"Skipped [{reason}]: {path_str}")
        else:
            logger.log_debug(f"Skipped [{reason}]: {path_str}")
    except (AttributeError, TypeError, ValueError, OSError) as e:
        # Path conversion or list append failed — log via debug (logger is always safe)
        logger.log_debug(f"_record_skip: failed to record skip for {path!r}: {e}")


# ---------------------------------------------------------------------------
# Phase A — Collect metadata snapshot
# ---------------------------------------------------------------------------

def collect_snapshot(
    folder:          Path,
    follow_symlinks: bool           = False,
    max_depth:       int | None     = None,
    policy:          ScanPolicy | None = None,
) -> tuple[list[FileEntry], list[dict]]:
    """
    Iterative traversal. One stat() per file. No recursion. O(N) memory.

    v2.1.0 upgrade:
      - Accepts ScanPolicy for full traversal control (symlink_policy, exclude_patterns,
        min_file_size, recursive, etc.)
      - When policy is given, all its settings apply and legacy params are ignored.
      - When policy is None, legacy params (follow_symlinks, max_depth) used for compat.
      - Cycle detection via (inode, device) set when symlink_policy="follow"
      - Glob exclusion applied to file and dir names before stat()
      - Symlink policy="error" raises ScanError immediately

    Skip policy (permanent):
      iterdir() PermissionError  → WARN + skipped
      iterdir() OSError          → WARN + skipped
      is_symlink/is_dir OSError  → DEBUG (vanished race condition)
      symlink ignored            → DEBUG + skipped
      stat() PermissionError     → WARN + skipped
      stat() OSError             → WARN + skipped
      resolve() OSError          → WARN + skipped
      cycle detected             → DEBUG + skipped
    """
    entries: list[FileEntry] = []
    skipped: list[dict] = []

    # Resolve effective traversal parameters
    if policy is not None:
        effective_symlink = policy.symlink_policy
        effective_max_depth: int | None = (
            0 if not policy.recursive else policy.max_depth
        )
        exclude_pats     = policy.exclude_patterns
        min_size         = policy.min_file_size
        ignore_ext_set   = frozenset(
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in policy.ignore_extensions
        )
        ignore_dir_set   = frozenset(policy.ignore_folders)
    else:
        # Legacy parameter path
        effective_symlink    = SYMLINK_FOLLOW if follow_symlinks else SYMLINK_IGNORE
        effective_max_depth  = max_depth
        exclude_pats         = ()
        min_size             = 0
        ignore_ext_set       = frozenset()
        ignore_dir_set       = frozenset()

    # Cycle detection: track visited real directories by (inode, device)
    visited: set[tuple[int, int]] = set()
    try:
        root_st = folder.stat()
        visited.add((root_st.st_ino, root_st.st_dev))
    except OSError as e:
        _record_skip(skipped, folder, f"root_stat_failed:{type(e).__name__}", log_level="warn")
        return [], skipped

    stack: list[tuple[Path, int]] = [(folder, 0)]

    while stack:
        current, depth = stack.pop()

        if effective_max_depth is not None and depth > effective_max_depth:
            continue

        # os.scandir() returns DirEntry objects whose is_file()/is_dir()/
        # is_symlink() consume kernel-cached inode data, and whose
        # stat(follow_symlinks=False) is also cached on Linux/macOS.
        # This eliminates one lstat() syscall per directory entry.
        try:
            with os.scandir(current) as sd:
                dir_entries = sorted(sd, key=lambda e: e.name)
        except PermissionError:
            _record_skip(skipped, current, "PermissionError", log_level="warn")
            continue
        except (FileNotFoundError, OSError) as e:
            _record_skip(skipped, current, type(e).__name__, log_level="warn")
            continue

        for de in dir_entries:

            # Skip atomic temp files
            if de.name.startswith(TEMP_PREFIX):
                continue

            try:
                is_sym  = de.is_symlink()   # cached
                is_dir  = de.is_dir()       # follows symlinks, cached
                is_file = de.is_file()      # follows symlinks, cached
            except OSError:
                logger.log_debug(f"Skipped (vanished): {de.path}")
                continue

            # Materialise Path after cheap type checks — avoids object creation
            # for entries that will be filtered out.
            child = Path(de.path)

            # ── Symlink policy ───────────────────────────────────────────
            if is_sym:
                if effective_symlink == SYMLINK_IGNORE:
                    _record_skip(skipped, child, "symlink_skipped", log_level="debug")
                    continue
                elif effective_symlink == SYMLINK_ERROR:
                    raise ScanError(
                        f"Symlink encountered at {child} — symlink_policy is 'error'."
                    )
                # SYMLINK_FOLLOW: fall through to dir/file handling below

            # ── Directory handling ────────────────────────────────────────
            if is_dir:
                # Respect ignore_folders
                if de.name in ignore_dir_set:
                    continue
                # Respect exclude_patterns for directory names
                if _matches_exclusion(de.name, exclude_pats):
                    continue
                # Cycle detection for symlinked directories
                if is_sym and effective_symlink == SYMLINK_FOLLOW:
                    try:
                        real_st = child.stat()
                        key = (real_st.st_ino, real_st.st_dev)
                        if key in visited:
                            _record_skip(skipped, child, "cycle_detected", log_level="debug")
                            continue
                        visited.add(key)
                    except OSError as e:
                        _record_skip(skipped, child, type(e).__name__, log_level="warn")
                        continue
                stack.append((child, depth + 1))
                continue

            # ── File handling ─────────────────────────────────────────────
            if not is_file:
                continue

            # Apply name exclusion patterns
            if _matches_exclusion(de.name, exclude_pats):
                continue

            # Apply extension filter (os.path.splitext avoids split heuristics)
            if os.path.splitext(de.name)[1].lower() in ignore_ext_set:
                continue

            # Apply folder name filter (current already known — no extra parse)
            if current.name in ignore_dir_set:
                continue

            # DirEntry.stat(follow_symlinks=False) is kernel-cached on Linux/macOS
            try:
                st = de.stat(follow_symlinks=False)
            except PermissionError:
                _record_skip(skipped, child, "PermissionError", log_level="warn")
                continue
            except OSError as e:
                _record_skip(skipped, child, type(e).__name__, log_level="warn")
                continue

            if not stat_mod.S_ISREG(st.st_mode):
                continue

            # Apply min file size filter
            if st.st_size < min_size:
                continue

            try:
                resolved = child.resolve(strict=False)
            except OSError as e:
                _record_skip(skipped, child, f"resolve_failed:{type(e).__name__}", log_level="warn")
                continue

            entries.append(FileEntry(
                path     = resolved,
                size     = st.st_size,
                inode    = st.st_ino,
                mtime_ns = st.st_mtime_ns,
                device   = st.st_dev,
            ))

    entries.sort(key=_ENTRY_KEY)
    return entries, skipped


# ---------------------------------------------------------------------------
# Phase B — Group by size (zero I/O, O(N) references only)
# ---------------------------------------------------------------------------

def group_by_size(
    snapshot: list[FileEntry],
) -> dict[int, list[FileEntry]]:
    groups: dict[int, list[FileEntry]] = defaultdict(list)
    for entry in snapshot:
        groups[entry.size].append(entry)
    for group in groups.values():
        group.sort(key=_ENTRY_KEY)
    return groups


# ---------------------------------------------------------------------------
# Phase C — Hashing
# ---------------------------------------------------------------------------

def _verify_snapshot(entry: FileEntry) -> bool:
    """Re-stat to detect changes since snapshot. Returns False on any failure."""
    try:
        current = entry.path.stat(follow_symlinks=False)
    except OSError:
        return False
    return (
        current.st_size     == entry.size     and
        current.st_ino      == entry.inode    and
        current.st_mtime_ns == entry.mtime_ns
    )


def _hash_entry(
    entry: FileEntry,
    chunk_size: int = CHUNK_SIZE,
    max_bytes: int | None = None,
) -> tuple[FileEntry, str | None]:
    """
    Hash one file in fixed-size chunks. Pure function — no shared mutable state.

    Memory: at most chunk_size bytes held at once.
    Partial hash state discarded on any failure — no false duplicates possible.
    Returns (entry, hexdigest) on success, (entry, None) on any failure.
    """
    if not _verify_snapshot(entry):
        return entry, None

    fd = -1
    try:
        fd = os.open(str(entry.path), os.O_RDONLY)
        fd_stat = os.fstat(fd)

        # TOCTOU check: file replaced between verify and open?
        if fd_stat.st_ino != entry.inode or fd_stat.st_dev != entry.device:
            return entry, None

        hasher = hashlib.sha256()
        remaining = max_bytes

        with os.fdopen(fd, "rb") as f:
            fd = -1  # fdopen owns fd now
            while True:
                to_read = chunk_size if remaining is None else min(chunk_size, remaining)
                chunk = f.read(to_read)
                if not chunk:
                    break
                hasher.update(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
                    if remaining <= 0:
                        break

        return entry, hasher.hexdigest()

    except OSError:
        return entry, None  # partial hasher discarded here
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass


def _group_by_hash_parallel(
    entries: list[FileEntry],
    skipped: list[dict],
    max_workers: int = DEFAULT_WORKERS,
    chunk_size: int = CHUNK_SIZE,
    max_bytes: int | None = None,
    hash_cache: "_HashCache | None" = None,
) -> dict[str, list[FileEntry]]:
    """
    Hash entries using bounded ThreadPoolExecutor.

    v2.6.0: accepts optional hash_cache.
      - Before submitting a file to a worker, check the cache.
        Cache hit → reuse digest without I/O.  Only valid when entry.size
        <= PARTIAL_BYTES (partial-phase digest equals full digest for files
        that fit within the partial-read window).
      - After a worker returns a digest for a small file
        (size <= max_bytes when max_bytes is not None), store in cache
        so the full-hash phase can reuse it.

    Thread-safety audit (v1.5.0 — locked):
      Component           | Thread-safe? | Method
      --------------------|--------------|---------------------------
      logger              | YES          | GIL + swallowed exceptions
      groups (result)     | YES          | main thread only
      skipped list        | YES          | main thread only
      _hash_entry         | YES          | pure function, local state
      hash_cache          | YES          | internal threading.Lock()
      timer               | YES          | GIL-protected module dict
      file writes         | YES          | single-thread export only

    Interrupt safety:
    - KeyboardInterrupt propagated to main thread via future.result()
    - context manager __exit__ calls executor.shutdown(wait=True)
    - Workers finish current chunk then exit — no abrupt kill

    Determinism:
    - as_completed() order is nondeterministic (intentional — faster)
    - Groups sorted by _ENTRY_KEY after ALL futures complete
    - Output order is independent of thread completion order
    """
    groups: dict[str, list[FileEntry]] = defaultdict(list)
    queue_depth = max_workers * _QUEUE_DEPTH_MULTIPLIER

    # ── Cache pre-pass (main thread, zero I/O) ───────────────────────────────
    # Files whose full digest was already captured during the partial-hash phase
    # (possible only when size <= PARTIAL_BYTES) skip re-hashing entirely.
    to_hash: list[FileEntry]
    if hash_cache is not None:
        to_hash = []
        for entry in entries:
            cached = hash_cache.get(entry)
            if cached is not None:
                groups[cached].append(entry)
            else:
                to_hash.append(entry)
    else:
        to_hash = list(entries)

    def _drain(futures_batch: list) -> None:
        for future in as_completed(futures_batch):
            file_entry, digest = future.result()
            if digest is None:
                _record_skip(skipped, file_entry.path,
                             "changed_or_unreadable", log_level="warn")
            else:
                groups[digest].append(file_entry)
                # Cache the digest when the file was fully read during the partial
                # phase (max_bytes set and file fits within the read window).
                if (hash_cache is not None
                        and max_bytes is not None
                        and file_entry.size <= max_bytes):
                    hash_cache.put(file_entry, digest)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for entry in to_hash:
            futures.append(executor.submit(_hash_entry, entry, chunk_size, max_bytes))

            # Drain when queue fills — bounded memory
            if len(futures) >= queue_depth:
                _drain(futures)
                futures.clear()

        # Drain remainder
        _drain(futures)

    # Sort after all futures complete — deterministic regardless of thread order
    for group in groups.values():
        group.sort(key=_ENTRY_KEY)
    return groups


def group_by_partial_hash(
    entries: list[FileEntry],
    skipped: list[dict],
    max_workers: int = DEFAULT_WORKERS,
    chunk_size: int = CHUNK_SIZE,
    hash_cache: "_HashCache | None" = None,
) -> dict[str, list[FileEntry]]:
    return _group_by_hash_parallel(
        entries, skipped,
        max_workers=max_workers,
        chunk_size=chunk_size,
        max_bytes=PARTIAL_BYTES,
        hash_cache=hash_cache,
    )


def group_by_hash(
    entries: list[FileEntry],
    skipped: list[dict],
    max_workers: int = DEFAULT_WORKERS,
    chunk_size: int = CHUNK_SIZE,
    hash_cache: "_HashCache | None" = None,
) -> dict[str, list[FileEntry]]:
    return _group_by_hash_parallel(
        entries, skipped,
        max_workers=max_workers,
        chunk_size=chunk_size,
        max_bytes=None,
        hash_cache=hash_cache,
    )


# ---------------------------------------------------------------------------
# Phase D — Find duplicates
# ---------------------------------------------------------------------------

def find_duplicates(
    groups: dict[str, list[FileEntry]],
    keep: str = "oldest",
) -> dict[str, list[FileEntry]]:
    """
    Filter to groups with >1 entry.
    Sort by canonical key: (size, hash, first_path).
    """
    strategy = KEEP_STRATEGIES[keep]

    duplicate_groups = {
        h: sorted(entries, key=strategy)
        for h, entries in groups.items()
        if len(entries) > 1
    }

    return dict(sorted(
        duplicate_groups.items(),
        key=lambda kv: (kv[1][0].size, kv[0], str(kv[1][0].path))
    ))


# ---------------------------------------------------------------------------
# Pipeline orchestration — explicit phase memory release
# ---------------------------------------------------------------------------

def run_hash_pipeline(
    snapshot: list[FileEntry],
    keep: str,
    skipped: list[dict],
    max_workers: int = DEFAULT_WORKERS,
    chunk_size: int = CHUNK_SIZE,
) -> tuple[dict[str, list[FileEntry]], int, int, int]:
    """
    Three-phase pipeline with stale reference release after each phase.
    max_workers threads used for both partial and full hash phases.
    """
    n_snapshot = len(snapshot)
    logger.log_info(f"Pipeline: {n_snapshot} files, {max_workers} workers")

    # Phase 1 — size grouping
    timer.start_timer("size_group")
    size_groups = group_by_size(snapshot)
    timer.end_timer("size_group")

    size_candidates = sorted(
        [e for entries in size_groups.values() if len(entries) > 1 for e in entries],
        key=_ENTRY_KEY,
    )
    n_size_candidates = len(size_candidates)
    del size_groups  # Phase 1 complete — release dict

    logger.log_info(f"Size filter: {n_size_candidates} candidates after excluding singletons")

    # Hash cache (v2.6.0): captures partial digests of small files so the full-
    # hash phase can skip re-reading them.  Scoped to this pipeline run only.
    hash_cache = _HashCache()

    # Phase 2 — partial hash
    timer.start_timer("partial_hash")
    partial_groups = group_by_partial_hash(
        size_candidates, skipped,
        max_workers=max_workers,
        chunk_size=chunk_size,
        hash_cache=hash_cache,
    )
    timer.end_timer("partial_hash")
    del size_candidates  # Phase 2 input done

    partial_candidates = sorted(
        [e for entries in partial_groups.values() if len(entries) > 1 for e in entries],
        key=_ENTRY_KEY,
    )
    n_partial_candidates = len(partial_candidates)
    del partial_groups  # Phase 2 complete — release dict

    logger.log_info(f"Partial hash filter: {n_partial_candidates} candidates remaining")

    # Phase 3 — full hash
    timer.start_timer("full_hash")
    full_groups = group_by_hash(
        partial_candidates, skipped,
        max_workers=max_workers,
        chunk_size=chunk_size,
        hash_cache=hash_cache,
    )
    timer.end_timer("full_hash")
    del partial_candidates  # Phase 3 input done
    del hash_cache           # Pipeline complete — release cache

    timer.start_timer("grouping")
    duplicates = find_duplicates(full_groups, keep=keep)
    timer.end_timer("grouping")
    del full_groups  # Only duplicates retained

    logger.log_info(f"Found {len(duplicates)} duplicate groups")

    skipped_by_size    = n_snapshot          - n_size_candidates
    skipped_by_partial = n_size_candidates   - n_partial_candidates
    hashed_count       = n_partial_candidates

    return duplicates, skipped_by_size, skipped_by_partial, hashed_count


# ---------------------------------------------------------------------------
# Accounting
# ---------------------------------------------------------------------------

def _wasted_bytes(duplicates: dict[str, list[FileEntry]]) -> int:
    return sum(
        entries[0].size * (len(entries) - 1)
        for entries in duplicates.values()
        if entries
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_workers(requested: int | None) -> int:
    """
    Compute final worker count from user request or default.

    Rules (locked permanently):
    - None   → min(32, cpu_count + 4)  — I/O-bound heuristic
    - N < 1  → clamped to 1
    - N > cpu_count × 4 → clamped to hard cap
    """
    if requested is None:
        return DEFAULT_WORKERS
    return max(1, min(requested, MAX_WORKERS_HARD_CAP))


def scan_duplicates(
    folder:          Path,
    keep:            str            = "oldest",
    follow_symlinks: bool           = False,
    max_depth:       int | None     = None,
    chunk_size:      int            = CHUNK_SIZE,
    max_workers:     int | None     = None,
    policy:          ScanPolicy | None = None,
) -> dict:
    """
    Full duplicate scan.

    max_workers: number of hashing threads.
      None  → default heuristic: min(32, cpu_count + 4)
      N     → clamped to [1, cpu_count × 4]

    Memory: O(N) — no file contents stored.
    Deterministic: parallel results sorted before exposure.
    """
    effective_workers = resolve_workers(max_workers)
    logger.log_info(f"Starting scan: {folder} | workers={effective_workers}")

    t_start = time.perf_counter()
    timer.reset()
    timer.start_timer("total")
    timer.start_timer("scan")

    snapshot, skipped = collect_snapshot(
        folder,
        follow_symlinks=follow_symlinks,
        max_depth=max_depth,
        policy=policy,
    )
    total_scanned = len(snapshot)

    timer.end_timer("scan")
    logger.log_debug(f"Snapshot collected: {total_scanned} files")

    duplicates, skipped_by_size, skipped_by_partial, total_hashed = run_hash_pipeline(
        snapshot, keep, skipped,
        max_workers=effective_workers,
        chunk_size=chunk_size,
    )
    del snapshot  # No longer needed

    timer.end_timer("total")

    return {
        "folder":                str(folder),
        "keep_strategy":         keep,
        "total_scanned":         total_scanned,
        "skipped_by_size":       skipped_by_size,
        "skipped_by_partial":    skipped_by_partial,
        "total_hashed":          total_hashed,
        "workers":               effective_workers,
        "skipped_unreadable":    sorted(skipped, key=lambda s: s["path"]),
        "duplicates":            duplicates,
        "total_duplicate_files": sum(len(e) - 1 for e in duplicates.values()),
        "wasted_bytes":          _wasted_bytes(duplicates),
        "scan_duration_seconds": time.perf_counter() - t_start,
    }



def export_json(
    duplicates: dict[str, list[FileEntry]],
    output_path: Path,
) -> Path:
    """Atomic write: temp file + rename. No partial file on crash or Ctrl+C."""
    data = [
        {
            "hash":       file_hash,
            "keep":       entries[0].path.as_posix(),
            "duplicates": [e.path.as_posix() for e in entries[1:]],
            "size_bytes": entries[0].size,
        }
        for file_hash, entries in duplicates.items()
    ]
    payload = json.dumps(data, indent=2, sort_keys=True)

    tmp_path = output_path.with_suffix(".tmp")
    try:
        tmp_path.write_text(payload)
        tmp_path.rename(output_path)
    except OSError:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise

    return output_path
