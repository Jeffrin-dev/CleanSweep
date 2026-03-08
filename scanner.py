"""
CleanSweep v2.6.0 — Traversal Engine.

v2.6.0 (performance pass):
  scan_files() — inner loop now uses os.scandir() instead of Path.iterdir().
    DirEntry.is_file() / is_dir() / is_symlink() consume cached inode data from
    the kernel readdir buffer, eliminating one lstat() syscall per entry.
    DirEntry.stat(follow_symlinks=False) is also kernel-cached on Linux/macOS,
    removing the extra stat() call previously needed for min_file_size filtering.
    Public API and yielded types are unchanged.

v2.1.0 upgrade:
  ScanPolicy  — immutable, validated traversal configuration
  ScanError   — raised when symlink_policy="error" and a symlink is found
  scan_files()— generator-based iterative DFS, never recurses the Python stack
  Symlink policies: ignore | follow (with cycle detection) | error
  Glob exclusion via fnmatch, case-insensitive, applied before rule engine
  Depth limiting: recursive=False → top-level only, max_depth=N → bounded DFS
  Per-directory sorted ordering — deterministic yielding order

v1.x backward compatibility:
  validate_folder()  — unchanged
  list_files()       — thin sorted wrapper over scan_files(); signature unchanged

Architecture contract (permanent, locked in INVARIANTS.md §19):
  scan_files(root, policy) → Iterator[Path]
    Pure traversal provider. No rule engine. No file mutation. No printing.
    No exiting. Yields resolved, absolute paths of regular files matching policy.
    Memory: O(depth * branching_factor) for the stack — never O(total_files).
"""

from __future__ import annotations

import fnmatch
import os
import re
import stat as stat_mod
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterator

from file_operation_manager import TEMP_PREFIX


# ---------------------------------------------------------------------------
# Symlink policy constants
# ---------------------------------------------------------------------------

SYMLINK_IGNORE: Final[str] = "ignore"
SYMLINK_FOLLOW: Final[str] = "follow"
SYMLINK_ERROR:  Final[str] = "error"

_VALID_SYMLINK_POLICIES: Final[frozenset[str]] = frozenset(
    {SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR}
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ScanError(Exception):
    """
    Raised by scan_files() when symlink_policy="error" and a symlink is found.

    Always carries the symlink path. Callers must catch and map to exit code.
    Never swallowed inside the scanner.
    """


# ---------------------------------------------------------------------------
# Glob pattern validation
# ---------------------------------------------------------------------------

def _validate_glob_pattern(pat: str, index: int) -> None:
    """
    Reject structurally invalid glob patterns.

    Checks:
      - Pattern must be a non-empty string
      - Character classes must be properly closed: every '[' needs a ']'

    Raises ValueError with a precise message on any violation.
    Pure function — no filesystem access, no side effects.
    """
    if not isinstance(pat, str):
        raise ValueError(
            f"exclude_patterns[{index}]: pattern must be a string, "
            f"got {type(pat).__name__}."
        )
    if not pat:
        raise ValueError(
            f"exclude_patterns[{index}]: pattern must not be empty."
        )

    # Validate character classes — every '[' must have a matching ']'
    i = 0
    while i < len(pat):
        if pat[i] == "[":
            j = i + 1
            # Allow ]  or !] as first chars inside class (special fnmatch syntax)
            if j < len(pat) and pat[j] == "!":
                j += 1
            if j < len(pat) and pat[j] == "]":
                j += 1
            found = False
            while j < len(pat):
                if pat[j] == "]":
                    found = True
                    break
                j += 1
            if not found:
                raise ValueError(
                    f"exclude_patterns[{index}]: pattern {pat!r} has an unclosed "
                    f"'[' bracket. Character classes must be closed with ']'."
                )
        i += 1

    # Also verify the compiled regex is valid (belt-and-suspenders)
    try:
        re.compile(fnmatch.translate(pat))
    except re.error as exc:
        raise ValueError(
            f"exclude_patterns[{index}]: pattern {pat!r} produces an invalid "
            f"regular expression: {exc}."
        ) from exc


# ---------------------------------------------------------------------------
# ScanPolicy — immutable traversal configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScanPolicy:
    """
    Captures all traversal decisions in one validated, immutable object.

    Passed to scan_files(). Never mutated after creation.
    Validated in __post_init__ — invalid values raise ValueError immediately.

    Fields:
      recursive        — must be bool. False: top-level files only (depth=0).
                         True: full DFS.
      max_depth        — None: unlimited. N >= 0: descend at most N levels below
                         root. Depth 0 = root directory itself.
                         Effective only when recursive=True.
      symlink_policy   — "ignore": skip all symlinks.
                         "follow": follow symlinked dirs with cycle detection.
                         "error":  raise ScanError on first symlink encountered.
      exclude_patterns — fnmatch glob patterns (case-insensitive, validated at
                         construction). Applied to filename/dirname only (not
                         the full path). Each pattern must have balanced brackets.
      min_file_size    — files strictly smaller than this (bytes) are excluded.
                         0 = no minimum.
      ignore_extensions — file extensions to exclude (dot-prefixed, case-folded).
      ignore_folders   — directory names to skip entirely during traversal.
    """
    recursive:         bool              = True
    max_depth:         int | None        = None
    symlink_policy:    str               = SYMLINK_IGNORE
    exclude_patterns:  tuple[str, ...]   = ()
    min_file_size:     int               = 0
    ignore_extensions: tuple[str, ...]   = ()
    ignore_folders:    tuple[str, ...]   = ()

    def __post_init__(self) -> None:
        # 'recursive' must be strictly bool — not just truthy
        if not isinstance(self.recursive, bool):
            raise ValueError(
                f"recursive must be a bool (True or False), "
                f"got {type(self.recursive).__name__}: {self.recursive!r}."
            )

        if self.symlink_policy not in _VALID_SYMLINK_POLICIES:
            raise ValueError(
                f"symlink_policy must be one of {sorted(_VALID_SYMLINK_POLICIES)}, "
                f"got {self.symlink_policy!r}."
            )
        if self.max_depth is not None and self.max_depth < 0:
            raise ValueError(
                f"max_depth must be >= 0 or None, got {self.max_depth}."
            )
        if self.min_file_size < 0:
            raise ValueError(
                f"min_file_size must be >= 0, got {self.min_file_size}."
            )
        # Validate every glob pattern at construction time
        for idx, pat in enumerate(self.exclude_patterns):
            _validate_glob_pattern(pat, idx)


# ---------------------------------------------------------------------------
# Exclusion matching
# ---------------------------------------------------------------------------

def _matches_exclusion(name: str, patterns: tuple[str, ...]) -> bool:
    """
    Return True if name matches any exclusion pattern (case-insensitive fnmatch).

    Pure function. No filesystem access. No side effects.
    Matches against the name only — not the full path.

    Examples:
      _matches_exclusion("node_modules", ("node_modules",))   -> True
      _matches_exclusion("report.log",   ("*.log",))          -> True
      _matches_exclusion("Report.LOG",   ("*.log",))          -> True  (case-insensitive)
      _matches_exclusion("main.py",      ("*.log", "*.tmp"))  -> False
    """
    if not patterns:
        return False
    name_lower = name.lower()
    return any(fnmatch.fnmatch(name_lower, pat.lower()) for pat in patterns)


# ---------------------------------------------------------------------------
# Core traversal generator
# ---------------------------------------------------------------------------

def scan_files(root: Path, policy: ScanPolicy) -> Iterator[Path]:
    """
    Generator. Yields resolved absolute paths of regular files matching policy.

    Traversal guarantees (permanent, locked in INVARIANTS.md §19):
      - Iterative DFS via explicit stack — zero Python recursion
      - Per-directory entries sorted by name — deterministic yield order
      - Stack memory: O(depth x avg_branching) — never O(total_files)
      - Symlink cycle detection via (inode, device) set when policy=follow
      - Exclusion patterns applied before any filesystem I/O beyond is_symlink/is_dir
      - Single stat() per file — no redundant metadata reads
      - OSError on any entry skipped, traversal continues
      - No printing, no exiting, no rule engine calls, no mutation

    Symlink policies:
      "ignore" — symlinks never yielded, never descended into
      "follow" — symlinked directories followed; cycle detection via inode+device;
                 symlinked files yielded normally
      "error"  — ScanError raised on first symlink encountered

    Depth semantics:
      recursive=False  -> only depth-0 files (root directory entries)
      max_depth=0      -> same as recursive=False
      max_depth=N      -> descend at most N levels below root (root = level 0)
      max_depth=None   -> unlimited when recursive=True
    """
    # Effective depth ceiling
    if not policy.recursive:
        depth_limit: int | None = 0
    else:
        depth_limit = policy.max_depth

    # Pre-compute normalised ignore sets for O(1) lookup
    ignore_ext: frozenset[str] = frozenset(
        e.lower() if e.startswith(".") else f".{e.lower()}"
        for e in policy.ignore_extensions
    )
    ignore_dir: frozenset[str] = frozenset(policy.ignore_folders)

    # Visited set for cycle detection (inode, device) — only used when following
    visited: set[tuple[int, int]] = set()

    # Seed visited with root's identity
    try:
        root_st = root.stat()
        visited.add((root_st.st_ino, root_st.st_dev))
    except OSError:
        return  # root unreadable — yield nothing

    # Stack: (directory_path, depth)
    stack: list[tuple[Path, int]] = [(root, 0)]

    while stack:
        current_dir, depth = stack.pop()

        # Read and sort directory entries for deterministic traversal order.
        # os.scandir() caches inode metadata in the DirEntry — is_file(),
        # is_dir(), is_symlink() and stat(follow_symlinks=False) are all
        # served from that cache on Linux/macOS, cutting syscalls per entry.
        try:
            with os.scandir(current_dir) as sd:
                dir_entries = sorted(sd, key=lambda e: e.name)
        except (PermissionError, FileNotFoundError, OSError):
            continue  # unreadable directory — skip, never fatal

        # Collect subdirs to push; process files immediately.
        # Uses += list concatenation, not individual element pushing.
        # Only current-directory subdirectories are held — no file list accumulation.
        pending_dirs: list[tuple[Path, int]] = []

        for de in dir_entries:
            # Always skip in-progress atomic temp files
            if de.name.startswith(TEMP_PREFIX):
                continue

            # Determine entry type — guard every call.
            # DirEntry methods use cached kernel data; no extra syscall for
            # regular files on Linux/macOS.
            try:
                is_sym  = de.is_symlink()
                is_dir  = de.is_dir()    # follows symlinks
                is_file = de.is_file()   # follows symlinks
            except OSError:
                continue  # entry vanished or access denied (race condition)

            # Materialise Path only after cheap type checks pass
            child = Path(de.path)

            # ── Symlink handling ──────────────────────────────────────────
            if is_sym:
                if policy.symlink_policy == SYMLINK_IGNORE:
                    continue
                elif policy.symlink_policy == SYMLINK_ERROR:
                    raise ScanError(
                        f"Symlink encountered at {child} "
                        f"— symlink_policy is 'error'."
                    )
                # SYMLINK_FOLLOW: fall through to normal dir/file handling,
                # with cycle detection applied for directories.

            # ── Directory handling ─────────────────────────────────────────
            if is_dir:
                # Respect ignore_folders by name
                if de.name in ignore_dir:
                    continue
                # Respect exclude_patterns by name
                if _matches_exclusion(de.name, policy.exclude_patterns):
                    continue
                # Depth limit — push only if we can descend further
                if depth_limit is not None and depth >= depth_limit:
                    continue
                # Cycle detection for symlinked directories
                if is_sym and policy.symlink_policy == SYMLINK_FOLLOW:
                    try:
                        real_st = de.stat(follow_symlinks=True)
                        key = (real_st.st_ino, real_st.st_dev)
                        if key in visited:
                            continue  # cycle — skip
                        visited.add(key)
                    except OSError:
                        continue
                pending_dirs += [(child, depth + 1)]
                continue  # do not fall through to file handling

            # ── File handling ──────────────────────────────────────────────
            if not is_file:
                continue  # special file (device, socket, etc.)

            # Exclude by name pattern
            if _matches_exclusion(de.name, policy.exclude_patterns):
                continue

            # Exclude by extension (os.path.splitext avoids string.split heuristics)
            if os.path.splitext(de.name)[1].lower() in ignore_ext:
                continue

            # Exclude by parent folder name (current_dir already known — no parse)
            if current_dir.name in ignore_dir:
                continue

            # Exclude by size.
            # DirEntry.stat(follow_symlinks=False) is kernel-cached from scandir
            # on Linux/macOS — no additional syscall for regular (non-symlink) files.
            if policy.min_file_size > 0:
                try:
                    st = de.stat(follow_symlinks=True)
                    if not stat_mod.S_ISREG(st.st_mode):
                        continue
                    if st.st_size < policy.min_file_size:
                        continue
                except OSError:
                    continue

            # Yield the file path.
            # Symlinked files (SYMLINK_FOLLOW): yield the symlink's own path —
            # resolve() would follow to the target, losing symlink identity.
            # Regular files: resolve() normalises any .. or . components.
            try:
                if is_sym:
                    yield child
                else:
                    yield child.resolve(strict=False)
            except OSError:
                continue

        # Push subdirs in reverse-sorted order so LIFO stack processes them
        # alphabetically (first alpha subdir is last pushed, first popped).
        stack.extend(reversed(pending_dirs))


# ---------------------------------------------------------------------------
# validate_folder — unchanged from v1.x
# ---------------------------------------------------------------------------

def validate_folder(path: Path) -> Path:
    """Validate that path exists and is a directory. Raises on failure."""
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a folder: {path}")
    return path


# ---------------------------------------------------------------------------
# list_files — v1.x backward-compatible wrapper over scan_files()
# ---------------------------------------------------------------------------

def list_files(
    folder:            Path,
    ignore_extensions: list[str] | None = None,
    ignore_folders:    list[str] | None = None,
    min_file_size:     int = 0,
    follow_symlinks:   bool = False,
    max_depth:         int | None = None,
    exclude_patterns:  list[str] | None = None,
    symlink_policy:    str | None = None,
) -> list[Path]:
    """
    Return sorted list of resolved file paths matching the given policy.

    v2.1: thin wrapper over scan_files(). All filtering delegated to ScanPolicy.

    Backward compatibility:
      follow_symlinks=True  -> symlink_policy="follow"  (if symlink_policy not given)
      follow_symlinks=False -> symlink_policy="ignore"  (if symlink_policy not given)
      All other parameters unchanged.

    New in v2.1:
      exclude_patterns — fnmatch glob patterns to exclude files/dirs by name
      symlink_policy   — explicit policy; overrides follow_symlinks when given
    """
    # Resolve symlink policy — explicit arg wins over legacy follow_symlinks
    if symlink_policy is not None:
        resolved_policy = symlink_policy
    else:
        resolved_policy = SYMLINK_FOLLOW if follow_symlinks else SYMLINK_IGNORE

    policy = ScanPolicy(
        recursive         = True,
        max_depth         = max_depth,
        symlink_policy    = resolved_policy,
        exclude_patterns  = tuple(exclude_patterns or []),
        min_file_size     = min_file_size,
        ignore_extensions = tuple(ignore_extensions or []),
        ignore_folders    = tuple(ignore_folders or []),
    )

    return sorted(scan_files(folder, policy))
