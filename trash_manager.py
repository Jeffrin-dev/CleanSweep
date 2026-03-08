"""
CleanSweep TrashManager — v1.7.0

Cross-platform, OS-aware trash backend.

Supported platforms:
  Linux   — XDG Trash spec (~/.local/share/Trash or $XDG_DATA_HOME/Trash)
  macOS   — ~/.Trash via os.rename
  Windows — winshell / ctypes SHFileOperation

All OS-specific logic is contained here.
Nothing outside this module may perform trash operations.

Fallback policy (locked permanently):
  If trash unavailable → abort with TrashUnavailableError
  Permanent delete requires explicit --permanent flag
  No silent permanent fallback. Ever.
"""

from __future__ import annotations

import datetime
import os
import platform
import shutil
import sys
from pathlib import Path
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TrashError(Exception):
    """Base class for all trash-related errors."""


class TrashUnavailableError(TrashError):
    """
    Raised when the system trash cannot be used for a file.
    Caller must prompt user to use --permanent for hard delete.
    """


class TrashFailedError(TrashError):
    """Raised when the trash move itself fails (permissions, I/O, etc.)."""


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

class TrashResult(NamedTuple):
    path:        str
    status:      str   # "trashed" | "failed" | "unavailable" | "dry_run"
    trash_path:  str   # where the file ended up in trash (empty if failed)
    error:       str   # empty on success


# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

def _platform() -> str:
    """Return normalized platform name: 'linux' | 'macos' | 'windows' | 'unknown'."""
    system = platform.system().lower()
    if system == "linux":
        return "linux"
    elif system == "darwin":
        return "macos"
    elif system == "windows":
        return "windows"
    return "unknown"


# ---------------------------------------------------------------------------
# Linux — XDG Trash specification
# https://specifications.freedesktop.org/trash-spec/trashspec-1.0.html
# ---------------------------------------------------------------------------

def _xdg_trash_root() -> Path:
    """
    Return the XDG Trash root directory.
    Respects $XDG_DATA_HOME if set. Default: ~/.local/share/Trash
    """
    xdg_data = os.environ.get("XDG_DATA_HOME", "")
    if xdg_data and Path(xdg_data).is_absolute():
        base = Path(xdg_data)
    else:
        base = Path.home() / ".local" / "share"
    return base / "Trash"


def _xdg_trash_available(trash_root: Path) -> bool:
    """Return True if XDG trash can be created/used."""
    try:
        trash_root.mkdir(parents=True, exist_ok=True)
        (trash_root / "files").mkdir(exist_ok=True)
        (trash_root / "info").mkdir(exist_ok=True)
        return True
    except OSError:
        return False


def _unique_trash_name(trash_files: Path, name: str) -> str:
    """
    Return a filename not already in trash_files/.
    Appends _2, _3, ... until unique.
    """
    candidate = name
    stem = Path(name).stem
    suffix = Path(name).suffix
    counter = 2
    while (trash_files / candidate).exists():
        candidate = f"{stem}_{counter}{suffix}"
        counter += 1
    return candidate


def _write_trashinfo(trash_info_dir: Path, trash_name: str, original_path: Path) -> None:
    """
    Write .trashinfo metadata file per XDG spec.

    Format:
      [Trash Info]
      Path=/original/absolute/path
      DeletionDate=YYYY-MM-DDTHH:MM:SS
    """
    info_path = trash_info_dir / f"{trash_name}.trashinfo"
    deletion_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    content = (
        "[Trash Info]\n"
        f"Path={original_path.resolve()}\n"
        f"DeletionDate={deletion_date}\n"
    )
    info_path.write_text(content, encoding="utf-8")


def _trash_linux(file_path: Path) -> TrashResult:
    """
    Move file to XDG Trash.
    Creates trash directories if they don't exist.
    Raises TrashUnavailableError if trash cannot be set up.
    Raises TrashFailedError if the move itself fails.
    """
    trash_root = _xdg_trash_root()

    if not _xdg_trash_available(trash_root):
        raise TrashUnavailableError(
            f"XDG Trash unavailable at {trash_root}. "
            "Use --permanent to permanently delete."
        )

    trash_files = trash_root / "files"
    trash_info  = trash_root / "info"

    trash_name = _unique_trash_name(trash_files, file_path.name)
    dest = trash_files / trash_name

    try:
        _write_trashinfo(trash_info, trash_name, file_path)
        shutil.move(str(file_path), str(dest))
    except OSError as e:
        # Clean up partial trashinfo if move failed
        info_path = trash_info / f"{trash_name}.trashinfo"
        try:
            info_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise TrashFailedError(f"Trash move failed: {e}") from e

    return TrashResult(
        path=file_path.as_posix(),
        status="trashed",
        trash_path=dest.as_posix(),
        error="",
    )


# ---------------------------------------------------------------------------
# macOS — ~/.Trash
# ---------------------------------------------------------------------------

def _trash_macos(file_path: Path) -> TrashResult:
    """Move file to macOS ~/.Trash."""
    trash_dir = Path.home() / ".Trash"
    try:
        trash_dir.mkdir(exist_ok=True)
    except OSError as e:
        raise TrashUnavailableError(f"macOS Trash unavailable: {e}") from e

    # Find unique name in trash
    dest_name = file_path.name
    stem = file_path.stem
    suffix = file_path.suffix
    counter = 2
    dest = trash_dir / dest_name
    while dest.exists():
        dest_name = f"{stem}_{counter}{suffix}"
        dest = trash_dir / dest_name
        counter += 1

    try:
        shutil.move(str(file_path), str(dest))
    except OSError as e:
        raise TrashFailedError(f"Trash move failed: {e}") from e

    return TrashResult(
        path=file_path.as_posix(),
        status="trashed",
        trash_path=dest.as_posix(),
        error="",
    )


# ---------------------------------------------------------------------------
# Windows — SHFileOperation via ctypes
# ---------------------------------------------------------------------------

def _trash_windows(file_path: Path) -> TrashResult:
    """Send file to Windows Recycle Bin via ctypes SHFileOperation."""
    try:
        import ctypes
        from ctypes import wintypes

        SHFileOperation = ctypes.windll.shell32.SHFileOperationW

        class SHFILEOPSTRUCT(ctypes.Structure):
            _fields_ = [
                ("hwnd",                  wintypes.HWND),
                ("wFunc",                 wintypes.UINT),
                ("pFrom",                 wintypes.LPCWSTR),
                ("pTo",                   wintypes.LPCWSTR),
                ("fFlags",                wintypes.WORD),
                ("fAnyOperationsAborted", wintypes.BOOL),
                ("hNameMappings",         ctypes.c_void_p),
                ("lpszProgressTitle",     wintypes.LPCWSTR),
            ]

        FO_DELETE    = 0x0003
        FOF_ALLOWUNDO       = 0x0040   # send to recycle bin
        FOF_NOCONFIRMATION  = 0x0010
        FOF_SILENT          = 0x0004

        path_str = str(file_path.resolve()) + "\0\0"
        op = SHFILEOPSTRUCT()
        op.wFunc  = FO_DELETE
        op.pFrom  = path_str
        op.fFlags = FOF_ALLOWUNDO | FOF_NOCONFIRMATION | FOF_SILENT

        result = SHFileOperation(ctypes.byref(op))
        if result != 0:
            raise TrashFailedError(f"SHFileOperation returned {result}")

    except (ImportError, AttributeError, OSError) as e:
        raise TrashUnavailableError(f"Windows Recycle Bin unavailable: {e}") from e

    return TrashResult(
        path=file_path.as_posix(),
        status="trashed",
        trash_path="(Recycle Bin)",
        error="",
    )


# ---------------------------------------------------------------------------
# TrashManager — public API
# ---------------------------------------------------------------------------

class TrashManager:
    """
    Cross-platform trash backend.
    One instance per run. Detect platform once at init.

    Usage:
        tm = TrashManager()
        result = tm.trash(file_path)

    Fallback policy:
        If trash unavailable → raise TrashUnavailableError
        Never silently fall back to permanent delete.
    """

    def __init__(self) -> None:
        self._platform = _platform()

    @property
    def platform(self) -> str:
        return self._platform

    def is_available(self) -> bool:
        """Quick check: can trash be used on this system?"""
        try:
            if self._platform == "linux":
                return _xdg_trash_available(_xdg_trash_root())
            elif self._platform == "macos":
                return (Path.home() / ".Trash").exists() or True  # will be created
            elif self._platform == "windows":
                return True  # assume Recycle Bin available; fail at runtime if not
            return False
        except OSError:
            return False

    def trash(self, file_path: Path) -> TrashResult:
        """
        Move file to system trash.

        Raises:
            TrashUnavailableError — trash cannot be used; caller must handle
            TrashFailedError      — trash available but move failed
        """
        if not file_path.exists():
            # Already gone — treat as success (idempotent)
            return TrashResult(
                path=file_path.as_posix(),
                status="trashed",
                trash_path="(already gone)",
                error="",
            )

        if self._platform == "linux":
            return _trash_linux(file_path)
        elif self._platform == "macos":
            return _trash_macos(file_path)
        elif self._platform == "windows":
            return _trash_windows(file_path)
        else:
            raise TrashUnavailableError(
                f"Trash not supported on platform: {self._platform}. "
                "Use --permanent to permanently delete."
            )
