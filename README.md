# CleanSweep

**File scanner, duplicate finder, and rule-based organizer.**  
Deterministic · Atomic · Zero external dependencies · Scales to 1 million files.

```
cleansweep scan       ~/Downloads
cleansweep duplicates ~/Downloads --dry-run
cleansweep organize   ~/Downloads --dry-run
```

---

## Table of contents

1. [Overview](#1-overview)
2. [Features](#2-features)
3. [Installation](#3-installation)
4. [Quick start](#4-quick-start)
5. [CLI reference](#5-cli-reference)
6. [Configuration guide](#6-configuration-guide)
7. [Rule engine](#7-rule-engine)
8. [Duplicate detection](#8-duplicate-detection)
9. [Safety guarantees](#9-safety-guarantees)
10. [Architecture](#10-architecture)
11. [Performance benchmarks](#11-performance-benchmarks)
12. [Exit codes](#12-exit-codes)

---

## 1. Overview

CleanSweep is a command-line tool for managing large file collections. It scans
directories, detects exact duplicates using a three-stage hash pipeline, and
organizes files by extension or custom rules. Every operation defaults to dry-run.
Destructive actions require explicit flags and are always reversible unless you
add `--permanent`.

CleanSweep requires Python 3.10 or higher and no third-party packages.

---

## 2. Features

**Scanning**
- Recursive directory traversal using an iterative DFS stack (no recursion limit)
- Configurable depth, symlink policy (ignore / follow / error), and file exclusions
- O(depth) memory — never loads the full file list into RAM

**Duplicate detection**
- Three-stage pipeline: group by size → partial SHA-256 (4 KB) → full SHA-256
- Concurrent hashing with a bounded thread pool
- Configurable keep strategy: `oldest`, `newest`, or `first` (alphabetical)
- JSON report export (atomic write — crash-safe)

**Organization**
- Extension-based routing to subdirectories using a built-in ruleset
- Custom rule files with priority, size constraints, and filename glob matching
- All moves are atomic (temp-rename protocol); batch failures trigger full rollback
- Conflict policies: `rename` (default), `skip`, or `error`

**Safety**
- Every run defaults to `--dry-run` via config (configurable)
- Deletion sends files to the system trash by default; `--permanent` required for hard delete
- All destructive actions are boundary-checked against the scan root
- No silent failures — every skipped or failed file is reported

---

## 3. Installation

CleanSweep is a single-directory Python project with no dependencies.

```bash
git clone https://github.com/Jeffrin-dev/cleansweep.git
cd cleansweep
```

**Verify it works:**

```bash
python3 main.py --version
# CleanSweep 3.0.0
```

**Optional: create an alias**

```bash
# Add to ~/.bashrc or ~/.zshrc
alias cleansweep="python3 /path/to/cleansweep/main.py"
```

**Requirements:**
- Python 3.10+
- No `pip install` required — standard library only

---

## 4. Quick start

**Scan a directory and see what's there:**

```bash
cleansweep scan ~/Downloads
cleansweep scan ~/Downloads --summary-only
cleansweep scan ~/Downloads --unit MB
```

**Find duplicates (read-only — no changes made):**

```bash
cleansweep duplicates ~/Downloads
```

**Preview what a duplicate cleanup would do:**

```bash
cleansweep duplicates ~/Downloads --dry-run --keep newest
```

**Delete duplicates (moves to system trash — reversible):**

```bash
cleansweep duplicates ~/Downloads --delete --keep newest
```

**Preview file organization:**

```bash
cleansweep organize ~/Downloads --dry-run
```

**Organize files (moves them into subdirectories):**

```bash
cleansweep organize ~/Downloads
```

> **Note:** `default_dry_run` is `true` in the default config. To actually move
> files you must either pass `--dry-run false` or set `"default_dry_run": false`
> in your config.json. This is intentional — CleanSweep never moves files without
> your explicit acknowledgement.

---

## 5. CLI reference

### Global flags

```
cleansweep [--config FILE] [--version] [--help] COMMAND ...
```

| Flag | Description |
|---|---|
| `--config FILE` | Path to a JSON config file. Defaults to `./config.json` if present. |
| `--version` | Print `CleanSweep 3.0.0` and exit. |
| `--help` | Show usage and exit. |

---

### `cleansweep scan PATH`

List files and report sizes. **Read-only — never modifies the filesystem.**

```bash
cleansweep scan ~/Documents
cleansweep scan ~/Documents --summary-only
cleansweep scan ~/Documents --unit MB --verbose
```

| Flag | Description |
|---|---|
| `PATH` | Directory to scan (required). |
| `--summary-only` | Print only total file count and size; skip the per-file list. |
| `--unit KB\|MB` | Display unit. Default: `KB`. |
| `--verbose` | Show INFO-level messages and timing. |
| `--quiet` | Suppress all output except errors. |
| `--debug` | Show all diagnostic messages. |

---

### `cleansweep duplicates PATH`

Detect exact duplicate files. By default, reports only — no files are changed.

```bash
cleansweep duplicates ~/Downloads
cleansweep duplicates ~/Downloads --dry-run --keep newest
cleansweep duplicates ~/Downloads --delete --keep oldest
cleansweep duplicates ~/Downloads --delete --permanent
cleansweep duplicates ~/Downloads --json-report /tmp/dupes.json
```

| Flag | Description |
|---|---|
| `PATH` | Directory to scan (required). |
| `--keep oldest\|newest\|first` | Which copy to keep. `oldest` = earliest mtime. `newest` = latest mtime. `first` = alphabetically first path. Default: `oldest`. |
| `--delete` | Move duplicates to the system trash (reversible). Without this flag, CleanSweep only reports. |
| `--permanent` | Hard-delete instead of trashing. **Irreversible.** Requires `--delete`. |
| `--dry-run` | Simulate the full operation without changing any files. |
| `--json-report FILE` | Write a JSON duplicate report to FILE (atomic write). |
| `--workers N` | Parallel hashing threads (1–cpu_count×4). Default: min(32, cpu_count+4). |
| `--verbose` | Show timing summary per phase. |
| `--quiet` | Suppress all output except errors. |
| `--debug` | Show all diagnostic messages including per-file hash decisions. |

---

### `cleansweep organize PATH`

Move files into subdirectories by type using the built-in or a custom ruleset.

```bash
cleansweep organize ~/Downloads --dry-run
cleansweep organize ~/Downloads
cleansweep organize ~/Downloads --rules-file my_rules.json
cleansweep organize ~/Downloads --policy strict
cleansweep organize ~/Downloads --report-file organize_log.txt
```

| Flag | Description |
|---|---|
| `PATH` | Directory to organize (required). Only direct children are moved — not recursive. |
| `--dry-run` | Preview moves without modifying any files. |
| `--rules-file FILE` | Custom JSON rule file. When omitted, built-in rules are used. |
| `--policy strict\|safe\|warn` | Conflict resolution when a file matches multiple rules. `strict` = abort. `safe` = skip file (default). `warn` = use first match, log warning. |
| `--report-file FILE` | Write execution summary to FILE (UTF-8 text). Optional; non-fatal if write fails. |
| `--verbose` | Show INFO-level messages. |
| `--quiet` | Suppress all output except errors. |
| `--debug` | Show all diagnostic messages. |

**Built-in categories:**

| Category | Extensions |
|---|---|
| Images | `.jpg` `.jpeg` `.png` `.gif` `.webp` `.svg` `.heic` `.tiff` |
| Documents | `.pdf` `.docx` `.doc` `.txt` `.xlsx` `.xls` `.csv` `.md` `.odt` |
| Videos | `.mp4` `.mkv` `.avi` `.mov` `.wmv` `.flv` `.webm` |
| Audio | `.mp3` `.wav` `.flac` `.aac` `.ogg` `.m4a` `.opus` |
| Archives | `.zip` `.tar` `.gz` `.bz2` `.rar` `.7z` `.xz` |
| Code | `.py` `.js` `.ts` `.jsx` `.tsx` `.html` `.css` `.json` `.yaml` `.yml` `.toml` `.sh` `.rs` `.go` `.java` `.cpp` `.c` `.h` |
| Others | everything else |

---

## 6. Configuration guide

CleanSweep looks for `config.json` in the current working directory. Override
with `--config /path/to/file.json`. CLI flags always override config values.

**Full reference:**

```json
{
  "version": "2.7",
  "scan": {
    "recursive": true,
    "symlink_policy": "ignore",
    "max_depth": null,
    "exclude": []
  },
  "ignore_extensions": [".log", ".tmp", ".DS_Store"],
  "ignore_folders":    ["node_modules", ".git", "__pycache__", ".venv"],
  "default_scan_path": null,
  "default_dry_run":   true,
  "min_file_size":     0,
  "hash_chunk_size":   1048576,
  "log_level":         "INFO",
  "workers":           null,
  "policy_mode":       "safe"
}
```

| Key | Type | Default | Description |
|---|---|---|---|
| `version` | string | required | Config schema version. Must be `"2.7"`. |
| `scan.recursive` | bool | `true` | Descend into subdirectories. |
| `scan.symlink_policy` | string | `"ignore"` | `"ignore"`, `"follow"`, or `"error"`. |
| `scan.max_depth` | int or null | `null` | Max directory depth below scan root. `null` = unlimited. |
| `scan.exclude` | list[string] | `[]` | Glob patterns (fnmatch) matched against filenames and directory names. |
| `ignore_extensions` | list[string] | `[".log", ".tmp"]` | Skip files with these extensions. |
| `ignore_folders` | list[string] | `["node_modules", ".git"]` | Skip directories with these names. |
| `default_dry_run` | bool | `true` | When `true`, all commands simulate unless `--dry-run false`. |
| `min_file_size` | int | `0` | Skip files smaller than this many bytes. |
| `hash_chunk_size` | int | `1048576` | Bytes read per chunk during hashing. |
| `log_level` | string | `"INFO"` | `"ERROR"`, `"WARN"`, `"INFO"`, or `"DEBUG"`. |
| `workers` | int or null | `null` | Hashing thread count. `null` = auto (min(32, cpu_count+4)). |
| `policy_mode` | string | `"safe"` | Default rule conflict policy for `organize`. |

---

## 7. Rule engine

Custom rules let you define exactly which files go where. Create a JSON file
and pass it to `cleansweep organize --rules-file my_rules.json`.

### Schema version 2.2 (current)

```json
{
  "version": "2.2",
  "rules": [
    {
      "name": "Large images",
      "priority": 1,
      "match": {
        "extensions": [".jpg", ".png", ".tiff"],
        "min_size": 10485760
      },
      "destination": "LargeImages"
    },
    {
      "name": "Images",
      "priority": 2,
      "match": {
        "extensions": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]
      },
      "destination": "Images"
    },
    {
      "name": "Documents",
      "priority": 5,
      "match": {
        "extensions": [".pdf", ".docx", ".txt", ".xlsx", ".csv", ".md"]
      },
      "destination": "Documents"
    },
    {
      "name": "Temp files",
      "priority": 0,
      "match": {
        "filename_pattern": "*.tmp"
      },
      "destination": "Temp"
    }
  ],
  "default_destination": "Others"
}
```

### Rule fields

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique identifier. |
| `priority` | No | Integer. Lower = evaluated first. Default: `0`. |
| `match.extensions` | No* | List of file extensions (case-insensitive). |
| `match.min_size` | No* | Minimum file size in bytes (inclusive). |
| `match.max_size` | No* | Maximum file size in bytes (inclusive). |
| `match.filename_pattern` | No* | fnmatch glob matched against the filename (case-insensitive). |
| `destination` | Yes | Logical key for the target subdirectory. |

*At least one `match` criterion is required per rule.

### Evaluation semantics

- Rules are sorted by `(priority ASC, config_order ASC)` at parse time
- First rule where **all** present criteria match wins (strict AND — no OR)
- If no rule matches, the file goes to `default_destination` (or is skipped if not set)
- `--policy strict` aborts on any conflict; `safe` skips ambiguous files; `warn` uses the first match

### Schema version 2.0 (legacy, still supported)

Version 2.0 rules support only `extensions` and `destination`. The `priority`,
`min_size`, `max_size`, and `filename_pattern` fields are not available in v2.0
files. CleanSweep will parse v2.0 files correctly in v3.x.

---

## 8. Duplicate detection

CleanSweep uses a three-stage pipeline designed to minimise I/O while
guaranteeing that only byte-identical files are declared duplicates.

**Stage 1 — Size grouping**  
Files are grouped by byte count. Files with a unique size cannot be duplicates
and are immediately eliminated from consideration.

**Stage 2 — Partial hash**  
For each size group, the first 4 KB of each file is hashed with SHA-256. Files
with unique partial hashes are eliminated.

**Stage 3 — Full hash**  
Remaining candidates are fully hashed. Only files with an identical full
SHA-256 digest are declared duplicates.

This three-stage design means that on a typical directory, the vast majority of
files never need a full read. Small files (≤ 4 KB) are fully read in stage 2
and cached, so they incur zero extra I/O in stage 3.

**Keep strategies:**

| Strategy | Behaviour |
|---|---|
| `oldest` (default) | Keep the file with the earliest modification time. |
| `newest` | Keep the file with the latest modification time. |
| `first` | Keep the alphabetically first file path. |

Ties are broken by path string comparison (deterministic).

**JSON report format:**

```json
[
  {
    "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "size": 1048576,
    "count": 3,
    "keep": "/home/user/docs/file.pdf",
    "duplicates": [
      "/home/user/backup/file.pdf",
      "/home/user/old/file.pdf"
    ]
  }
]
```

---

## 9. Safety guarantees

**Default dry-run**  
`default_dry_run: true` in the default config. No files are moved or deleted
until you explicitly disable this.

**Explicit delete flag**  
`--delete` must be passed for any file deletion to occur. There is no
auto-clean, smart-clean, or implicit deletion mode.

**Reversible deletion by default**  
`--delete` moves files to the system trash (XDG on Linux, `~/.Trash` on macOS,
Recycle Bin on Windows). Files can be restored from there. Hard deletion
requires both `--delete` and `--permanent`.

**Boundary checking**  
Every file targeted for a move or delete is validated to be inside the scan
root. Attempts to operate on files outside the root are rejected with an error.

**Atomic moves**  
Files are moved using a temp-rename protocol. A crash during a move leaves
a `.cleansweep_tmp_*` file that can be safely deleted — it never leaves the
destination in a corrupted state.

**Atomic JSON export**  
Reports are written to a temp file and renamed atomically. A crash during
export never produces a partial JSON file.

**No silent failures**  
Every file that could not be processed (permission denied, broken symlink,
hash failure) is recorded and reported. The process exits with code 4 if
any file failed in a batch operation.

**Graceful degradation**  
A single unreadable directory or file never aborts the scan. CleanSweep
records the error, continues, and reports all failures at the end.

---

## 10. Architecture

CleanSweep is organized into strict layers. The import graph is locked — no
circular dependencies exist and none may be introduced.

```
CLI layer           main.py
                       │
Config layer        config.py
                       │
Engine layer  ┌────────┼─────────┬──────────┐
              │        │         │          │
           scanner  duplicates  rules   destination_map
              │        │         │          │
Action layer  └──── action_controller ──────┘
                    file_operation_manager
                    trash_manager
                       │
Report layer        report.py ← analyzer.py
Observability       logger.py, timer.py
```

**Module responsibilities:**

| Module | Responsibility | May NOT |
|---|---|---|
| `main.py` | Argument parsing, dispatch, exit codes | Contain business logic or filesystem mutations |
| `scanner.py` | File traversal, snapshot collection | Print, mutate files, import rules |
| `duplicates.py` | Duplicate detection pipeline | Print, mutate files, import action_controller at runtime |
| `rules.py` | Rule parsing and evaluation (pure) | Access filesystem, print, import scanner or duplicates |
| `destination_map.py` | Path template resolution (pure) | Access filesystem, import rules |
| `organizer.py` | Coordinate file moves | Bypass action_controller for destructive ops |
| `batch_engine.py` | Atomic batch execution with rollback | Contain rule evaluation logic |
| `action_controller.py` | Gate for all destructive filesystem actions | Be called from engine modules |
| `report.py` | All terminal output | Modify execution logic or engine state |
| `config.py` | Config loading and validation | Print, access filesystem outside config file |
| `logger.py` | Centralised logging | Affect engine behaviour |
| `timer.py` | Phase timing | Affect engine behaviour |

---

## 11. Performance benchmarks

Measured on two environments:
- **Container**: virtual CPU, tmpfs
- **Real hardware**: HP 245 G7 (AMD CPU, ext4 SSD)

### Traversal throughput (`scan` command)

| Files | Container | Container files/sec | Real hardware | Real hardware files/sec |
|---|---|---|---|---|
| 1,000 | ~0.2s | ~5,000 | < 0.05s | — |
| 10,000 | ~4s | ~2,400 | < 0.5s | — |
| 100,000 | ~40s | ~2,500 | ~18s | ~5,600 |
| **1,000,000** | **~7 min** | **~2,500** | **177.93s** | **5,620** |

Memory model: O(depth × branching_factor) for the DFS stack — never O(total_files).  
At 1M files, peak stack memory is under 1 MB.

### Hashing throughput (`duplicates` command)

| File size | Workers | Container files/sec | Real hardware files/sec |
|---|---|---|---|
| 128 B | 4 | ~2,000 | — |
| 512 B | 4 | ~808 | ~2,040 |
| 4 KB | 4 | ~400 | — |
| 1 MB | 4 | disk-limited | disk-limited |

**Tuning for large directories:**

```bash
# Increase workers on machines with fast SSDs (NVMe)
cleansweep duplicates ~/large_dir --workers 16

# Reduce workers on spinning disks to avoid seek thrashing
cleansweep duplicates ~/large_dir --workers 2
```

---

## 12. Exit codes

Exit codes are locked permanently as of v1.9.0. They will not change in v3.x or v4.x.

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Unexpected runtime error |
| `2` | Invalid arguments (bad flag, constraint violation, missing required arg) |
| `3` | Filesystem error (path not found, not a directory, permission denied on root) |
| `4` | Partial failure — some files failed; others succeeded. See output for details. |
| `130` | Interrupted by Ctrl+C |

Use exit codes in scripts:

```bash
cleansweep duplicates ~/Downloads --delete --keep newest
if [ $? -eq 4 ]; then
    echo "Warning: some deletions failed. Check output above."
fi
```

---

## Licence

MIT. See `LICENSE` for the full text.
