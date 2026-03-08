# CleanSweep v2.2.0 — Non-Negotiable Invariants

These rules must not be violated. Any PR or change that breaks an invariant is rejected.

---

## 1. Determinism Invariant

- Snapshot list sorted immediately after collection: `(size, device, inode, path)`
- Size groups: entries sorted within each group
- Hash groups: entries sorted within each group
- Final duplicate groups: sorted by `(size, path)`
- JSON export: `sort_keys=True`, absolute paths only
- No raw `dict` or `set` iteration used for output ordering
- Rule evaluation order: sorted by `(priority ASC, config_index ASC)` at parse time — never at evaluation time

**Test:** Run scan twice on unchanged data. Diff outputs. Must be identical.

---

## 2. Safety Invariant

Every filesystem call must be inside `try/except OSError` (or broader):

| Call | Required guard |
|---|---|
| `iterdir()` | `try/except (PermissionError, FileNotFoundError, OSError)` |
| `is_file()` | `try/except OSError` |
| `is_symlink()` | `try/except OSError` |
| `stat()` | `try/except OSError` |
| `open()` / `os.open()` | `try/except OSError` |
| `unlink()` | `try/except (FileNotFoundError, OSError)` |

**No check-then-act patterns.** Never: `if path.exists(): open(path)`.  
Always: `try: open(path) except OSError: skip`.

---

## 3. Correctness Invariant

- Duplicate declared **only** after full SHA-256 hash match
- Size match alone: insufficient
- Partial hash match: insufficient — used only to filter candidates
- Wasted bytes: calculated only from confirmed full-hash duplicate groups
- Snapshot verification (inode + mtime_ns) required before accepting hash

---

## 4. Concurrency Invariant

- Worker pool: `min(32, os.cpu_count() * 2)` — hard cap
- Futures queue: `MAX_WORKERS * 4` — drained before re-filling
- Hash worker: pure function — accepts `FileEntry`, returns `(entry, digest | None)`
- No shared mutable state inside workers
- All group mutations in main thread only

---

## 5. Memory Invariant

- No file content stored in memory
- No whole-file reads — chunked at `CHUNK_SIZE = 1MB`
- Snapshot stores only metadata (`path`, `size`, `inode`, `mtime_ns`, `device`)
- Memory grows linearly with file count, not file size

---

## 6. Output Safety Invariant

- JSON written to temp file, then renamed atomically
- A crash during write must never leave a partial output file
- Exit code non-zero on fatal error
- No corrupted output on failure

---

## 7. Engine Purity Rule

These modules must contain **zero** `print()` statements:

- `duplicates.py`
- `scanner.py`
- `organizer.py`
- `analyzer.py`
- `config.py`
- `rules.py`

All terminal output routes through `report.py` exclusively.

---

## 8. No Circular Imports Rule

Permitted import directions (updated v2.2.0 — reflects actual module graph):

```
stdlib modules  → (no project imports)
logger.py       → stdlib only
timer.py        → stdlib only
config.py       → stdlib only
analyzer.py     → stdlib only
file_operation_manager.py → logger
trash_manager.py          → stdlib only
scanner.py      → file_operation_manager  (TEMP_PREFIX constant only)
duplicates.py   → logger, timer, file_operation_manager, scanner
action_controller.py → logger, trash_manager, file_operation_manager
                       TYPE_CHECKING only: duplicates (FileEntry type annotation)
rules.py        → stdlib only  (fnmatch, re, dataclasses, pathlib, typing)
organizer.py    → action_controller, file_operation_manager, rules
report.py       → analyzer
main.py         → all modules above
```

Hard rules (permanent):
- No engine module (scanner, duplicates, analyzer, rules) may import report.py
- No engine module may import main.py
- No engine module may import config.py
- duplicates.py must never import action_controller at runtime (TYPE_CHECKING guard only)
- action_controller.py must never import duplicates at runtime (TYPE_CHECKING guard only)
- `rules.py` must never be imported by `duplicates.py`, `scanner.py`, or `analyzer.py`
- No circular runtime imports at any depth

Architectural note:
`delete_duplicates()` lived in `duplicates.py` through v2.1, creating a runtime
circular import (duplicates → action_controller → duplicates). It was moved to
`action_controller.py` in the v2.1 patch. The TYPE_CHECKING-guarded import of
`FileEntry` in action_controller.py is not a runtime import and does not create a cycle.

---

## 9. Organizer Isolation Rule

`organizer.py` must never:
- Be called during duplicate analysis
- Modify any file that is part of an active snapshot
- Import from `duplicates.py`

`duplicates.py` must never import from `organizer.py`.

Organizer runs only when `--organize` CLI flag is explicitly passed,  
after any duplicate analysis is complete.

---

## 10. Export I/O Rule (Pending Refactor)

`export_json()` currently lives in `duplicates.py`.  
This is a known architectural debt — export is I/O, not engine logic.

Target state (next refactor):
- `duplicates.py` returns structured data only
- `export_json()` moves to `report.py` or dedicated `export.py`

Until refactored: `export_json()` must use atomic write (temp + rename).  
It must never be called from inside the duplicate analysis pipeline.

---

## 11. Global Sorted Ordering Guarantee (v1.1.0)

**Every externally visible collection must be sorted explicitly before exposure.**

This applies without exception to:

| Collection | Sort key |
|---|---|
| Snapshot (file list after traversal) | `(size, device, inode, str(path))` |
| Size group entries | `(size, device, inode, str(path))` |
| Hash group entries | `(size, device, inode, str(path))` |
| Files inside duplicate groups | keep strategy, then `str(path)` as tiebreaker |
| Duplicate group list | `(size, hash, str(first_path_in_group))` |
| Skipped/error files | `str(path)` |
| JSON export structure | group canonical order + `sort_keys=True` |
| Report printing order (files) | `str(path)` |
| Report printing order (organizer) | `str(file)` |
| Deleted/failed in deletion result | `str(path)` |
| RuleSet.rules tuple | `(priority ASC, config_index ASC)` |

**No output may rely on:**
- `dict` insertion order
- `set` iteration order
- OS `iterdir()` / `scandir()` traversal order
- Thread completion order from `as_completed()`
- Filesystem inode order
- Object `id()`-based ordering
- Random seeds
- Time-based metadata in exported data

**Test requirement:** If the snapshot list is shuffled before the pipeline runs,
JSON output must remain byte-identical to an unshuffled run.

---

## 12. Observability Invariant (v1.2.0 — Permanent)

**Observability is a passive layer. It must never modify, reorder, or influence
duplicate detection results.**

Specific rules:

- Engine modules (`duplicates.py`, `scanner.py`, `organizer.py`, `analyzer.py`)
  call `log_*` functions only — never `print()` directly
- Engine modules never import or check `logger._state` or log level
- `logger.set_log_level()` is called **once** in `main.py` — never again
- Logging failures are silently swallowed — observability never crashes the program
- Timing data (`timer.get_timings()`) is never written to JSON export
- JSON output must be byte-identical under `--quiet`, `--verbose`, and `--debug`
- The 4 verbosity modes are fixed permanently: `ERROR`, `WARN`, `INFO`, `DEBUG`
- Phase names in `timer.PHASE_ORDER` are hardcoded — never dynamically generated
- Log calls inside tight loops must be guarded with `if logger.is_debug():`

---

## 13. Graceful Degradation Invariant (v1.3.0 — Permanent)

**All filesystem errors must degrade gracefully. No unhandled OSError.
No corrupted intermediate state. Ctrl+C must exit cleanly without traceback.**

Specific rules:

- `iterdir()` failure → WARN log + add directory to skipped, continue traversal
- `stat()` PermissionError → WARN log + add file to skipped, never fatal
- `stat()` other OSError → WARN log + add file to skipped, never fatal
- `is_symlink()` / `is_dir()` OSError → DEBUG log + skip entry (race condition)
- `resolve()` OSError → WARN log + add to skipped, never fatal
- Symlink (follow_symlinks=False) → DEBUG log + add to skipped, never hashed
- Broken symlink → caught by symlink policy above, never crashes
- Symlink loop → iterative traversal (no recursion) prevents infinite loop
- Mid-hash read failure → `_hash_entry` returns `(entry, None)`, partial hasher discarded
- `(entry, None)` → recorded via `_record_skip`, never enters hash group
- No partial hash state is ever stored — false duplicates from read failures are impossible
- `KeyboardInterrupt` caught in `main()` — prints clean message to stderr, exits 130
- Atomic JSON write (temp + rename) — Ctrl+C during export leaves no partial file
- All skip recording goes through `_record_skip()` — single point, consistent format
- `stat()` failure inside `organizer._read_file_size()` → returns 0, never fatal;
  size-constrained rules conservatively skip the file (min_size > 0 will not match size=0)

---

## 14. Memory Discipline Invariant (v1.4.0 — Permanent)

**Memory usage must scale O(N) with file count. No file contents stored.**

Rules:

- `CHUNK_SIZE = 1MB` — fixed constant, never dynamically resized per file
- `PARTIAL_BYTES = 4096` — fixed constant for pre-hash window
- `_hash_entry` holds at most one chunk in memory at a time
- Hash state (`hashlib.sha256`) is local to `_hash_entry` — released on return
- `FileEntry` holds metadata only (~370 bytes per file) — never file contents
- `group_by_size` / `group_by_hash` store references to existing `FileEntry` objects — no copies
- After each pipeline phase, the intermediate dict is explicitly `del`'d:
  - `size_groups` deleted after `size_candidates` extracted
  - `partial_groups` deleted after `partial_candidates` extracted
  - `full_groups` deleted after `find_duplicates` completes
  - `snapshot` deleted after pipeline completes
- Worker pool bounded: `MAX_WORKERS = min(32, cpu_count × 2)` — bounded concurrent RAM
- Future queue bounded: `QUEUE_DEPTH = MAX_WORKERS × 4` — no unbounded accumulation

**Test requirement:** `CHUNK_SIZE` must equal `1024 * 1024`. `PARTIAL_BYTES` must equal `4096`.
Any PR that changes these values requires explicit version bump and audit.

---

## 15. Parallel Execution Invariant (v1.5.0 — Permanent)

**Concurrency model is locked. No future version changes the threading strategy.**

Architecture rules:
- Use `ThreadPoolExecutor` only — no `multiprocessing`, no `asyncio`, no raw threads
- One executor per pipeline phase — never nested executors
- Executor always used as context manager — `shutdown()` guaranteed
- Workers are pure functions — zero shared mutable state inside workers
- All aggregation (groups, skipped) happens in main thread only

Worker count rules:
- Default: `min(32, cpu_count + 4)` — Python's I/O-bound heuristic, locked permanently
- User override: `--workers N` or `workers` in config
- Hard floor: 1 (never zero workers)
- Hard cap: `cpu_count × 4` — prevents runaway thread creation
- `resolve_workers()` is the single function that applies all clamping logic

Queue discipline:
- Future queue bounded to `max_workers × _QUEUE_DEPTH_MULTIPLIER` (4)
- Queue drained before refilling — no unbounded future accumulation
- `_QUEUE_DEPTH_MULTIPLIER = 4` is a constant, never dynamically computed

Determinism:
- `as_completed()` order is intentionally nondeterministic (performance)
- All groups sorted by `_ENTRY_KEY` after all futures drain
- Output byte-identical across all worker counts — verified by test

Interrupt safety:
- `KeyboardInterrupt` caught only in `main.py` — never swallowed in workers
- `ThreadPoolExecutor` context manager handles clean shutdown
- Workers finish current chunk, then exit — no abrupt kill

Forbidden forever:
- `asyncio` / `async def`
- `multiprocessing`
- Dynamic auto-scaling of workers at runtime
- Per-file executor creation
- Daemon threads
- Shared mutable state in worker functions
- Third-party concurrency libraries

---

## 16. Safe Action Framework (v1.6.0 — Permanent)

**All destructive filesystem operations on user files route through `ActionController`. Zero exceptions.**

Permitted `unlink()` call sites (complete list, locked permanently):

| Module | Permitted unlink() purpose |
|---|---|
| `ActionController._delete_permanent()` | User file hard delete (permanent mode) |
| `FileOperationManager._cleanup_one()` | CleanSweep-owned `.cleansweep_tmp_*` temp file cleanup |
| `FileOperationManager.atomic_move()` | Cross-device: delete source after successful copy+verify |
| `FileOperationManager._move_to_temp()` | Failed copy cleanup of own temp file |
| `duplicates.export_json()` | Failed atomic write: cleanup of own `.tmp` output file |

Forbidden forever in all other modules:
- `Path.unlink()` — forbidden outside the five sites above
- `shutil.move()` — only in `FileOperationManager.atomic_move()`
- `os.remove()` / `os.rmdir()` / `shutil.rmtree()` — forbidden everywhere

Design rationale:
`FileOperationManager` is the atomic move backend. It must clean up its own
temp files on failure — this is fundamentally different from user file deletion.
`export_json()` owns a `.tmp` file it creates — cleaning it up on write failure
is internal housekeeping, not user-data destruction. Neither bypasses the
boundary check or audit trail that governs user file deletion.

Execution mode rules (locked):
- Default scan → zero filesystem changes
- `--dry-run` → zero filesystem changes, full simulation
- `--delete` → explicit flag required, never implicit
- No auto-clean, smart-clean, or environment-variable-triggered deletion

Boundary rules (locked):
- Every user-file delete/move validated: `path.is_relative_to(scan_root)`
- Violation raises `ValueError` — never silently bypassed
- Directories never deleted
- Symlinks never followed outside scan root

Idempotency (locked):
- `FileNotFoundError` during delete → counted as success
- Running twice with `--delete` never crashes or misreports

Audit trail (locked):
- Every user-file action produces structured event: action, file, size, hash, status, timestamp
- `audit_log()` returns a copy — internal log never exposed as mutable
- Timestamps only in audit log — never in JSON export (preserves determinism)

Victim policy (locked):
- Determined by `--keep` strategy at scan time
- Deterministic: same input → identical keep/delete decisions always
- Never depends on thread timing or filesystem traversal order

Partial failure behavior (locked):
- Failed deletions (duplicate removal) logged, counted, reported
- Remaining files processed — process never aborted on single failure
- `has_failures()` returns `True` → `main.py` exits with code 1

Partial failure behavior for organize (batch rollback — v3.0.0):
- If any move fails during EXECUTE, all completed moves in that batch are reversed
- Rollback uses `actual_dst.rename(original_src)` — same atomic rename primitive
- If a rollback rename fails, it is logged as an error and skipped (best-effort)
- After rollback, executed/bytes_moved counters reset to 0 in BatchReport
- `fail_index` is still set — main.py exits code 4 on any batch failure
- Rollback only applies to the organize pipeline; duplicate deletion is not batched

---

## 17. Trash Strategy Invariant (v1.7.0 — Permanent)

**`--delete` means trash by default. Permanent deletion is double-explicit.**

Mode matrix (locked permanently):
  (none)                  → analyze only, zero filesystem changes
  --dry-run               → full simulation, zero filesystem changes
  --delete                → move to system trash (reversible)
  --delete --permanent    → hard delete (irreversible)
  --permanent (alone)     → rejected immediately with exit code 1

Trash backend rules:
  Linux   → XDG Trash spec (~/.local/share/Trash/files + .trashinfo)
  macOS   → ~/.Trash
  Windows → Recycle Bin via SHFileOperation
  All OS logic isolated in TrashManager — nothing else touches trash

Fallback policy (locked permanently):
  TrashUnavailableError → abort deletion, record failure, never silent permanent fallback
  Hint "Use --permanent" included in every TrashUnavailableError failure record
  No environment variable, config key, or flag can enable silent permanent fallback

TrashManager rules:
  Instantiated once per ActionController
  trash() returns TrashResult — never raises into caller on already-gone files
  Collision names resolved: file_2.txt, file_3.txt, ...
  .trashinfo written before shutil.move — cleaned up if move fails

Forbidden forever:
  Silent fallback from trash to permanent delete
  --permanent without --delete
  Trash logic scattered outside TrashManager
  Modifying hashing pipeline, memory system, or victim selection for trash purposes

---

## 18. Rule Engine Invariant (v2.2.0 — Permanent)

**All organization category decisions route through `rules.resolve_destination()`. Zero exceptions.**

### Architecture rules (locked permanently):

- `rules.py` is the sole authority for rule parsing, validation, and destination resolution
- `resolve_destination(filename, ruleset, file_size)` is a **pure function** — no filesystem access, no side effects, no print(), no exit()
- `parse_rules(data)` returns an immutable `RuleSet` or raises `RuleError` — never silently corrects bad input
- `organizer.py` calls `resolve_destination()` only — it never evaluates extensions or sizes directly
- `organizer._read_file_size(path)` is the only site in the organizer layer that calls `stat()` for size — result passed as plain int to the pure rule engine
- No module other than `rules.py` may parse or validate rule JSON

### Schema versioning (locked permanently):

- Accepted schema versions: `"2.0"` and `"2.2"`
- Current version: `SCHEMA_VERSION = "2.2"`
- Version `"2.0"` files parse with the strict v2.0 validator — `priority`, `min_size`, `max_size`, `filename_pattern` are hard errors in v2.0 files
- Version `"2.2"` files parse with the expanded v2.2 validator
- Unknown versions (e.g. `"1.0"`, `"2.1"`, `"3.0"`) are hard errors — no silent fallback
- New schema versions require an explicit minor version bump

### v2.2 rule criteria (locked permanently):

Each rule may contain any combination of:

| Criterion | Field | Type | Semantics |
|---|---|---|---|
| Extension match | `match.extensions` | list of strings | File suffix must be in the set (case-insensitive) |
| Minimum size | `match.min_size` | int (bytes) | File size must be ≥ value (inclusive) |
| Maximum size | `match.max_size` | int (bytes) | File size must be ≤ value (inclusive) |
| Filename glob | `match.filename_pattern` | fnmatch string | Filename must match the glob (case-insensitive) |

Evaluation is **strict AND** — all present criteria must pass. No OR. No partial matching.  
An empty `match` object (zero criteria) is a hard parse error — it would match everything, which is never the intent.  
In v2.2, `extensions` is optional; at least one criterion is required.

### Priority rules (locked permanently):

- `priority` is an explicit integer field at the rule level (v2.2 only)
- Lower number = evaluated first (e.g. `priority: 1` is evaluated before `priority: 5`)
- Default priority when field is absent: `0` — documented, not hidden
- Stable sort: equal priority → config file order preserved as the tiebreaker
- Sort applied at `parse_rules()` return time — `RuleSet.rules` is already sorted
- No runtime re-sorting — sort order is fixed for the lifetime of the `RuleSet`
- `priority` must be a plain `int` — `float`, `bool`, `str`, and `None` are hard errors

### Size constraint rules (locked permanently):

- All size values are raw bytes — no unit parsing in the engine core
- Caller (organizer) reads file size via `stat()` and passes as `int` — no strings, no units
- Both `min_size` and `max_size` are inclusive boundaries
- `max_size < min_size` is a hard parse error — caught at `parse_rules()` time
- `bool` is explicitly rejected even though Python's `bool` is a subclass of `int`
- Size-constrained rules use `file_size=0` as the default when the caller does not supply a size — this is the safe conservative choice: a rule with `min_size > 0` will not match

### Filename pattern rules (locked permanently):

- `fnmatch` glob syntax only — no regex, no partial regex interpretation
- Applied to the filename (basename) only — never to the full path
- Case-insensitive: both pattern and filename lowercased before `fnmatch.fnmatchcase()` call
- Leading/trailing whitespace is a hard error at parse time
- Empty string is a hard error at parse time

### Conflict detection rules (locked permanently):

- Duplicate rule names → `RuleError` at parse time (names identify rules, must be unique)
- Overlapping extensions across rules → **allowed** — priority order is the resolver
- Intra-rule duplicate extensions after normalization → `RuleError` at parse time
- Conflict detection runs after normalization — `.JPG` and `jpg` are the same extension

### Extension normalization rules (locked permanently):

- All extensions lowercased at parse time
- Leading dot ensured at parse time (`jpg` → `.jpg`, `.JPG` → `.jpg`)
- Trailing/leading whitespace on an extension is a hard parse error — not silently stripped
- `resolve_destination()` lowercases the filename suffix before matching

### Evaluation semantics (locked permanently):

- `RuleSet.rules` tuple is sorted at parse time by `(priority ASC, config_index ASC)` — immutable thereafter
- `resolve_destination()` iterates the tuple linearly; first rule where all criteria match wins
- No parallel rule evaluation
- No runtime rule mutation — `RuleSet` and `Rule` are frozen dataclasses
- `_ext_index` retained in `RuleSet` as introspection metadata; not used by `resolve_destination()`

### RuleSet lifecycle (locked permanently):

- `DEFAULT_RULESET` built at module import time — never rebuilt at runtime; uses v2.0 schema
- User-supplied ruleset loaded once per `cleansweep organize` invocation
- Loaded via `--rules-file FILE`; parsed by `_load_ruleset()` in `main.py`
- `RuleError` during load → `_cli_error()` → exit code 2

### Forbidden forever:

- Hardcoded extension lists inside `organizer.py` for routing decisions
- Silent fallback when `--rules-file` contains invalid JSON
- Rule evaluation inside any module other than `rules.py`
- Dynamic rule mutation after `parse_rules()` returns
- AI/heuristic/probabilistic destination inference
- Regex inside rule matching (fnmatch only)
- Unit parsing for size values in the engine core (bytes only)
- Implicit OR semantics between criteria in a single rule
- Importing `rules.py` from `duplicates.py`, `scanner.py`, or `analyzer.py`

---

## 19. Traversal Engine Invariant (v2.1.0 — Permanent)

**All file traversal routes through `scan_files()` or `collect_snapshot()` with `ScanPolicy`.
Zero hardcoded depth, symlink behavior, or exclusion logic outside these two functions.**

### ScanPolicy contract (locked permanently):

- `ScanPolicy` is a frozen dataclass — immutable after construction
- Invalid values (`symlink_policy` not in allowed set, `max_depth < 0`, `min_file_size < 0`) raise `ValueError` at construction time — never at scan time
- All traversal decisions are encoded in `ScanPolicy` before scanning begins
- No traversal parameter may be passed ad-hoc outside a `ScanPolicy`

### scan_files() guarantees (locked permanently):

- Returns a `Generator[Path, None, None]` — never a list, never a tuple
- Iterative DFS via explicit stack — zero Python recursion (no `RecursionError` on deep trees)
- Stack memory: O(depth × branching_factor) — never O(total_file_count)
- Per-directory entries sorted by name — deterministic yield order regardless of OS
- Single `stat()` per file (only when `min_file_size > 0`) — no redundant metadata reads
- `OSError` on any single entry → entry skipped, traversal continues (no fatal errors)
- Zero `print()` calls, zero `sys.exit()` calls, zero rule engine calls, zero file mutations

### Depth semantics (locked permanently):

- `recursive=False` → identical behavior to `max_depth=0` — only root directory files
- `max_depth=N` → descend at most N directory levels below root (root = level 0)
- `max_depth=None` + `recursive=True` → unlimited traversal
- Depth is measured from the scan root, not from the filesystem root

### Symlink policies (locked permanently):

- `"ignore"` (default) — symlinks never yielded, never descended into
- `"follow"` — symlinked directories followed with cycle detection; symlinked files yielded as their own path (not resolved to target)
- `"error"` — `ScanError` raised immediately on first symlink encountered
- No other policy values are valid — unknown values raise `ValueError` at ScanPolicy construction
- Default is `"ignore"` — never guessing, never OS-dependent

### Cycle detection (locked permanently):

- Activated only when `symlink_policy="follow"`
- Tracking mechanism: `set[tuple[int, int]]` of `(inode, device)` pairs
- Root directory seeded into visited set before traversal begins
- Each symlinked directory: stat() the target, check (inode, dev) against visited set
- Cycle detected → entry recorded in skipped as `"cycle_detected"`, traversal continues
- Regular (non-symlink) directories: not cycle-checked (filesystem DAG guarantee)

### Glob exclusion semantics (locked permanently):

- Applied via `fnmatch.fnmatch()` against the **filename or dirname only** (not the full path)
- Case-insensitive: both pattern and name lowercased before matching
- Applied to files and directories equally — excluded dirs are never descended into
- Applied before any `stat()` call — excluded entries produce zero filesystem I/O
- Exclusion is independent of the rule engine — `scanner.py` never imports `rules.py`

### Backward compatibility (locked permanently):

- `list_files()` signature is stable — all v1.x callers work unchanged
- `follow_symlinks=True` maps to `symlink_policy="follow"` when `symlink_policy` not given
- `follow_symlinks=False` maps to `symlink_policy="ignore"` when `symlink_policy` not given
- Explicit `symlink_policy` always overrides `follow_symlinks`
- `collect_snapshot()` legacy parameters (`follow_symlinks`, `max_depth`) still accepted when `policy=None`

### Forbidden forever:

- Recursive Python functions for directory traversal (stack overflow risk)
- Hardcoded depth limits, symlink behavior, or exclusion logic outside `ScanPolicy`
- Regex-based exclusion (only `fnmatch` glob in v2.x)
- Storing the full file list in memory inside `scan_files()` (generator only)
- Calling `resolve()` on symlinked files when `symlink_policy="follow"` (would lose identity)
- Importing `rules.py`, `organizer.py`, `action_controller.py`, or `report.py` from `scanner.py`
- Parallel scanning (no `ThreadPoolExecutor` in scanner — reserved for hashing only)

---

## 20. Destination Mapping Invariant (v2.3.0 — Permanent)

**All physical filesystem path construction routes through `destination_map.resolve_destination_path()` when a `DestinationMap` is active. Zero exceptions.**

### Three-layer flow (locked permanently):

```
Rule engine  →  dest_key (logical, no path)
Mapping layer → absolute Path (pure, no I/O)
Organizer    →  mkdir + atomic move (all I/O here only)
```

### Architecture rules (locked permanently):

- `destination_map.py` is the sole authority for destination map parsing, validation, and path resolution
- `resolve_destination_path(dest_key, filename, file_size, mtime_ns, dest_map)` is a **pure function** — no filesystem access, no side effects, no `print()`, no `exit()`
- `parse_destination_map(data, default_base_dir)` returns an immutable `DestinationMap` or raises `DestinationMapError` — never silently corrects bad input
- `organizer.py` calls `resolve_destination_path()` after `resolve_destination()` when a `DestinationMap` is active; it never constructs file paths directly
- `organizer._create_destination_dir(path)` is the only place directories are created — NOT in the mapping layer
- `organizer._read_file_mtime(path)` is the only site that reads `mtime_ns` for template resolution — result passed as plain `int | None` to the pure mapping layer

### Destination map schema (locked permanently):

- Accepted schema version: `"2.3"` only
- Required keys: `version`, `destinations`
- Optional keys: `base_dir`, `conflict_policy`, `template_fallback`
- `destinations` must be a non-empty dict of `{logical_key: path_template}` pairs
- Destination keys must not contain `/`, `\\`, or null bytes
- Path templates may contain only known template variables: `{extension}`, `{year}`, `{month}`, `{size_bucket}`
- Unknown template variables are a hard parse error — they would silently produce garbage paths

### Template variable semantics (locked permanently):

| Variable | Source | Fallback when unresolvable |
|---|---|---|
| `{extension}` | File suffix, lowercased, without dot | `template_fallback` |
| `{year}` | UTC year from file `mtime_ns` | `template_fallback` |
| `{month}` | UTC zero-padded month (01–12) from `mtime_ns` | `template_fallback` |
| `{size_bucket}` | `small` / `medium` / `large` from `file_size` | N/A (`file_size=0` → `small`) |

Size bucket thresholds (locked permanently):
- `small`: `file_size < 1_048_576` (< 1 MB)
- `medium`: `1_048_576 ≤ file_size < 104_857_600` (1 MB – 100 MB)
- `large`: `file_size ≥ 104_857_600` (≥ 100 MB)

### Early validation rule (locked permanently):

- `validate_ruleset_destinations(ruleset_destinations, dest_map)` must be called before the first file is processed
- Missing keys → `DestinationMapError` immediately, before any filesystem mutation
- This is non-negotiable — partial organizes caused by missing keys corrupt user state

### Conflict policy rules (locked permanently):

- `rename` (default) — `FileOperationManager.atomic_move()` handles collisions with deterministic suffix: `file (1).txt`, `file (2).txt`, etc.
- `skip` — file left in place if `dst_dir/filename` already exists; recorded in results with `status: "skipped"`
- `error` — organize aborts with error if any collision is detected; no files moved after collision

### Directory creation rules (locked permanently):

- `_create_destination_dir(path)` in `organizer.py` — only site that calls `mkdir()`
- `parents=True, exist_ok=True` — idempotent, recursive, no crash on existing dirs
- Created before any file moves — all dirs created as a batch before the move loop
- Never in `destination_map.py` — mapping layer is pure

### Import graph additions (locked permanently — extends §8):

```
destination_map.py → stdlib only (datetime, re, dataclasses, pathlib, typing)
organizer.py       → action_controller, file_operation_manager, rules, destination_map
main.py            → destination_map (parse_destination_map, DestinationMap, DestinationMapError)
```

Hard rules (extending §8, permanent):
- `destination_map.py` must never import `organizer.py`, `rules.py`, `scanner.py`, `duplicates.py`, or `report.py`
- `rules.py` must never import `destination_map.py`
- No module may call `mkdir()` on a destination path except `organizer._create_destination_dir()`
- No rule definition may contain a filesystem path — only logical destination key strings

### Forbidden forever:

- Absolute paths inside rule `destination` fields
- Template logic inside `rules.py` or `organizer.py` (template layer is `destination_map.py` only)
- Filesystem access inside `destination_map.py`
- Implicit directory creation during path resolution
- Silent overwrite (all conflict policies are explicit)
- Non-deterministic renaming (random/timestamp-based suffixes)
- Unknown template variables silently passed through as literal text
- Partial organize state from missing destination key (must fail before first move)

---

## 20. Reporting Invariants (v2.5.0 — Permanent)

These rules govern the audit and report system added in v2.5.0.
They must never be violated in any future version.

### Separation of concerns (locked permanently):

- **Reporting must not modify execution logic.**
  No function in `report.py` may alter, re-order, or influence any engine operation.
  `report.py` is a read-only consumer of structured data produced by other modules.

- **Metrics must be collected only inside `batch_engine.py`.**
  No other module may increment or write `total_bytes_moved`, `destinations_created`,
  or any execution counter on `BatchReport`.
  Planner, organizer, rules, and scanner are metric-free layers.

- **`report.py` must be the only module producing output summaries.**
  The execution summary (both console and file) is generated exclusively by
  `_format_execution_summary()` inside `report.py`.
  No other module may produce summary-level formatted output.

### Determinism rules (locked permanently):

- **Report formatting must be deterministic.**
  `_format_execution_summary()` is a pure function: identical inputs always produce
  identical output strings. No timestamps, random values, or environment-dependent
  text may appear in the summary.

- **File reports must match console reports exactly.**
  `display_execution_summary()` and `save_report_file()` both call
  `_format_execution_summary()` on the same inputs.
  Any divergence between console and file content is a bug.

### Operational rules (locked permanently):

- **Reporting must function in both dry-run and execution modes.**
  All summary fields are valid and populated regardless of `dry_run`.
  In dry-run: `total_bytes_moved` reflects simulated sizes; `destinations_created = 0`
  (no directories are actually created in dry-run).

- **`--report-file` is always optional.**
  Omitting the flag must never affect console output or engine behavior.
  Providing the flag must never affect engine behavior.

- **Report file write errors are non-fatal.**
  An `OSError` writing the report file must be surfaced as a warning via `logger`
  and must never change the process exit code.

### Metrics definitions (locked permanently):

| Metric | Source | Notes |
|---|---|---|
| `total_bytes_moved` | `BatchReport.total_bytes_moved` | Sum of `st_size` captured before each move |
| `destinations_created` | `BatchReport.destinations_created` | Dirs newly created by `_prepare()`; 0 in dry-run |
| `failures` | `planned - executed - skipped` | Derived; never stored separately |
| `duration_seconds` | `BatchReport.duration_seconds` | Wall-clock from `run()` entry to return |

### Byte conversion rules (locked permanently):

- Conversion: `bytes → KB → MB → GB` using 1024-based divisors only.
- Always `round(value, 2)` to avoid floating-point noise.
- Display format: `"{value} {unit}"` — no padding, no trailing zeros stripped.
- Thresholds (exclusive lower bound):
  - `>= 1 GB` → display as GB
  - `>= 1 MB` → display as MB
  - `>= 1 KB` → display as KB
  - `< 1 KB` → display as B (integer, no decimal)

### Import graph additions (locked permanently — extends §8):

```
report.py → analyzer, pathlib (Path annotation for save_report_file)
```

No new runtime imports are introduced in v2.5.0 for any other module.

---

## v2.6.0 — Performance Invariants (locked permanently)

### §20 — Single metadata read per file (I/O batching)

> **Effective from v2.6.0.**

`scanner.scan_files()` and `duplicates.collect_snapshot()` use `os.scandir()`
instead of `Path.iterdir()`.  `DirEntry` objects cache inode metadata from the
kernel's readdir buffer.  Callers **must not** call `os.stat()` / `Path.stat()`
a second time on files already visited via `DirEntry` unless filesystem mutation
is expected between the scan and the call.

| Method | Cached? (Linux/macOS) |
|---|---|
| `DirEntry.is_symlink()` | YES — uses lstat cache |
| `DirEntry.is_dir()` | YES — uses lstat cache (for non-symlinks) |
| `DirEntry.is_file()` | YES — uses lstat cache (for non-symlinks) |
| `DirEntry.stat(follow_symlinks=False)` | YES — lstat result cached |
| `DirEntry.stat(follow_symlinks=True)` | YES for regular files; extra syscall only for symlinks |

### §21 — Hash cache scope and validity (v2.6.0)

`duplicates._HashCache` is created once per `run_hash_pipeline()` call and
freed immediately after Phase 3 completes.  It is **not** persisted across
runs.

A cached digest is reused in the full-hash phase only when:

1. Cache key matches: `(path, size, inode, mtime_ns)` — identical to the entry
   stored during the partial-hash phase.
2. The file was fully read during the partial-hash phase, meaning
   `entry.size <= PARTIAL_BYTES` (currently 4 096 bytes).

Violating either condition would allow a stale or wrong digest to be used,
producing false duplicate matches.  The cache **must never** be used when
`entry.size > PARTIAL_BYTES`.

### §22 — Determinism invariant preserved under v2.6.0 changes

- `os.scandir()` returns entries in arbitrary filesystem order.  Entries are
  sorted by name immediately after collection (same as the previous
  `sorted(iterdir())` call).  Output order is unchanged.
- Hash cache hits are inserted into `groups` before cache misses are submitted
  to workers.  All groups are sorted by `_ENTRY_KEY` after all futures complete.
  This preserves the determinism guarantee established in §1 and §4.

### §23 — analyzer.summarize() overload contract (v2.6.0)

`summarize(files)` accepts `list[Path]` **or** `list[FileEntry]`.
Detection is duck-typed on `hasattr(files[0], "size")`.

- `list[FileEntry]`: zero `stat()` calls — uses pre-fetched `.size`.
- `list[Path]`: one `stat()` call per file (legacy behaviour, unchanged).

Callers must not pass mixed lists.  An empty list always returns
`{"count": 0, "total_bytes": 0}` regardless of element type.
