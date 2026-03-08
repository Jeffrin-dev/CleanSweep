# Changelog

All notable changes to CleanSweep are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).  
Versioning follows [Semantic Versioning 2.0.0](https://semver.org/).

---

## [3.0.0] — 2026-03-07

**Production release.** Architecture frozen. Public interface stable.

### Added
- `version.py` — single source of truth for the version string; all modules import from here
- `CONTRIBUTING.md` — development setup, code style, test guide, PR process
- `CODE_OF_CONDUCT.md` — community standards
- `CHANGELOG.md` — this file
- GitHub issue templates: bug report, feature request, performance issue
- Branch policy: `main` (stable), `release/v3` (LTS), `dev` (new work)

### Changed
- `VERSION` constant in `main.py` updated to `"3.0.0"`; source of record is now `version.py`
- `README.md` rewritten: full CLI reference, configuration guide, architecture overview, performance benchmarks, safety guarantees

### Stability guarantees (v3.x)
- `config.json` schema versions `"2.0"` and `"2.2"` remain parseable for the lifetime of v3
- All CLI flags defined in v3.0.0 remain available without change until v4.0.0
- All exit codes (0, 1, 2, 3, 4, 130) are frozen
- No breaking behavior changes inside any v3.x release

---

## [2.9.0] — Stability Freeze Candidate

### Verified
- Cross-module integration tests (57 tests) — full pipeline scan → rules → batch → report
- Stress tests — 10K default; scalable to 1M files via environment variable
- Failure simulation — permission denied, broken symlinks, rename collision, missing source mid-scan
- Determinism tests — two identical runs produce byte-identical output
- Performance baseline — 1M files in 177.93s on real hardware (HP 245 G7, AMD CPU, ext4 SSD)
- Large-scale data generator extended with `create_stress_tree()` and `create_duplicate_stress()`

### Architecture
No module boundaries moved, no APIs added, no behaviors changed from v2.8.

---

## [2.8.0] — Batch Engine Hardening

### Added
- `BatchEngine.run_from_files()` — full planner + executor pipeline with rollback
- Atomic move verification — content hash check before source removal on cross-device moves
- `BatchReport.fail_index` — first failure index for partial-failure exit code

---

## [2.7.0] — CLI Unification

### Changed
- `main.py` fully rewritten as pure dispatcher: no business logic, no filesystem mutation
- `Exit` class introduced — all exit codes documented and locked permanently
- `--rules-file` flag added to `organize` subcommand
- `--report-file` flag added to `organize` subcommand
- `--policy` flag added to `organize` subcommand
- `_cli_error()` — all argument errors use a single exit-2 path

---

## [2.6.0] — Performance Pass

### Changed
- `scanner.scan_files()` and `duplicates.collect_snapshot()` migrated to `os.scandir()`
- `duplicates._HashCache` added — files ≤ 4 KB skip full-hash re-read
- `analyzer.summarize()` overloaded to accept `list[FileEntry]` (zero extra stat calls)

---

## [2.5.0] — Reporting Layer

### Added
- `report.py` — all terminal output centralised; zero `print()` in engine modules
- `BatchReport` dataclass — structured execution summary
- `save_report_file()` — write execution summary to disk (optional, non-fatal on error)
- `display_execution_summary()` — deterministic formatted summary to stdout

---

## [2.4.0] — Rule Engine v2.2

### Added
- `priority` field on rules — explicit integer, lower = evaluated first
- `min_size` / `max_size` criteria — byte-exact boundaries, both inclusive
- `filename_pattern` criterion — `fnmatch` glob on basename, case-insensitive
- Schema version `"2.2"` — backward-compatible with `"2.0"` files
- `DestinationMap` — logical key → filesystem path template mapping
- Template variables: `{extension}`, `{year}`, `{month}`, `{size_bucket}`

---

## [2.3.0] — Destination Map

### Added
- `destination_map.py` — pure mapping layer between rule keys and filesystem paths
- Three-layer flow locked: rule engine → dest key → mapping layer → organizer I/O

---

## [2.2.0] — Invariants Document

### Added
- `INVARIANTS.md` — 20 numbered invariants, all locked permanently
- Rule Engine Invariant (#18)
- Traversal Engine Invariant (#19)
- Destination Mapping Invariant (#20)

---

## [2.1.0] — Circular Import Fix

### Fixed
- `delete_duplicates()` moved from `duplicates.py` to `action_controller.py`
- `FileEntry` imported under `TYPE_CHECKING` guard only — no runtime circular import

---

## [2.0.0] — Atomic Moves & Safe Action Framework

### Added
- `action_controller.py` — all destructive operations gated here
- `file_operation_manager.py` — atomic move with temp-rename protocol
- `trash_manager.py` — XDG/macOS/Windows trash backends
- Safe Action Invariant (#16)
- Trash Strategy Invariant (#17)
- `--delete` required for any file deletion; `--permanent` requires `--delete`

---

## [1.9.0] — CLI Contract Lock

### Locked permanently
- Subcommand structure: `scan`, `duplicates`, `organize`
- Exit code registry: 0, 1, 2, 3, 4, 130

---

## [1.0.0] — Initial Release

- Recursive file scanner with configurable depth and symlink policy
- Duplicate detector: size → partial hash → full SHA-256 pipeline
- Basic organizer: extension-based category routing
- JSON config support
- `--dry-run` flag
