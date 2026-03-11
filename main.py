"""
CleanSweep v3.0.0 — CLI entry point.

Responsibilities of this module (LOCKED):
  - Argument parsing
  - Config loading
  - Log level initialisation
  - Dispatching to engine functions
  - Mapping outcomes to exit codes
  - Printing the interrupt warning

This module MUST NOT:
  - Contain any filesystem mutation logic
  - Read sys.argv outside of build_parser() / main()
  - Call sys.exit() anywhere except main() and _cli_error()

CLI contract (locked permanently as of v1.9.0):
  cleansweep scan       PATH [options]
  cleansweep duplicates PATH [options]
  cleansweep organize   PATH [options]
  cleansweep --version
  cleansweep --help

Exit code policy (locked permanently as of v1.9.0):
  0   — Success
  1   — General / runtime error  (unexpected exception)
  2   — Invalid arguments        (bad flags, constraint violations)
  3   — Filesystem error         (path not found, not a directory)
  4   — Partial failure          (some files failed; see output)
  130 — Interrupted              (Ctrl+C)
"""

import argparse
import json
import sys
from pathlib import Path

from config import load_config, parse_config, AppConfig, ConfigError, MAX_WORKERS_HARD_CAP
import logger
import timer
from action_controller import ActionController, DELETE_MODE_TRASH, DELETE_MODE_PERMANENT
from scanner import validate_folder, list_files, ScanPolicy, ScanError, SYMLINK_IGNORE, SYMLINK_FOLLOW, SYMLINK_ERROR
from rules import parse_rules, RuleSet, RuleError, DEFAULT_RULESET
from duplicates import scan_duplicates, export_json
from report import (
    display_files, display_summary,
    display_organize_result, display_batch_report,
    display_scan_result, display_deletion_preview, display_deletion_result,
    display_json_saved, display_timings,
    display_execution_summary, save_report_file,
)

from version import VERSION


# ---------------------------------------------------------------------------
# Exit code registry — LOCKED permanently as of v1.9.0
# ---------------------------------------------------------------------------

class Exit:
    SUCCESS          = 0    # Normal completion
    GENERAL_ERROR    = 1    # Unexpected runtime error
    INVALID_ARGS     = 2    # Bad flags, constraint violations, missing required args
    FILESYSTEM_ERROR = 3    # Path not found, not a directory, inaccessible
    PARTIAL_FAILURE  = 4    # Some files failed; others succeeded
    INTERRUPTED      = 130  # Ctrl+C


# ---------------------------------------------------------------------------
# CLI error helper — invalid args always exit 2
# ---------------------------------------------------------------------------

def _cli_error(msg: str) -> None:
    """Print a user-facing argument error to stderr and exit INVALID_ARGS (2)."""
    print(f"error: {msg}", file=sys.stderr)
    print("Try 'cleansweep --help' for usage information.", file=sys.stderr)
    sys.exit(Exit.INVALID_ARGS)


# ---------------------------------------------------------------------------
# Shared parser fragments
# ---------------------------------------------------------------------------

def _add_verbosity(parser: argparse.ArgumentParser) -> None:
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--quiet",   action="store_true", help="Suppress all output except errors.")
    grp.add_argument("--verbose", action="store_true", help="Show INFO-level messages and timing summary.")
    grp.add_argument("--debug",   action="store_true", help="Show all diagnostic messages.")


def _add_workers(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workers", "--threads", type=int, default=None, metavar="N", dest="workers",
        help=(
            f"Parallel hashing threads (also accepted as --threads). "
            f"Range: 1\u2013{MAX_WORKERS_HARD_CAP} (cpu_count \u00d7 4). "
            f"Default: min(32, cpu_count+4)."
        ),
    )


# ---------------------------------------------------------------------------
# Argument parser — structure only, zero logic
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="cleansweep",
        description=(
            "CleanSweep \u2014 file scanner, duplicate finder, and organizer.\n"
            "\n"
            "Commands:\n"
            "  scan        List files and report sizes\n"
            "  duplicates  Find (and optionally delete) duplicate files\n"
            "  organize    Sort files into category subdirectories\n"
            "\n"
            "Run 'cleansweep COMMAND --help' for per-command usage."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    root.add_argument(
        "--version", action="version", version=f"CleanSweep v{VERSION}",
        help="Show version and exit.",
    )
    root.add_argument(
        "--config", type=Path, metavar="FILE", default=None,
        help=(
            "Path to a JSON config file. "
            "Defaults to config.json in the current directory if present. "
            "CLI flags always override config file values."
        ),
    )

    sub = root.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ── scan ─────────────────────────────────────────────────────────────────
    scan_p = sub.add_parser(
        "scan",
        help="List files and report sizes.",
        description=(
            "Scan PATH and display each file with its size.\n"
            "This command is read-only \u2014 it never modifies the filesystem.\n"
            "\n"
            "Examples:\n"
            "  cleansweep scan ~/Downloads\n"
            "  cleansweep scan ~/Downloads --summary-only\n"
            "  cleansweep scan ~/Downloads --unit MB\n"
            "\n"
            "Exit codes:\n"
            "  0   Success\n"
            "  2   Invalid arguments\n"
            "  3   PATH does not exist or is not a directory\n"
            "  130 Interrupted (Ctrl+C)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    scan_p.add_argument("path", type=Path, metavar="PATH", help="Directory to scan.")
    scan_p.add_argument("--summary-only", action="store_true",
                        help="Print only total file count and size.")
    scan_p.add_argument("--unit", choices=["KB", "MB"], default="KB",
                        help="Size display unit. Default: KB.")
    _add_verbosity(scan_p)

    # ── duplicates ────────────────────────────────────────────────────────────
    dup_p = sub.add_parser(
        "duplicates",
        help="Find duplicate files. Optionally delete them.",
        description=(
            "Detect duplicate files using a three-stage pipeline:\n"
            "  1. Group by size\n"
            "  2. Group by partial hash (4 KB)\n"
            "  3. Confirm with full SHA-256\n"
            "\n"
            "By default, no files are modified.\n"
            "\n"
            "Examples:\n"
            "  cleansweep duplicates ~/Downloads\n"
            "  cleansweep duplicates ~/Downloads --dry-run\n"
            "  cleansweep duplicates ~/Downloads --delete\n"
            "  cleansweep duplicates ~/Downloads --delete --keep newest\n"
            "  cleansweep duplicates ~/Downloads --delete --permanent\n"
            "  cleansweep duplicates ~/Downloads --json-report report.json\n"
            "\n"
            "Exit codes:\n"
            "  0   Success (includes: no duplicates found)\n"
            "  2   Invalid arguments (e.g. --permanent without --delete)\n"
            "  3   PATH does not exist or is not a directory\n"
            "  4   Partial failure: some deletions failed\n"
            "  130 Interrupted (Ctrl+C)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    dup_p.add_argument("path", type=Path, metavar="PATH", help="Directory to scan.")
    dup_p.add_argument(
        "--keep", choices=["oldest", "newest", "first"], default="oldest",
        help=(
            "Which copy to keep. 'oldest' = earliest mtime. "
            "'newest' = latest mtime. 'first' = alphabetically first path. "
            "Default: oldest."
        ),
    )
    dup_p.add_argument(
        "--delete", action="store_true",
        help=(
            "Move duplicate files to the system trash (reversible). "
            "Without this flag CleanSweep only reports duplicates."
        ),
    )
    dup_p.add_argument(
        "--permanent", action="store_true",
        help=(
            "With --delete: permanently remove files instead of trashing. "
            "IRREVERSIBLE. Requires --delete."
        ),
    )
    # FIX: BooleanOptionalAction enables --dry-run and --no-dry-run.
    # default=None means "user did not set this flag" — config value is used as fallback.
    dup_p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Simulate the full operation without modifying any files. "
            "Use --no-dry-run to override config default_dry_run=true."
        ),
    )
    dup_p.add_argument(
        "--json-report", type=Path, metavar="FILE",
        help="Write JSON duplicate report to FILE (atomic write).",
    )
    _add_workers(dup_p)
    _add_verbosity(dup_p)

    # ── organize ──────────────────────────────────────────────────────────────
    org_p = sub.add_parser(
        "organize",
        help="Sort files into category subdirectories.",
        description=(
            "Move files in PATH into subdirectories by file type.\n"
            "\n"
            "Categories (by extension):\n"
            "  Images     .jpg .jpeg .png .gif .webp .svg\n"
            "  Documents  .pdf .docx .txt .xlsx .csv .md\n"
            "  Videos     .mp4 .mkv .avi .mov .wmv\n"
            "  Audio      .mp3 .wav .flac .aac .ogg\n"
            "  Archives   .zip .tar .gz .rar .7z\n"
            "  Code       .py .js .ts .html .css .json .yaml\n"
            "  Others     everything else\n"
            "\n"
            "All moves are atomic. Batch failures trigger full rollback.\n"
            "\n"
            "Examples:\n"
            "  cleansweep organize ~/Downloads\n"
            "  cleansweep organize ~/Downloads --dry-run\n"
            "  cleansweep organize ~/Downloads --no-dry-run\n"
            "\n"
            "Exit codes:\n"
            "  0   Success\n"
            "  2   Invalid arguments\n"
            "  3   PATH does not exist or is not a directory\n"
            "  130 Interrupted (Ctrl+C)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    org_p.add_argument("path", type=Path, metavar="PATH", help="Directory to organize.")
    # FIX: BooleanOptionalAction enables --dry-run and --no-dry-run.
    # default=None means "user did not set this flag" — config value is used as fallback.
    org_p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Preview moves without modifying any files. "
            "Use --no-dry-run to override config default_dry_run=true."
        ),
    )
    org_p.add_argument(
        "--rules-file", type=Path, metavar="FILE", default=None,
        help=(
            "Path to a v2.0 JSON rule file defining custom organization categories. "
            "When omitted, the built-in default rules are used (Images, Documents, "
            "Videos, Audio, Archives, Code, Others). "
            "The file must contain a valid v2.0 rule schema."
        ),
    )
    org_p.add_argument(
        "--report-file", type=Path, metavar="FILE", default=None,
        help=(
            "Write the execution summary to FILE (UTF-8 text, overwrite allowed). "
            "When omitted, summary is printed to console only."
        ),
    )
    org_p.add_argument(
        "--policy", choices=["strict", "safe", "warn"], default=None,
        metavar="MODE",
        help=(
            "Rule conflict policy. "
            "'strict' aborts on any conflict. "
            "'safe' skips ambiguous files. "
            "'warn' uses the first (highest-priority) matching rule and logs a warning. "
            "Overrides config file policy_mode. Default: safe."
        ),
    )
    _add_verbosity(org_p)

    # ── report ────────────────────────────────────────────────────────────────
    rep_p = sub.add_parser(
        "report",
        help="Scan PATH and display a structured summary report.",
        description=(
            "Scan PATH and display a structured summary report including file count,\n"
            "total size, per-extension breakdown, largest files, and duplicate stats.\n"
            "This command is read-only — it never modifies the filesystem.\n"
            "\n"
            "Examples:\n"
            "  cleansweep report ~/Downloads\n"
            "  cleansweep report ~/Downloads --top 20\n"
            "  cleansweep report ~/Downloads --output report.txt\n"
            "\n"
            "Exit codes:\n"
            "  0   Success\n"
            "  2   Invalid arguments\n"
            "  3   PATH does not exist or is not a directory\n"
            "  130 Interrupted (Ctrl+C)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    rep_p.add_argument("path", type=Path, metavar="PATH", help="Directory to report on.")
    rep_p.add_argument(
        "--top", type=int, default=10, metavar="N",
        help="Number of largest files to list. Default: 10.",
    )
    rep_p.add_argument(
        "--output", type=Path, default=None, metavar="FILE",
        help="Write the report to FILE (UTF-8 text). Optional.",
    )
    rep_p.add_argument(
        "--unit", choices=["KB", "MB"], default="MB",
        help="Size display unit. Default: MB.",
    )
    _add_verbosity(rep_p)

    return root


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_config(config_flag: Path | None) -> AppConfig:
    """
    Load AppConfig from file or return defaults.

    Delegates all resolution, I/O, and validation to config.load_config().
    Maps ConfigError to _cli_error() (exit 2) so CLI error semantics are
    preserved: an invalid config always halts before the engine starts.

    Resolution order (owned by config.load_config):
      1. --config FILE        (explicit path from CLI flag)
      2. ./config.json        (implicit auto-load if present)
      3. DEFAULT_CONFIG_DATA  (built-in validated defaults)
    """
    try:
        return load_config(config_flag)
    except ConfigError as e:
        _cli_error(str(e))


# ---------------------------------------------------------------------------
# Shared validation
# ---------------------------------------------------------------------------

def _validate_workers(workers: int | None) -> None:
    """Validate --workers value. Exits INVALID_ARGS (2) on violation."""
    if workers is None:
        return
    if workers < 1:
        _cli_error(f"--workers must be >= 1, got {workers}")
    if workers > MAX_WORKERS_HARD_CAP:
        _cli_error(f"--workers must be <= {MAX_WORKERS_HARD_CAP} (cpu_count \u00d7 4), got {workers}")


def _resolve_log_level(args: argparse.Namespace, cfg: AppConfig) -> str:
    if getattr(args, "quiet", False):
        return "ERROR"
    if getattr(args, "verbose", False):
        return "INFO"
    if getattr(args, "debug", False):
        return "DEBUG"
    return cfg.log_level


# ---------------------------------------------------------------------------
# Subcommand handlers
#
# Rules (permanent):
#   - Return int exit code. Never call sys.exit() directly.
#   - Call _cli_error() for argument constraint violations (argument errors exit 2).
#   - Use logger for error messages. Do not print() errors directly.
# ---------------------------------------------------------------------------

def _cmd_scan(args: argparse.Namespace, cfg: AppConfig) -> int:
    try:
        folder = validate_folder(args.path)
    except FileNotFoundError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR
    except NotADirectoryError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR

    files = list_files(
        folder,
        ignore_extensions=list(cfg.ignore_extensions),
        ignore_folders=list(cfg.ignore_folders),
        min_file_size=cfg.min_file_size,
        follow_symlinks=cfg.follow_symlinks,
        max_depth=cfg.max_depth,
        exclude_patterns=list(cfg.exclude),
        symlink_policy=cfg.symlink_policy,
    )
    if args.summary_only:
        display_summary(files, args.unit)
    else:
        display_files(files, args.unit)
    return Exit.SUCCESS


def _cmd_duplicates(args: argparse.Namespace, cfg: AppConfig) -> int:
    # Argument constraint: --permanent requires --delete
    if args.permanent and not args.delete:
        _cli_error("--permanent requires --delete. "
                   "Use: --delete --permanent")

    workers = args.workers if args.workers is not None else cfg.workers
    _validate_workers(workers)

    # FIX: CLI flag takes precedence; fall back to config only when not set by user.
    dry_run     = args.dry_run if args.dry_run is not None else cfg.default_dry_run
    delete_mode = DELETE_MODE_PERMANENT if args.permanent else DELETE_MODE_TRASH

    try:
        folder = validate_folder(args.path)
    except FileNotFoundError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR
    except NotADirectoryError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR

    scan_policy = ScanPolicy(
        recursive         = cfg.recursive,
        max_depth         = cfg.max_depth,
        symlink_policy    = cfg.symlink_policy,
        exclude_patterns  = cfg.exclude,
        min_file_size     = cfg.min_file_size,
        ignore_extensions = cfg.ignore_extensions,
        ignore_folders    = cfg.ignore_folders,
    )
    result = scan_duplicates(
        folder,
        keep=args.keep,
        follow_symlinks=cfg.follow_symlinks,
        max_depth=cfg.max_depth,
        chunk_size=cfg.hash_chunk_size,
        max_workers=workers,
        policy=scan_policy,
    )
    display_scan_result(result)

    if getattr(args, "verbose", False) or getattr(args, "debug", False):
        display_timings(timer.get_timings())

    if args.json_report:
        saved = export_json(result["duplicates"], args.json_report)
        display_json_saved(saved)

    if result["duplicates"]:
        if args.delete or dry_run:
            controller = ActionController(
                dry_run=dry_run,
                scan_root=folder,
                delete_mode=delete_mode,
            )
            display_deletion_preview(controller.preview_deletion(result["duplicates"]))
            deletion = controller.execute_deletions(result["duplicates"])
            display_deletion_result(deletion)
            if controller.has_failures():
                return Exit.PARTIAL_FAILURE
        else:
            print(
                "\nDuplicates found. "
                "Run with --delete to move them to trash, "
                "or --dry-run to preview."
            )
    return Exit.SUCCESS


def _load_ruleset(rules_file: Path | None) -> "RuleSet | None":
    """
    Load and validate a RuleSet from a JSON file.

    Returns None if rules_file is None (caller uses DEFAULT_RULESET).
    Exits INVALID_ARGS (2) on JSON syntax error or schema violation.
    Exits FILESYSTEM_ERROR (3) if the file cannot be read.
    """
    if rules_file is None:
        return None
    try:
        raw = json.loads(rules_file.read_text())
    except FileNotFoundError:
        _cli_error(f"--rules-file: file not found: {rules_file}")
    except OSError as e:
        logger.log_error(f"--rules-file: cannot read {rules_file}: {e}")
        return None   # unreachable — _cli_error exits, but satisfies type checker
    except json.JSONDecodeError as e:
        _cli_error(f"--rules-file '{rules_file}': invalid JSON — {e}")
    try:
        return parse_rules(raw)
    except RuleError as e:
        _cli_error(f"--rules-file '{rules_file}': {e}")


def _cmd_organize(args: argparse.Namespace, cfg: AppConfig) -> int:
    """
    v2.4.0: Routes through BatchEngine directly.
    No direct organizer calls — planner + BatchEngine own the full lifecycle.
    """
    from batch_engine import BatchEngine
    from destination_map import DestinationMapError

    # FIX: CLI flag takes precedence; fall back to config only when not set by user.
    dry_run = args.dry_run if args.dry_run is not None else cfg.default_dry_run

    # Load custom ruleset if --rules-file supplied; None → DEFAULT_RULESET
    ruleset = _load_ruleset(getattr(args, "rules_file", None))
    rs      = ruleset if ruleset is not None else DEFAULT_RULESET

    # Resolve policy mode: CLI flag overrides config; config overrides default.
    policy_mode = getattr(args, "policy", None) or cfg.policy_mode

    try:
        folder = validate_folder(args.path)
    except FileNotFoundError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR
    except NotADirectoryError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR

    # File discovery (immediate children only — non-recursive, sorted)
    try:
        files = sorted(
            [f for f in folder.iterdir() if f.is_file()],
            key=lambda p: p.name,
        )
    except (PermissionError, FileNotFoundError, OSError) as exc:
        logger.log_error(f"Cannot read folder {folder}: {type(exc).__name__}: {exc}")
        return Exit.FILESYSTEM_ERROR

    if not files:
        display_batch_report(None, total=0, dry_run=dry_run)
        display_execution_summary(None, total_scanned=0, dry_run=dry_run)
        return Exit.SUCCESS

    # Run through planner + BatchEngine (no direct organizer call)
    engine = BatchEngine(scan_root=folder)
    try:
        report = engine.run_from_files(
            files       = files,
            ruleset     = rs,
            dest_map    = None,   # dest_map wiring via CLI reserved for future version
            dry_run     = dry_run,
            policy_mode = policy_mode,
        )
    except DestinationMapError as exc:
        logger.log_error(f"Destination map error: {exc}")
        return Exit.INVALID_ARGS

    display_batch_report(report, total=len(files), dry_run=dry_run)
    display_execution_summary(report, total_scanned=len(files), dry_run=dry_run)

    report_file = getattr(args, "report_file", None)
    if report_file is not None:
        try:
            save_report_file(report, total_scanned=len(files), dry_run=dry_run, path=report_file)
            display_json_saved(report_file)
        except OSError as exc:
            logger.log_error(f"--report-file: cannot write {report_file}: {exc}")

    if report.fail_index is not None:
        return Exit.PARTIAL_FAILURE
    return Exit.SUCCESS


def _cmd_report(args: argparse.Namespace, cfg: AppConfig) -> int:
    """
    Scan PATH and produce a structured summary report.

    Read-only — never modifies the filesystem. Combines scanner +
    analyzer to produce: file count, total size, per-extension
    breakdown, N largest files, and a duplicate-candidate estimate
    (files sharing size, before hashing).
    """
    try:
        folder = validate_folder(args.path)
    except FileNotFoundError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR
    except NotADirectoryError as e:
        logger.log_error(str(e))
        return Exit.FILESYSTEM_ERROR

    # Collect files via scanner
    files = list(list_files(
        folder,
        ignore_extensions=list(cfg.ignore_extensions),
        ignore_folders=list(cfg.ignore_folders),
        min_file_size=cfg.min_file_size,
        follow_symlinks=cfg.follow_symlinks,
        max_depth=cfg.max_depth,
        exclude_patterns=list(cfg.exclude),
        symlink_policy=cfg.symlink_policy,
    ))

    unit = getattr(args, "unit", "MB")
    top_n = getattr(args, "top", 10)

    from report import display_full_report
    display_full_report(
        folder=folder,
        files=files,
        unit=unit,
        top_n=top_n,
    )

    output_path = getattr(args, "output", None)
    if output_path is not None:
        from report import save_text_report
        try:
            save_text_report(
                folder=folder,
                files=files,
                unit=unit,
                top_n=top_n,
                path=output_path,
            )
            from report import display_json_saved
            display_json_saved(output_path)
        except OSError as exc:
            logger.log_error(f"--output: cannot write {output_path}: {exc}")

    return Exit.SUCCESS


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Parse arguments, load config, dispatch subcommand, exit with correct code.

    Exception mapping:
      KeyboardInterrupt → Exit.INTERRUPTED (130)
      Exception         → Exit.GENERAL_ERROR (1)

    Only main() and _cli_error() call sys.exit().
    Subcommand handlers return int codes.
    """
    parser = build_parser()
    # argparse itself exits 2 on unrecognised flags — standard POSIX behaviour.
    args = parser.parse_args()

    cfg = _load_config(args.config)
    logger.set_log_level(_resolve_log_level(args, cfg))

    try:
        if args.command == "scan":
            code = _cmd_scan(args, cfg)
        elif args.command == "duplicates":
            code = _cmd_duplicates(args, cfg)
        elif args.command == "organize":
            code = _cmd_organize(args, cfg)
        elif args.command == "report":
            code = _cmd_report(args, cfg)
        else:
            _cli_error(f"unknown command: {args.command!r}")

    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user (Ctrl+C). Shutting down cleanly.", file=sys.stderr)
        partial = timer.get_timings()
        if partial:
            print("[WARN] Partial timings at interrupt:", file=sys.stderr)
            for phase, duration in partial.items():
                print(f"[WARN]   {phase:<14}: {duration:.3f}s", file=sys.stderr)
        sys.exit(Exit.INTERRUPTED)

    except Exception as e:
        logger.log_error(f"Unexpected error: {type(e).__name__}: {e}")
        sys.exit(Exit.GENERAL_ERROR)

    if code != Exit.SUCCESS:
        sys.exit(code)


if __name__ == "__main__":
    main()
