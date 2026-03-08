from pathlib import Path
from analyzer import summarize

DIVISORS = {"KB": 1024, "MB": 1024 ** 2}


def format_size(size_bytes: int, unit: str) -> str:
    return f"{size_bytes / DIVISORS[unit]:.2f} {unit}"


# ── file listing ──────────────────────────────────────────────────────────────

def display_summary(files: list[Path], unit: str) -> None:
    stats = summarize(files)
    print(f"Files : {stats['count']}")
    print(f"Total : {format_size(stats['total_bytes'], unit)}")


def display_files(files: list[Path], unit: str) -> None:
    if not files:
        print("No files found.")
        return

    # Explicit sort — never rely on caller's ordering
    files = sorted(files, key=lambda f: str(f))

    sizes = {}
    for f in files:
        try:
            sizes[f] = f.stat().st_size
        except OSError:
            sizes[f] = 0  # file vanished after scan — display 0, don't crash
    max_name = max(len(f.name) for f in files)
    col_width = max(len(format_size(s, unit)) for s in sizes.values())
    sep = "-" * (max_name + 2 + col_width)

    print(f"\n{'Filename':<{max_name}}  Size ({unit})")
    print(sep)
    for f in files:
        print(f"{f.name:<{max_name}}  {format_size(sizes[f], unit)}")

    # Reuse the already-fetched sizes dict — no second round of stat() calls.
    total_bytes = sum(sizes.values())
    total_label = f"Total: {len(files)} files"
    print(sep)
    print(f"{total_label:<{max_name}}  {format_size(total_bytes, unit)}")


# ── organizer ─────────────────────────────────────────────────────────────────

def display_organize_result(result: dict) -> None:
    label = "[DRY RUN] " if result["dry_run"] else ""

    # Explicit sort by file name — never rely on iterdir() order
    sorted_results = sorted(result["results"], key=lambda r: r.get("file", ""))

    for r in sorted_results:
        if r["status"] == "moved":
            print(f"  Moved   : {r['file']}  →  {r['destination']}/")
        elif r["status"] == "dry_run":
            print(f"  {label}{r['file']}  →  {r['destination']}/")
        elif r["status"] == "failed":
            print(f"  Failed  : {r['file']}  ({r.get('error', 'unknown')})")
        else:
            print(f"  Skipped : {r['file']}  (already exists)")

    moved   = sum(1 for r in result["results"] if r["status"] in ("moved", "dry_run"))
    skipped = sum(1 for r in result["results"] if r["status"] == "skipped")
    print(f"\n{label}Files scanned : {result['total']}")
    print(f"{label}Files moved   : {moved}")
    print(f"{label}Files skipped : {skipped}")


# ── duplicates ────────────────────────────────────────────────────────────────

def display_scan_result(result: dict) -> None:
    print(f"Scanning : {result['folder']}")
    print(f"Strategy : keep {result['keep_strategy']}\n")
    print(f"Files scanned     : {result['total_scanned']}")
    print(f"Skipped (size)    : {result['skipped_by_size']}")
    print(f"Skipped (partial) : {result['skipped_by_partial']}")
    print(f"Full-hashed       : {result['total_hashed']}")
    if result["skipped_unreadable"]:
        print(f"Unreadable        : {len(result['skipped_unreadable'])}")
    workers = result.get("workers", "?")
    print(f"Workers           : {workers}")
    print(f"Scan duration     : {result['scan_duration_seconds']:.3f}s\n")

    duplicates = result["duplicates"]
    if not duplicates:
        print("No duplicate files found.")
        return

    # duplicates dict is already in canonical order (size, hash, path)
    for i, (file_hash, files) in enumerate(duplicates.items(), 1):
        size = files[0].size              # from FileEntry snapshot — no stat()
        print(f"\nGroup {i}  [{file_hash[:8]}...]  {len(files)} copies  ({size / 1024:.2f} KB each)")
        for j, file in enumerate(files):
            tag = "[keep]" if j == 0 else "[dupe]"
            print(f"    {tag}  {file.path.as_posix()}")

    print(f"\nDuplicate files : {result['total_duplicate_files']}")
    print(f"Wasted space    : {result['wasted_bytes'] / 1024:.2f} KB")


def display_deletion_preview(preview: dict) -> None:
    """
    Display the pre-deletion summary produced by ActionController.preview_deletion().

    Called by CLI layer (main.py) before any file is touched.
    Receives a pure data dict — zero coupling to ActionController internals.
    """
    dry_run     = preview["dry_run"]
    delete_mode = preview["delete_mode"]
    platform    = preview["platform"]
    total_files = preview["total_files"]
    total_bytes = preview["total_bytes"]
    scan_root   = preview["scan_root"]

    if dry_run:
        mode_str = "DRY RUN — no files will be modified"
    elif delete_mode == "permanent":
        mode_str = "EXECUTION — PERMANENT DELETE (irreversible)"
    else:
        mode_str = f"EXECUTION — Move to trash ({platform})"

    print(f"\nMode            : {mode_str}")
    print(f"Files to delete : {total_files}")
    print(f"Total size      : {total_bytes / 1024:.2f} KB  ({total_bytes:,} bytes)")
    print(f"Scan root       : {scan_root}")
    print()


def display_deletion_result(result: dict) -> None:
    label      = "[DRY RUN] " if result["dry_run"] else ""
    delete_mode = result.get("delete_mode", "trash")
    action_word = "Trashed" if delete_mode == "trash" else "Deleted"

    for path in result["deleted"]:
        print(f"  {label}{action_word} : {path}")
    for entry in result["failed"]:
        hint = f" — {entry['hint']}" if "hint" in entry else ""
        print(f"  Failed  : {entry['path']}  ({entry['error']}){hint}")

    n_deleted = len(result["deleted"])
    n_failed  = len(result["failed"])
    n_skipped = len(result.get("skipped", []))
    freed     = result.get("freed_bytes", 0)

    print(f"\n{label}Files {action_word.lower()} : {n_deleted}")
    if n_failed:
        print(f"Failures        : {n_failed}")
    if n_skipped:
        print(f"Skipped         : {n_skipped}")
    if freed:
        print(f"Freed space     : {freed / 1024:.2f} KB  ({freed:,} bytes)")


def display_json_saved(path: Path) -> None:
    print(f"\nReport saved: {path}")



# ── batch engine (v2.4.0) ─────────────────────────────────────────────────────

def display_batch_report(report: object, total: int, dry_run: bool) -> None:
    """
    Display BatchReport from BatchEngine.

    Called by main._cmd_organize() (v2.4.0+).
    report=None signals an empty folder (total=0, nothing to display except summary).

    All formatting lives here — BatchEngine returns structured data only.
    """
    label = "[DRY RUN] " if dry_run else ""

    if report is None or total == 0:
        print(f"\n{label}Files scanned : 0")
        print(f"{label}Files moved   : 0")
        return

    for r in sorted(report.results, key=lambda x: x.get("file", "")):
        status = r.get("status", "")
        if status == "moved":
            print(f"  Moved   : {r['file']}  →  {r['destination']}/")
        elif status == "dry_run":
            print(f"  {label}{r['file']}  →  {r['destination']}/")
        elif status == "failed":
            print(f"  Failed  : {r['file']}  ({r.get('error', 'unknown')})")
        elif status == "skipped":
            print(f"  Skipped : {r['file']}  (already exists)")
        elif status == "validation_failed":
            print(f"  Invalid : {r['file']}  ({r.get('error', 'validation error')})")

    moved   = sum(1 for r in report.results if r.get("status") in ("moved", "dry_run"))
    failed  = sum(1 for r in report.results if r.get("status") == "failed")
    skipped = report.skipped

    print(f"\n{label}Files scanned : {total}")
    print(f"{label}Files moved   : {moved}")
    if skipped:
        print(f"{label}Files skipped : {skipped}")
    if failed:
        print(f"{label}Files failed  : {failed}")
    if hasattr(report, "duration_seconds"):
        print(f"{label}Duration      : {report.duration_seconds:.3f}s")
    if report.fail_index is not None:
        print(f"\n[WARN] Batch aborted at action {report.fail_index} "
              f"(phase: {report.phase_reached})")


def display_timings(timings: dict) -> None:
    """
    Display phase timing summary.
    Order follows timer.PHASE_ORDER — never dict insertion order.
    Shown only in --verbose or --debug mode.
    Never called in --quiet mode.
    Never written to JSON export.
    """
    if not timings:
        return
    print("\nExecution Summary:")
    for phase, duration in timings.items():
        print(f"  {phase:<14}: {duration:.3f}s")


# ── execution summary (v2.5.0) ────────────────────────────────────────────────

def _format_bytes(n: int) -> str:
    """
    Deterministic bytes → human-readable string.

    Conversion: bytes → KB → MB → GB (1024-based).
    Rounded to 2 decimal places to avoid floating-point noise.
    """
    if n >= 1024 ** 3:
        return f"{round(n / 1024 ** 3, 2)} GB"
    if n >= 1024 ** 2:
        return f"{round(n / 1024 ** 2, 2)} MB"
    if n >= 1024:
        return f"{round(n / 1024, 2)} KB"
    return f"{n} B"


def _format_execution_summary(report: object, total_scanned: int, dry_run: bool) -> str:
    """
    Produce the canonical execution summary string.

    Format matches the v2.5.0 roadmap spec exactly:
      - 28-dash separator
      - Label field width: 28 characters (left-aligned), followed by one space, then value
      - Blank line between header and metric block
      - Blank line between count metrics and size/time metrics
      - Execution time displayed as X.XX seconds (2 decimal places)
      - Values left-aligned after the label column (no right-padding)

    Deterministic: identical inputs always produce identical output.
    Called by both display_execution_summary (console) and save_report_file (file)
    so that file and console output are always byte-identical.

    Args:
      report:        BatchReport from BatchEngine.run() (or None for empty runs).
      total_scanned: total files discovered before planning (len(files) in main).
      dry_run:       True if this was a dry-run.

    Returns:
      Multi-line UTF-8 string; trailing newline included.
    """
    # "Destination folders created:" is exactly 28 chars — the natural width of the
    # widest label.  All labels are padded to this width so values align on one column.
    W   = 28                       # label column width
    SEP = "-" * W                  # "----------------------------"
    dr  = " [DRY RUN]" if dry_run else ""

    def row(lbl: str, val: str) -> str:
        return f"{lbl:<{W}} {val}"

    if report is None or total_scanned == 0:
        lines = [
            f"CleanSweep Execution Summary{dr}",
            SEP,
            "",
            row("Files scanned:",              "0"),
            row("Files matched by rules:",      "0"),
            row("Actions planned:",             "0"),
            row("Actions executed:",            "0"),
            row("Actions skipped:",             "0"),
            row("Failures:",                    "0"),
            "",
            row("Total data reorganized:",      "0 B"),
            row("Destination folders created:", "0"),
            row("Execution time:",              "0.00 seconds"),
        ]
        return "\n".join(lines)

    planned  = report.total_planned
    executed = report.total_executed
    skipped  = report.skipped
    failures = planned - executed - skipped

    lines = [
        f"CleanSweep Execution Summary{dr}",
        SEP,
        "",
        row("Files scanned:",              f"{total_scanned:,}"),
        row("Files matched by rules:",      f"{planned:,}"),
        row("Actions planned:",             f"{planned:,}"),
        row("Actions executed:",            f"{executed:,}"),
        row("Actions skipped:",             f"{skipped:,}"),
        row("Failures:",                    f"{failures:,}"),
        "",
        row("Total data reorganized:",      _format_bytes(report.total_bytes_moved)),
        row("Destination folders created:", f"{report.destinations_created:,}"),
        row("Execution time:",              f"{report.duration_seconds:.2f} seconds"),
    ]

    # ── Policy section (v2.8.0) ───────────────────────────────────────────────
    pm = getattr(report, "policy_metrics", None)
    if pm is not None and (
        pm.conflicts_detected > 0
        or pm.files_skipped > 0
        or pm.overrides_applied > 0
    ):
        lines += [
            "",
            SEP,
            f"Policy Mode: {pm.mode.upper()}",
            row("Conflicts detected:",           f"{pm.conflicts_detected:,}"),
            row("Files skipped (policy):",        f"{pm.files_skipped:,}"),
            row("Rule overrides applied:",        f"{pm.overrides_applied:,}"),
        ]
        if pm.conflict_details:
            lines.append("")
            lines.append("Conflict details:")
            for c in pm.conflict_details:
                rules_str = ", ".join(c.matching_rules)
                lines.append(f"  {c.filename}  →  [{rules_str}]")

    return "\n".join(lines)


def display_execution_summary(report: object, total_scanned: int, dry_run: bool) -> None:
    """
    Print the structured execution summary to stdout.

    Content is always identical to what save_report_file() writes.
    Called by main._cmd_organize() after display_batch_report().
    report=None is valid (empty folder — all metrics are zero).
    """
    print(_format_execution_summary(report, total_scanned, dry_run))


def save_report_file(report: object, total_scanned: int, dry_run: bool, path: "Path") -> None:
    """
    Write the execution summary to a UTF-8 text file.

    Content is byte-identical to what display_execution_summary() prints.
    Overwrites any existing file at path.
    OSError propagates to caller (main.py surfaces it as a non-fatal warning).

    Args:
      report:        BatchReport (or None for empty runs).
      total_scanned: total files discovered before planning.
      dry_run:       True if this was a dry-run.
      path:          destination file path (UTF-8, overwrite allowed).
    """
    content = _format_execution_summary(report, total_scanned, dry_run)
    path.write_text(content, encoding="utf-8")  # I/O permitted: report.py §20 (Reporting Invariant)


# ---------------------------------------------------------------------------
# Report subcommand output (display_full_report, save_text_report)
# ---------------------------------------------------------------------------

def _format_full_report(
    folder: "Path",
    files: list,
    unit: str = "MB",
    top_n: int = 10,
) -> str:
    """
    Pure formatter for the `report` subcommand.

    Produces a deterministic, human-readable text report covering:
      - Total file count and aggregate size
      - Per-extension breakdown (sorted by count DESC, then extension ASC)
      - Top-N largest files
      - Duplicate-candidate count (files sharing size — before hashing)

    This is a pure function: identical inputs always produce identical output.
    No filesystem access, no print(), no side effects.

    Args:
      folder: the scanned directory (used for display only).
      files:  list of Path objects returned by scanner.list_files().
      unit:   "KB" or "MB" — display unit for sizes.
      top_n:  number of largest files to list.
    """
    from collections import defaultdict, Counter

    divisor = 1024 if unit == "KB" else 1024 * 1024

    def _fmt_size(n_bytes: int) -> str:
        if n_bytes == 0:
            return f"0 {unit}"
        val = n_bytes / divisor
        return f"{val:.2f} {unit}"

    # Build size map via stat() — one call per file, no caching needed here
    sizes: dict[Path, int] = {}
    for f in files:
        try:
            sizes[f] = f.stat().st_size
        except OSError:
            sizes[f] = 0

    total_bytes = sum(sizes.values())
    total_files = len(files)

    # Per-extension breakdown
    ext_counts: dict[str, int] = defaultdict(int)
    ext_bytes: dict[str, int] = defaultdict(int)
    for f in files:
        ext = f.suffix.lower() if f.suffix else "(no extension)"
        ext_counts[ext] += 1
        ext_bytes[ext] += sizes[f]

    # Sort: count DESC, then extension ASC (deterministic)
    ext_rows = sorted(ext_counts.items(), key=lambda kv: (-kv[1], kv[0]))

    # Top-N largest files (sorted by size DESC, path ASC as tiebreaker)
    top_files = sorted(files, key=lambda f: (-sizes[f], str(f)))[:top_n]

    # Duplicate candidates: files sharing the same byte size
    size_buckets: dict[int, int] = defaultdict(int)
    for sz in sizes.values():
        if sz > 0:
            size_buckets[sz] += 1
    dup_candidates = sum(count for count in size_buckets.values() if count > 1)

    lines: list[str] = []
    lines.append("=" * 60)
    lines.append(f"CleanSweep Report — {folder}")
    lines.append("=" * 60)
    lines.append(f"Total files : {total_files:,}")
    lines.append(f"Total size  : {_fmt_size(total_bytes)}")
    lines.append(f"Dup candidates (same size, unverified): {dup_candidates:,}")
    lines.append("")

    lines.append(f"{'Extension':<22} {'Count':>8}  {'Size':>12}")
    lines.append("-" * 46)
    for ext, count in ext_rows:
        lines.append(f"{ext:<22} {count:>8,}  {_fmt_size(ext_bytes[ext]):>12}")

    lines.append("")
    lines.append(f"Top {min(top_n, total_files)} largest files:")
    lines.append("-" * 60)
    for i, f in enumerate(top_files, 1):
        rel = f.relative_to(folder) if f.is_relative_to(folder) else f
        lines.append(f"  {i:>3}. {_fmt_size(sizes[f]):>10}  {rel}")

    lines.append("=" * 60)
    return "\n".join(lines)


def display_full_report(
    folder: "Path",
    files: list,
    unit: str = "MB",
    top_n: int = 10,
) -> None:
    """Print the full structured report for the `report` subcommand."""
    print(_format_full_report(folder, files, unit, top_n))


def save_text_report(
    folder: "Path",
    files: list,
    unit: str = "MB",
    top_n: int = 10,
    path: "Path" = None,
) -> None:
    """
    Write the full structured report to a UTF-8 text file.  # I/O permitted: report.py §20

    Content is byte-identical to what display_full_report() prints.
    Overwrites any existing file at path. OSError propagates to caller.
    """
    content = _format_full_report(folder, files, unit, top_n)
    path.write_text(content, encoding="utf-8")
