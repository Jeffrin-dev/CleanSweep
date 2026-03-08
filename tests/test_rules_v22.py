"""
CleanSweep v2.2.0 — Rule Engine Tests.

Covers all v2.2 additions without regressing v2.0 behaviour:

  1.  Schema versioning — accepted versions, rejection of unknown versions
  2.  v2.0 backward compatibility — existing "2.0" rule files still parse correctly
  3.  v2.0 field isolation — new v2.2 fields rejected in "2.0" schema
  4.  Size rule validation — type checks, negative values, range coherence
  5.  Size rule evaluation — 0 bytes, exact boundary, just-over/under, huge file
  6.  Filename pattern validation — whitespace, empty, type errors
  7.  Filename pattern evaluation — glob semantics, case insensitivity
  8.  Priority parsing — type errors, explicit values
  9.  Priority evaluation — sort order, equal-priority config-order preservation
 10.  Combined rule evaluation — AND semantics (extension + size + pattern)
 11.  No OR leakage — partial match does not trigger rule
 12.  Determinism — 3 repeated identical invocations produce identical output
 13.  Extensions optional in v2.2 — size-only and pattern-only rules
 14.  Empty match object rejected
 15.  RuleSet repr updated for v2.2
 16.  resolve_destination backward compat — default file_size=0
 17.  DEFAULT_RULESET unchanged — v2.0 rules still parse under v2.2 parser
 18.  Stress — 100-rule mixed set with priorities
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from rules import (
    parse_rules,
    resolve_destination,
    RuleSet,
    Rule,
    RuleError,
    DEFAULT_RULESET,
    SCHEMA_VERSION,
    _ACCEPTED_VERSIONS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _v22_single(
    name:             str = "Rule1",
    destination:      str = "Dest",
    extensions:       list[str] | None = None,
    priority:         int | None = None,
    min_size:         int | None = None,
    max_size:         int | None = None,
    filename_pattern: str | None = None,
) -> dict:
    """Build a minimal valid v2.2 rules dict with one rule."""
    match: dict = {}
    if extensions is not None:
        match["extensions"] = extensions
    if min_size is not None:
        match["min_size"] = min_size
    if max_size is not None:
        match["max_size"] = max_size
    if filename_pattern is not None:
        match["filename_pattern"] = filename_pattern

    if not match:
        # At least one criterion — default to a simple extension rule
        match["extensions"] = [".txt"]

    rule: dict = {"name": name, "match": match, "destination": destination}
    if priority is not None:
        rule["priority"] = priority

    return {
        "version": "2.2",
        "rules": [rule],
        "default_destination": "Others",
    }


def _v20_minimal() -> dict:
    return {
        "version": "2.0",
        "rules": [
            {
                "name": "Images",
                "match": {"extensions": [".jpg", ".png"]},
                "destination": "Images",
            }
        ],
        "default_destination": "Others",
    }


def _v22_multi(*rules_kwargs) -> dict:
    """Build a v2.2 rules dict from multiple rule kwarg dicts."""
    rules = []
    for kw in rules_kwargs:
        match: dict = {}
        if "extensions" in kw:
            match["extensions"] = kw["extensions"]
        if "min_size" in kw:
            match["min_size"] = kw["min_size"]
        if "max_size" in kw:
            match["max_size"] = kw["max_size"]
        if "filename_pattern" in kw:
            match["filename_pattern"] = kw["filename_pattern"]
        if not match:
            match["extensions"] = [".bin"]
        rule: dict = {
            "name": kw["name"],
            "match": match,
            "destination": kw["destination"],
        }
        if "priority" in kw:
            rule["priority"] = kw["priority"]
        rules.append(rule)

    return {
        "version": "2.2",
        "rules": rules,
        "default_destination": "Others",
    }


# ===========================================================================
# 1. Schema versioning
# ===========================================================================

class TestSchemaVersioning(unittest.TestCase):

    def test_schema_version_is_22(self):
        self.assertEqual(SCHEMA_VERSION, "2.2")

    def test_accepted_versions_contain_20_and_22(self):
        self.assertIn("2.0", _ACCEPTED_VERSIONS)
        self.assertIn("2.2", _ACCEPTED_VERSIONS)

    def test_version_10_rejected(self):
        data = _v22_single()
        data["version"] = "1.0"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("1.0", str(ctx.exception))
        self.assertIn(SCHEMA_VERSION, str(ctx.exception))

    def test_version_30_rejected(self):
        data = _v22_single()
        data["version"] = "3.0"
        with self.assertRaises(RuleError):
            parse_rules(data)

    def test_version_21_rejected(self):
        data = _v22_single()
        data["version"] = "2.1"
        with self.assertRaises(RuleError):
            parse_rules(data)

    def test_version_integer_rejected(self):
        data = _v22_single()
        data["version"] = 2
        with self.assertRaises(RuleError):
            parse_rules(data)

    def test_v22_parses_successfully(self):
        rs = parse_rules(_v22_single())
        self.assertIsInstance(rs, RuleSet)
        self.assertEqual(rs.schema_version, "2.2")

    def test_v20_parses_successfully(self):
        rs = parse_rules(_v20_minimal())
        self.assertIsInstance(rs, RuleSet)
        self.assertEqual(rs.schema_version, "2.0")


# ===========================================================================
# 2. v2.0 backward compatibility
# ===========================================================================

class TestV20BackwardCompat(unittest.TestCase):

    def test_v20_extension_rule_resolves(self):
        rs = parse_rules(_v20_minimal())
        self.assertEqual(resolve_destination("photo.jpg", rs), "Images")
        self.assertEqual(resolve_destination("file.unknown", rs), "Others")

    def test_v20_extension_case_insensitive(self):
        rs = parse_rules(_v20_minimal())
        self.assertEqual(resolve_destination("PHOTO.JPG", rs), "Images")

    def test_v20_default_destination_used(self):
        rs = parse_rules(_v20_minimal())
        self.assertEqual(resolve_destination("Makefile", rs), "Others")

    def test_v20_rules_get_default_priority(self):
        rs = parse_rules(_v20_minimal())
        for rule in rs.rules:
            self.assertEqual(rule.priority, 0)

    def test_v20_rules_have_no_size_constraints(self):
        rs = parse_rules(_v20_minimal())
        for rule in rs.rules:
            self.assertIsNone(rule.min_size)
            self.assertIsNone(rule.max_size)
            self.assertIsNone(rule.filename_pattern)

    def test_v20_full_six_category_ruleset(self):
        """DEFAULT_RULESET (v2.0) must resolve all known extensions correctly."""
        cases = {
            "photo.jpg": "Images",
            "doc.pdf":   "Documents",
            "clip.mp4":  "Videos",
            "song.mp3":  "Audio",
            "file.zip":  "Archives",
            "main.py":   "Code",
            "unknown.x": "Others",
        }
        for fname, expected in cases.items():
            with self.subTest(fname=fname):
                self.assertEqual(resolve_destination(fname, DEFAULT_RULESET), expected)


# ===========================================================================
# 3. v2.0 field isolation — new fields forbidden in "2.0" schema
# ===========================================================================

class TestV20FieldIsolation(unittest.TestCase):

    def _inject_rule_field(self, key: str, value) -> dict:
        data = _v20_minimal()
        data["rules"][0][key] = value
        return data

    def _inject_match_field(self, key: str, value) -> dict:
        data = _v20_minimal()
        data["rules"][0]["match"][key] = value
        return data

    def test_priority_in_v20_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(self._inject_rule_field("priority", 1))
        self.assertIn("priority", str(ctx.exception))
        self.assertIn("2.2", str(ctx.exception))

    def test_min_size_in_v20_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(self._inject_match_field("min_size", 1000))
        self.assertIn("min_size", str(ctx.exception))

    def test_max_size_in_v20_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(self._inject_match_field("max_size", 1000))
        self.assertIn("max_size", str(ctx.exception))

    def test_filename_pattern_in_v20_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(self._inject_match_field("filename_pattern", "*.log"))
        self.assertIn("filename_pattern", str(ctx.exception))


# ===========================================================================
# 4. Size rule validation
# ===========================================================================

class TestSizeRuleValidation(unittest.TestCase):

    def test_min_size_zero_valid(self):
        rs = parse_rules(_v22_single(min_size=0))
        self.assertEqual(rs.rules[0].min_size, 0)

    def test_max_size_zero_valid(self):
        rs = parse_rules(_v22_single(max_size=0))
        self.assertEqual(rs.rules[0].max_size, 0)

    def test_large_size_valid(self):
        rs = parse_rules(_v22_single(min_size=10 ** 12))
        self.assertEqual(rs.rules[0].min_size, 10 ** 12)

    def test_min_size_negative_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(min_size=-1))
        self.assertIn("min_size", str(ctx.exception))

    def test_max_size_negative_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(max_size=-1))
        self.assertIn("max_size", str(ctx.exception))

    def test_max_size_less_than_min_size_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(min_size=1000, max_size=500))
        self.assertIn("max_size", str(ctx.exception))
        self.assertIn("min_size", str(ctx.exception))

    def test_min_size_equals_max_size_valid(self):
        # Exact size match — valid
        rs = parse_rules(_v22_single(min_size=1000, max_size=1000))
        self.assertEqual(rs.rules[0].min_size, 1000)
        self.assertEqual(rs.rules[0].max_size, 1000)

    def test_min_size_float_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(min_size=1000.5))  # type: ignore
        self.assertIn("min_size", str(ctx.exception))

    def test_max_size_string_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(max_size="1MB"))  # type: ignore
        self.assertIn("max_size", str(ctx.exception))

    def test_min_size_bool_raises(self):
        # bool is a subclass of int — must be explicitly rejected
        data = _v22_single()
        data["rules"][0]["match"]["min_size"] = True
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("min_size", str(ctx.exception))

    def test_priority_bool_raises(self):
        data = _v22_single()
        data["rules"][0]["priority"] = True
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("priority", str(ctx.exception))


# ===========================================================================
# 5. Size rule evaluation — boundary values
# ===========================================================================

class TestSizeRuleEvaluation(unittest.TestCase):

    def _make_size_rule(self, min_size=None, max_size=None) -> RuleSet:
        return parse_rules(_v22_single(
            name="SizeRule", destination="Sized",
            extensions=[".dat"],
            min_size=min_size, max_size=max_size,
        ))

    # ── min_size tests ────────────────────────────────────────────────────

    def test_min_size_zero_matches_empty_file(self):
        rs = self._make_size_rule(min_size=0)
        self.assertEqual(resolve_destination("file.dat", rs, 0), "Sized")

    def test_min_size_zero_matches_large_file(self):
        rs = self._make_size_rule(min_size=0)
        self.assertEqual(resolve_destination("file.dat", rs, 10 ** 9), "Sized")

    def test_min_size_1_rejects_empty_file(self):
        rs = self._make_size_rule(min_size=1)
        self.assertEqual(resolve_destination("file.dat", rs, 0), "Others")

    def test_min_size_exact_boundary_matches(self):
        rs = self._make_size_rule(min_size=1000)
        self.assertEqual(resolve_destination("file.dat", rs, 1000), "Sized")

    def test_min_size_one_below_boundary_rejects(self):
        rs = self._make_size_rule(min_size=1000)
        self.assertEqual(resolve_destination("file.dat", rs, 999), "Others")

    def test_min_size_one_above_boundary_matches(self):
        rs = self._make_size_rule(min_size=1000)
        self.assertEqual(resolve_destination("file.dat", rs, 1001), "Sized")

    def test_min_size_100mb(self):
        rs = self._make_size_rule(min_size=100_000_000)
        self.assertEqual(resolve_destination("f.dat", rs, 99_999_999), "Others")
        self.assertEqual(resolve_destination("f.dat", rs, 100_000_000), "Sized")
        self.assertEqual(resolve_destination("f.dat", rs, 100_000_001), "Sized")

    # ── max_size tests ────────────────────────────────────────────────────

    def test_max_size_zero_matches_empty_file_only(self):
        rs = self._make_size_rule(max_size=0)
        self.assertEqual(resolve_destination("file.dat", rs, 0), "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 1), "Others")

    def test_max_size_exact_boundary_matches(self):
        rs = self._make_size_rule(max_size=500)
        self.assertEqual(resolve_destination("file.dat", rs, 500), "Sized")

    def test_max_size_one_above_boundary_rejects(self):
        rs = self._make_size_rule(max_size=500)
        self.assertEqual(resolve_destination("file.dat", rs, 501), "Others")

    def test_max_size_huge_file(self):
        rs = self._make_size_rule(max_size=10 ** 12)
        self.assertEqual(resolve_destination("file.dat", rs, 10 ** 12), "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 10 ** 12 + 1), "Others")

    # ── range tests ───────────────────────────────────────────────────────

    def test_range_both_bounds_inclusive(self):
        rs = self._make_size_rule(min_size=100, max_size=200)
        self.assertEqual(resolve_destination("file.dat", rs, 99),  "Others")
        self.assertEqual(resolve_destination("file.dat", rs, 100), "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 150), "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 200), "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 201), "Others")

    def test_exact_size_rule(self):
        rs = self._make_size_rule(min_size=42, max_size=42)
        self.assertEqual(resolve_destination("file.dat", rs, 41),  "Others")
        self.assertEqual(resolve_destination("file.dat", rs, 42),  "Sized")
        self.assertEqual(resolve_destination("file.dat", rs, 43),  "Others")

    # ── size-only rule (no extension constraint) ──────────────────────────

    def test_size_only_rule_matches_any_extension(self):
        data = {
            "version": "2.2",
            "rules": [
                {"name": "SmallFiles", "match": {"max_size": 1024}, "destination": "Tiny"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("img.jpg",   rs, 500),  "Tiny")
        self.assertEqual(resolve_destination("doc.pdf",   rs, 1024), "Tiny")
        self.assertEqual(resolve_destination("video.mp4", rs, 1025), "Others")
        self.assertEqual(resolve_destination("Makefile",  rs, 0),    "Tiny")


# ===========================================================================
# 6. Filename pattern validation
# ===========================================================================

class TestPatternValidation(unittest.TestCase):

    def test_valid_glob_pattern(self):
        rs = parse_rules(_v22_single(filename_pattern="*.log"))
        self.assertEqual(rs.rules[0].filename_pattern, "*.log")

    def test_valid_prefix_glob(self):
        rs = parse_rules(_v22_single(filename_pattern="report_*"))
        self.assertIsNotNone(rs.rules[0].filename_pattern)

    def test_pattern_leading_whitespace_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(filename_pattern="  *.log"))
        self.assertIn("filename_pattern", str(ctx.exception))

    def test_pattern_trailing_whitespace_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(filename_pattern="*.log  "))
        self.assertIn("filename_pattern", str(ctx.exception))

    def test_pattern_empty_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(filename_pattern=""))
        self.assertIn("filename_pattern", str(ctx.exception))

    def test_pattern_whitespace_only_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(filename_pattern="   "))
        self.assertIn("filename_pattern", str(ctx.exception))

    def test_pattern_integer_raises(self):
        with self.assertRaises(RuleError) as ctx:
            parse_rules(_v22_single(filename_pattern=42))  # type: ignore
        self.assertIn("filename_pattern", str(ctx.exception))

    def test_pattern_none_not_in_match_object(self):
        """Absence of filename_pattern → rule.filename_pattern is None."""
        rs = parse_rules(_v22_single(extensions=[".txt"]))
        self.assertIsNone(rs.rules[0].filename_pattern)


# ===========================================================================
# 7. Filename pattern evaluation
# ===========================================================================

class TestPatternEvaluation(unittest.TestCase):

    def _make_pattern_rule(self, pattern: str, ext: str = ".zip") -> RuleSet:
        return parse_rules(_v22_single(
            name="PatternRule", destination="Matched",
            extensions=[ext] if ext else None,
            filename_pattern=pattern,
        ))

    def test_star_prefix_glob(self):
        rs = self._make_pattern_rule("*.log", ext=".log")
        self.assertEqual(resolve_destination("debug.log", rs), "Matched")
        self.assertEqual(resolve_destination("debug.txt", rs), "Others")

    def test_star_suffix_glob(self):
        rs = self._make_pattern_rule("report_*", ext=".pdf")
        self.assertEqual(resolve_destination("report_2024.pdf", rs), "Matched")
        self.assertEqual(resolve_destination("final_report.pdf", rs), "Others")

    def test_star_both_sides_glob(self):
        rs = parse_rules(_v22_single(
            name="BackupRule", destination="Backups",
            filename_pattern="*backup*",
            extensions=[".zip"],
        ))
        self.assertEqual(resolve_destination("mybackup.zip",         rs), "Backups")
        self.assertEqual(resolve_destination("backup_2024.zip",      rs), "Backups")
        self.assertEqual(resolve_destination("2024_backup_old.zip",  rs), "Backups")
        self.assertEqual(resolve_destination("archive.zip",          rs), "Others")

    def test_pattern_case_insensitive_lower_pattern(self):
        rs = self._make_pattern_rule("*.log", ext=".log")
        self.assertEqual(resolve_destination("DEBUG.LOG", rs), "Matched")
        self.assertEqual(resolve_destination("App.Log",   rs), "Matched")

    def test_pattern_case_insensitive_upper_pattern(self):
        rs = parse_rules(_v22_single(
            name="Logs", destination="Logs",
            extensions=[".log"],
            filename_pattern="*.LOG",
        ))
        self.assertEqual(resolve_destination("debug.log", rs), "Logs")
        self.assertEqual(resolve_destination("DEBUG.LOG", rs), "Logs")

    def test_pattern_exact_filename(self):
        rs = parse_rules(_v22_single(
            name="Exact", destination="Specific",
            filename_pattern="backup.zip",
            extensions=[".zip"],
        ))
        self.assertEqual(resolve_destination("backup.zip",       rs), "Specific")
        self.assertEqual(resolve_destination("mybackup.zip",     rs), "Others")
        self.assertEqual(resolve_destination("backup.zip.old",   rs), "Others")

    def test_pattern_only_no_extension(self):
        """Pattern-only rule (no extension constraint) matches any file with matching name."""
        data = {
            "version": "2.2",
            "rules": [
                {"name": "Logs", "match": {"filename_pattern": "*.log"}, "destination": "Logs"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("app.log",     rs), "Logs")
        self.assertEqual(resolve_destination("server.LOG",  rs), "Logs")
        self.assertEqual(resolve_destination("app.txt",     rs), "Others")
        self.assertEqual(resolve_destination("app",         rs), "Others")

    def test_pattern_no_match_goes_to_default(self):
        rs = self._make_pattern_rule("*backup*", ext=".zip")
        self.assertEqual(resolve_destination("archive.zip", rs), "Others")


# ===========================================================================
# 8. Priority parsing
# ===========================================================================

class TestPriorityParsing(unittest.TestCase):

    def test_explicit_priority_stored(self):
        rs = parse_rules(_v22_single(priority=5))
        self.assertEqual(rs.rules[0].priority, 5)

    def test_default_priority_is_zero(self):
        rs = parse_rules(_v22_single())
        self.assertEqual(rs.rules[0].priority, 0)

    def test_priority_zero_explicit(self):
        rs = parse_rules(_v22_single(priority=0))
        self.assertEqual(rs.rules[0].priority, 0)

    def test_priority_negative_allowed(self):
        """Negative priority values are valid — they sort before 0."""
        rs = parse_rules(_v22_single(priority=-10))
        self.assertEqual(rs.rules[0].priority, -10)

    def test_priority_large_positive_allowed(self):
        rs = parse_rules(_v22_single(priority=9999))
        self.assertEqual(rs.rules[0].priority, 9999)

    def test_priority_float_raises(self):
        data = _v22_single()
        data["rules"][0]["priority"] = 1.5
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("priority", str(ctx.exception))

    def test_priority_string_raises(self):
        data = _v22_single()
        data["rules"][0]["priority"] = "high"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("priority", str(ctx.exception))

    def test_priority_null_raises(self):
        data = _v22_single()
        data["rules"][0]["priority"] = None
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("priority", str(ctx.exception))


# ===========================================================================
# 9. Priority evaluation — sort order
# ===========================================================================

class TestPriorityEvaluation(unittest.TestCase):

    def test_lower_priority_number_wins(self):
        """
        Two rules match the same file.
        Rule with lower priority number (1) must win over rule with higher number (2).
        """
        data = _v22_multi(
            {"name": "LowPriority",  "destination": "LowDest",  "extensions": [".txt"], "priority": 2},
            {"name": "HighPriority", "destination": "HighDest", "extensions": [".txt"], "priority": 1},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("file.txt", rs), "HighDest")

    def test_higher_priority_number_loses(self):
        data = _v22_multi(
            {"name": "A", "destination": "DestA", "extensions": [".mp4"], "priority": 10},
            {"name": "B", "destination": "DestB", "extensions": [".mp4"], "priority":  3},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("video.mp4", rs), "DestB")

    def test_equal_priority_preserves_config_order(self):
        """
        Two rules with equal priority — first in config order wins.
        This is the deterministic tie-breaker: no ambiguity.
        """
        data = _v22_multi(
            {"name": "First",  "destination": "DestFirst",  "extensions": [".csv"], "priority": 5},
            {"name": "Second", "destination": "DestSecond", "extensions": [".csv"], "priority": 5},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("data.csv", rs), "DestFirst")

    def test_three_rules_sorted_correctly(self):
        data = _v22_multi(
            {"name": "P3", "destination": "Dest3", "extensions": [".bin"], "priority": 3},
            {"name": "P1", "destination": "Dest1", "extensions": [".bin"], "priority": 1},
            {"name": "P2", "destination": "Dest2", "extensions": [".bin"], "priority": 2},
        )
        rs = parse_rules(data)
        # P1 has lowest priority number → evaluated first → wins
        self.assertEqual(resolve_destination("file.bin", rs), "Dest1")

    def test_rules_in_ruleset_are_sorted_by_priority(self):
        """Verify the rules tuple itself is sorted, not just the resolution result."""
        data = _v22_multi(
            {"name": "P10", "destination": "D10", "extensions": [".x"], "priority": 10},
            {"name": "P2",  "destination": "D2",  "extensions": [".y"], "priority":  2},
            {"name": "P7",  "destination": "D7",  "extensions": [".z"], "priority":  7},
        )
        rs = parse_rules(data)
        priorities = [r.priority for r in rs.rules]
        self.assertEqual(priorities, sorted(priorities))

    def test_negative_priority_evaluated_first(self):
        data = _v22_multi(
            {"name": "Normal",   "destination": "NormalDest",   "extensions": [".log"], "priority":  0},
            {"name": "Elevated", "destination": "ElevatedDest", "extensions": [".log"], "priority": -1},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("app.log", rs), "ElevatedDest")

    def test_mixed_default_and_explicit_priority(self):
        """Rules without priority field default to 0; explicit priority overrides."""
        data = _v22_multi(
            {"name": "Default", "destination": "DefaultDest", "extensions": [".csv"]},          # priority 0
            {"name": "Earlier", "destination": "EarlierDest", "extensions": [".csv"], "priority": -5},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("data.csv", rs), "EarlierDest")

    def test_non_overlapping_rules_unaffected_by_priority(self):
        """Non-overlapping rules resolve independently regardless of priority."""
        data = _v22_multi(
            {"name": "Images", "destination": "Images", "extensions": [".jpg"], "priority": 99},
            {"name": "Docs",   "destination": "Docs",   "extensions": [".pdf"], "priority":  1},
        )
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("photo.jpg", rs), "Images")
        self.assertEqual(resolve_destination("paper.pdf", rs), "Docs")


# ===========================================================================
# 10. Combined rule evaluation — AND semantics
# ===========================================================================

class TestCombinedRuleEvaluation(unittest.TestCase):

    def _combined(self) -> RuleSet:
        """
        Rule: extension=.zip AND min_size=50MB AND pattern=*backup*.
        Only files satisfying ALL THREE go to "Backups".
        """
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "BackupZips",
                    "priority": 1,
                    "match": {
                        "extensions":       [".zip"],
                        "min_size":         52_428_800,   # 50 MB
                        "filename_pattern": "*backup*",
                    },
                    "destination": "Backups",
                }
            ],
            "default_destination": "Others",
        }
        return parse_rules(data)

    def test_all_three_criteria_match(self):
        rs = self._combined()
        self.assertEqual(
            resolve_destination("mybackup.zip", rs, 60_000_000), "Backups"
        )

    def test_wrong_extension_no_match(self):
        rs = self._combined()
        self.assertEqual(
            resolve_destination("mybackup.tar", rs, 60_000_000), "Others"
        )

    def test_too_small_no_match(self):
        rs = self._combined()
        # 50 MB - 1 byte
        self.assertEqual(
            resolve_destination("mybackup.zip", rs, 52_428_799), "Others"
        )

    def test_wrong_pattern_no_match(self):
        rs = self._combined()
        self.assertEqual(
            resolve_destination("archive.zip", rs, 60_000_000), "Others"
        )

    def test_two_of_three_criteria_no_match(self):
        rs = self._combined()
        # Correct extension and size, wrong pattern
        self.assertEqual(
            resolve_destination("archive.zip", rs, 60_000_000), "Others"
        )
        # Correct extension and pattern, too small
        self.assertEqual(
            resolve_destination("mybackup.zip", rs, 1000), "Others"
        )
        # Correct size and pattern, wrong extension
        self.assertEqual(
            resolve_destination("mybackup.tar", rs, 60_000_000), "Others"
        )

    def test_exact_size_boundary_passes(self):
        rs = self._combined()
        self.assertEqual(
            resolve_destination("mybackup.zip", rs, 52_428_800), "Backups"
        )

    def test_extension_and_size_only(self):
        """Two-criterion rule: extension AND min_size (no pattern)."""
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "LargeVideos",
                    "match": {"extensions": [".mp4"], "min_size": 100_000_000},
                    "destination": "LargeVideos",
                }
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("clip.mp4", rs, 200_000_000), "LargeVideos")
        self.assertEqual(resolve_destination("clip.mp4", rs,  99_999_999), "Others")
        self.assertEqual(resolve_destination("clip.avi", rs, 200_000_000), "Others")

    def test_extension_and_pattern_only(self):
        """Two-criterion rule: extension AND filename_pattern (no size)."""
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "Reports",
                    "match": {"extensions": [".pdf"], "filename_pattern": "report_*"},
                    "destination": "Reports",
                }
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("report_2024.pdf", rs), "Reports")
        self.assertEqual(resolve_destination("final.pdf",        rs), "Others")
        self.assertEqual(resolve_destination("report_2024.doc",  rs), "Others")


# ===========================================================================
# 11. No OR leakage
# ===========================================================================

class TestNoOrLeakage(unittest.TestCase):

    def test_partial_match_never_triggers(self):
        """
        If a rule has two criteria and only one is satisfied,
        the rule must not trigger. Ever.
        """
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "TwoConditions",
                    "match": {
                        "extensions":       [".log"],
                        "filename_pattern": "*error*",
                    },
                    "destination": "ErrorLogs",
                }
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)

        # Only extension matches — should NOT route to ErrorLogs
        self.assertEqual(resolve_destination("debug.log",      rs), "Others")
        # Only pattern matches (wrong extension) — should NOT route
        self.assertEqual(resolve_destination("error_trace.txt", rs), "Others")
        # Both match — should route
        self.assertEqual(resolve_destination("app_error.log",  rs), "ErrorLogs")

    def test_size_range_partial_no_trigger(self):
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "MediumFiles",
                    "match": {"min_size": 1000, "max_size": 5000},
                    "destination": "Medium",
                }
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("file.bin", rs,   999), "Others")  # too small
        self.assertEqual(resolve_destination("file.bin", rs,  1000), "Medium")  # boundary
        self.assertEqual(resolve_destination("file.bin", rs,  5000), "Medium")  # boundary
        self.assertEqual(resolve_destination("file.bin", rs,  5001), "Others")  # too large


# ===========================================================================
# 12. Determinism — 3 repeated identical invocations
# ===========================================================================

class TestDeterminism(unittest.TestCase):

    def _complex_ruleset_data(self) -> dict:
        return {
            "version": "2.2",
            "rules": [
                {
                    "name": "BackupZips",
                    "priority": 1,
                    "match": {
                        "extensions":       [".zip"],
                        "min_size":         1_000_000,
                        "filename_pattern": "*backup*",
                    },
                    "destination": "Backups",
                },
                {
                    "name": "AllZips",
                    "priority": 5,
                    "match": {"extensions": [".zip"]},
                    "destination": "Archives",
                },
                {
                    "name": "LargeFiles",
                    "priority": 10,
                    "match": {"min_size": 500_000_000},
                    "destination": "LargeFiles",
                },
                {
                    "name": "Logs",
                    "priority": 2,
                    "match": {"filename_pattern": "*.log"},
                    "destination": "Logs",
                },
            ],
            "default_destination": "Others",
        }

    def test_parse_three_times_identical(self):
        data = self._complex_ruleset_data()
        rs1 = parse_rules(data)
        rs2 = parse_rules(data)
        rs3 = parse_rules(data)
        self.assertEqual(rs1.rules, rs2.rules)
        self.assertEqual(rs2.rules, rs3.rules)
        self.assertEqual(rs1.default_destination, rs3.default_destination)

    def test_resolve_three_times_identical(self):
        rs = parse_rules(self._complex_ruleset_data())
        test_cases = [
            ("mybackup.zip",  2_000_000),
            ("archive.zip",   500),
            ("video.mp4",     600_000_000),
            ("server.log",    1000),
            ("README.md",     200),
        ]
        results_a = [resolve_destination(f, rs, s) for f, s in test_cases]
        results_b = [resolve_destination(f, rs, s) for f, s in test_cases]
        results_c = [resolve_destination(f, rs, s) for f, s in test_cases]
        self.assertEqual(results_a, results_b)
        self.assertEqual(results_b, results_c)

    def test_sorted_rule_order_stable(self):
        """Parsing same data multiple times must produce identical rule sort order."""
        data = self._complex_ruleset_data()
        orders = [
            [r.name for r in parse_rules(data).rules]
            for _ in range(3)
        ]
        self.assertEqual(orders[0], orders[1])
        self.assertEqual(orders[1], orders[2])


# ===========================================================================
# 13. Extensions optional in v2.2
# ===========================================================================

class TestOptionalExtensions(unittest.TestCase):

    def test_size_only_rule_valid(self):
        data = {
            "version": "2.2",
            "rules": [{"name": "Huge", "match": {"min_size": 1_000_000_000}, "destination": "Huge"}],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertIsNone(rs.rules[0].extensions)

    def test_pattern_only_rule_valid(self):
        data = {
            "version": "2.2",
            "rules": [{"name": "Logs", "match": {"filename_pattern": "*.log"}, "destination": "Logs"}],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertIsNone(rs.rules[0].extensions)

    def test_size_range_only_rule_valid(self):
        data = {
            "version": "2.2",
            "rules": [{"name": "Med", "match": {"min_size": 100, "max_size": 1000}, "destination": "Med"}],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertIsNone(rs.rules[0].extensions)

    def test_empty_match_object_raises(self):
        data = {
            "version": "2.2",
            "rules": [{"name": "Empty", "match": {}, "destination": "Dest"}],
            "default_destination": "Others",
        }
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("match", str(ctx.exception))

    def test_v20_still_requires_extensions(self):
        data = {
            "version": "2.0",
            "rules": [{"name": "NoExt", "match": {}, "destination": "Dest"}],
            "default_destination": "Others",
        }
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extensions", str(ctx.exception))


# ===========================================================================
# 14. RuleSet repr updated for v2.2
# ===========================================================================

class TestRuleSetRepr(unittest.TestCase):

    def test_repr_contains_ruleset(self):
        rs = parse_rules(_v22_single())
        self.assertIn("RuleSet", repr(rs))

    def test_repr_contains_rule_count(self):
        rs = parse_rules(_v22_single())
        self.assertIn("rules=1", repr(rs))

    def test_repr_contains_default_destination(self):
        rs = parse_rules(_v22_single())
        self.assertIn("Others", repr(rs))

    def test_repr_contains_schema_version(self):
        rs = parse_rules(_v22_single())
        self.assertIn("2.2", repr(rs))

    def test_v20_repr_contains_20(self):
        rs = parse_rules(_v20_minimal())
        self.assertIn("2.0", repr(rs))


# ===========================================================================
# 15. resolve_destination backward compatibility
# ===========================================================================

class TestResolveBackwardCompat(unittest.TestCase):

    def test_no_file_size_arg_works(self):
        """resolve_destination(filename, ruleset) — no size — must not error."""
        rs = parse_rules(_v22_single(extensions=[".txt"]))
        result = resolve_destination("file.txt", rs)
        self.assertEqual(result, "Dest")

    def test_no_file_size_size_rule_gets_zero(self):
        """
        Caller omits file_size → defaults to 0.
        Rule with min_size=1 will not match, file goes to default.
        This is the safe conservative behaviour.
        """
        rs = parse_rules(_v22_single(min_size=1, max_size=10_000))
        result = resolve_destination("file.txt", rs)
        # file_size=0 < min_size=1 → rule does not match → Others
        self.assertEqual(result, "Others")

    def test_extension_only_rule_unaffected_by_size(self):
        """Pure extension rules work identically whether or not file_size is supplied."""
        rs = parse_rules(_v22_single(extensions=[".jpg"]))
        self.assertEqual(resolve_destination("photo.jpg", rs),         "Dest")
        self.assertEqual(resolve_destination("photo.jpg", rs, 0),      "Dest")
        self.assertEqual(resolve_destination("photo.jpg", rs, 999999), "Dest")


# ===========================================================================
# 16. DEFAULT_RULESET — regression
# ===========================================================================

class TestDefaultRulesetRegression(unittest.TestCase):

    def test_default_ruleset_is_ruleset(self):
        self.assertIsInstance(DEFAULT_RULESET, RuleSet)

    def test_default_ruleset_schema_version(self):
        self.assertEqual(DEFAULT_RULESET.schema_version, "2.0")

    def test_default_ruleset_six_rules(self):
        self.assertEqual(len(DEFAULT_RULESET.rules), 6)

    def test_default_images(self):
        for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Images")

    def test_default_documents(self):
        for ext in [".pdf", ".docx", ".txt", ".xlsx", ".csv", ".md"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Documents")

    def test_default_videos(self):
        for ext in [".mp4", ".mkv", ".avi", ".mov", ".wmv"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Videos")

    def test_default_audio(self):
        for ext in [".mp3", ".wav", ".flac", ".aac", ".ogg"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Audio")

    def test_default_archives(self):
        for ext in [".zip", ".tar", ".gz", ".rar", ".7z"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Archives")

    def test_default_code(self):
        for ext in [".py", ".js", ".ts", ".html", ".css", ".json", ".yaml"]:
            with self.subTest(ext=ext):
                self.assertEqual(resolve_destination(f"file{ext}", DEFAULT_RULESET), "Code")

    def test_default_others(self):
        for name in ["Makefile", "file.xyz", ".bashrc", ""]:
            with self.subTest(name=name):
                self.assertEqual(resolve_destination(name, DEFAULT_RULESET), "Others")

    def test_default_ruleset_immutable(self):
        with self.assertRaises((AttributeError, TypeError)):
            DEFAULT_RULESET.rules = ()  # type: ignore

    def test_default_rule_frozen(self):
        with self.assertRaises((AttributeError, TypeError)):
            DEFAULT_RULESET.rules[0].name = "Hacked"  # type: ignore


# ===========================================================================
# 17. Stress — 100-rule mixed set with priorities
# ===========================================================================

class TestStressV22(unittest.TestCase):

    def test_100_rules_no_overlap(self):
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "priority": i,
                "match": {"extensions": [f".ext{i:03d}"]},
                "destination": f"Dir{i:03d}",
            })
        data = {"version": "2.2", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        self.assertEqual(len(rs.rules), 100)

    def test_100_rules_all_resolve_correctly(self):
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "priority": 100 - i,   # reversed priority — still must resolve correctly
                "match": {"extensions": [f".ext{i:03d}"]},
                "destination": f"Dir{i:03d}",
            })
        data = {"version": "2.2", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        for i in range(100):
            result = resolve_destination(f"file.ext{i:03d}", rs)
            self.assertEqual(result, f"Dir{i:03d}", f"Rule{i:03d} failed")

    def test_100_rules_priority_sorted_in_ruleset(self):
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "priority": 99 - i,   # inserted in reverse priority order
                "match": {"extensions": [f".x{i:03d}"]},
                "destination": f"D{i:03d}",
            })
        data = {"version": "2.2", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        priorities = [r.priority for r in rs.rules]
        self.assertEqual(priorities, sorted(priorities),
                         "Rules in RuleSet must be sorted by priority (ascending)")

    def test_100_mixed_rules_determinism(self):
        """Parsing same 100-rule set 3 times must produce identical sorted order."""
        rules = []
        for i in range(50):
            rules.append({
                "name": f"Ext{i:03d}",
                "priority": i % 10,
                "match": {"extensions": [f".e{i:03d}"]},
                "destination": f"E{i:03d}",
            })
        for i in range(50):
            rules.append({
                "name": f"Size{i:03d}",
                "priority": i % 7,
                "match": {"min_size": i * 1000, "max_size": (i + 1) * 1000},
                "destination": f"S{i:03d}",
            })
        data = {"version": "2.2", "rules": rules, "default_destination": "Others"}

        order_a = [r.name for r in parse_rules(data).rules]
        order_b = [r.name for r in parse_rules(data).rules]
        order_c = [r.name for r in parse_rules(data).rules]
        self.assertEqual(order_a, order_b)
        self.assertEqual(order_b, order_c)

    def test_priority_collision_large_set(self):
        """
        All 100 rules share priority 0 — config order must be the deterministic tiebreaker.
        """
        rules = [
            {
                "name": f"R{i:03d}",
                "priority": 0,
                "match": {"extensions": [f".q{i:03d}"]},
                "destination": f"D{i:03d}",
            }
            for i in range(100)
        ]
        data = {"version": "2.2", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        names_in_order = [r.name for r in rs.rules]
        # With equal priorities, config insertion order is preserved
        self.assertEqual(names_in_order, [f"R{i:03d}" for i in range(100)])


if __name__ == "__main__":
    unittest.main()


# ===========================================================================
# 19. Large-scale file volume stress test (10,000+ files)
# ===========================================================================

class TestLargeFileVolume(unittest.TestCase):

    def _build_ruleset(self):
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "LargeVideos",
                    "priority": 1,
                    "match": {"extensions": [".mp4", ".mkv"], "min_size": 100_000_000},
                    "destination": "LargeVideos",
                },
                {
                    "name": "BackupZips",
                    "priority": 2,
                    "match": {"extensions": [".zip"], "filename_pattern": "*backup*"},
                    "destination": "Backups",
                },
                {
                    "name": "SmallFiles",
                    "priority": 3,
                    "match": {"max_size": 1024},
                    "destination": "Tiny",
                },
                {
                    "name": "Images",
                    "priority": 5,
                    "match": {"extensions": [".jpg", ".jpeg", ".png", ".gif", ".webp"]},
                    "destination": "Images",
                },
                {
                    "name": "Docs",
                    "priority": 5,
                    "match": {"extensions": [".pdf", ".docx", ".txt", ".md"]},
                    "destination": "Documents",
                },
                {
                    "name": "AllVideos",
                    "priority": 10,
                    "match": {"extensions": [".mp4", ".mkv", ".avi", ".mov"]},
                    "destination": "Videos",
                },
                {
                    "name": "Archives",
                    "priority": 10,
                    "match": {"extensions": [".zip", ".tar", ".gz", ".rar"]},
                    "destination": "Archives",
                },
                {
                    "name": "Logs",
                    "priority": 15,
                    "match": {"filename_pattern": "*.log"},
                    "destination": "Logs",
                },
            ],
            "default_destination": "Others",
        }
        return parse_rules(data)

    def _build_file_list(self):
        """Build 10,000 (filename, size) pairs. Deterministic — no random."""
        files = []
        exts  = [".jpg", ".png", ".mp4", ".mkv", ".avi", ".zip", ".tar",
                 ".pdf", ".docx", ".txt", ".log", ".bin", ".unknown"]
        names = ["report", "photo", "video", "backup", "archive",
                 "document", "image", "clip", "data", "file"]
        sizes = [0, 512, 1024, 1025, 50_000, 1_000_000,
                 100_000_000, 200_000_000, 500_000_000]
        for i in range(10_000):
            ext  = exts[i % len(exts)]
            name = names[(i // len(exts)) % len(names)]
            size = sizes[i % len(sizes)]
            if i % 17 == 0:
                filename = f"backup_{name}_{i:05d}{ext}"
            else:
                filename = f"{name}_{i:05d}{ext}"
            files.append((filename, size))
        return files

    def test_10k_files_correct_routing(self):
        rs    = self._build_ruleset()
        files = self._build_file_list()
        self.assertEqual(len(files), 10_000)
        known_dests = {
            "LargeVideos", "Backups", "Tiny", "Images", "Documents",
            "Videos", "Archives", "Logs", "Others",
        }
        for filename, size in files:
            result = resolve_destination(filename, rs, size)
            self.assertIn(result, known_dests,
                f"Unknown destination {result!r} for {filename!r} size={size}")

    def test_10k_files_determinism_three_passes(self):
        rs    = self._build_ruleset()
        files = self._build_file_list()
        results_a = [resolve_destination(fn, rs, sz) for fn, sz in files]
        results_b = [resolve_destination(fn, rs, sz) for fn, sz in files]
        results_c = [resolve_destination(fn, rs, sz) for fn, sz in files]
        self.assertEqual(results_a, results_b)
        self.assertEqual(results_b, results_c)

    def test_10k_large_video_routing(self):
        """All .mp4 >= 100MB → LargeVideos (priority 1 beats AllVideos at priority 10)."""
        rs = self._build_ruleset()
        for i in range(1000):
            fn = f"clip_{i:05d}.mp4"
            sz = 100_000_000 + i * 1000
            self.assertEqual(resolve_destination(fn, rs, sz), "LargeVideos",
                f"{fn} size={sz} should be LargeVideos")

    def test_10k_small_file_routing(self):
        """All files <= 1024 bytes → Tiny regardless of extension."""
        rs = self._build_ruleset()
        for i in range(1000):
            fn   = f"file_{i:05d}.ext{i % 50:03d}"
            size = i % 1025
            self.assertEqual(resolve_destination(fn, rs, size), "Tiny",
                f"{fn} size={size} should be Tiny")

    def test_10k_backup_zip_routing(self):
        """.zip + 'backup' in name → Backups; plain .zip → Archives."""
        rs   = self._build_ruleset()
        size = 5_000_000  # above 1024 so Tiny rule doesn't fire
        for i in range(500):
            self.assertEqual(
                resolve_destination(f"my_backup_{i:04d}.zip", rs, size), "Backups")
            self.assertEqual(
                resolve_destination(f"archive_{i:04d}.zip", rs, size), "Archives")

    def test_200_rule_parse_stable(self):
        """Parsing the same 200-rule set 3 times produces identical sorted order."""
        data = {
            "version": "2.2",
            "rules": [
                {"name": f"R{i:04d}", "priority": i % 5,
                 "match": {"extensions": [f".x{i:04d}"]},
                 "destination": f"D{i:04d}"}
                for i in range(200)
            ],
            "default_destination": "Others",
        }
        order_a = [r.name for r in parse_rules(data).rules]
        order_b = [r.name for r in parse_rules(data).rules]
        order_c = [r.name for r in parse_rules(data).rules]
        self.assertEqual(order_a, order_b)
        self.assertEqual(order_b, order_c)
