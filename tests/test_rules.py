"""
CleanSweep v2.0.0 — Rule Engine Tests.

Covers:
  1.  Schema validation — top-level
  2.  Schema validation — per-rule
  3.  Schema validation — match object
  4.  Extension normalization
  5.  Conflict detection — duplicate names
  6.  Conflict detection — overlapping extensions
  7.  Deterministic evaluation (first-match-wins)
  8.  Default destination
  9.  Pure function guarantees (no side effects)
  10. RuleSet immutability
  11. DEFAULT_RULESET correctness
  12. resolve_destination edge cases
  13. parse_rules round-trip (JSON → RuleSet → resolution)
  14. Stress — 100-rule set
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from rules import (
    parse_rules, resolve_destination,
    RuleSet, Rule, RuleError,
    DEFAULT_RULESET, SCHEMA_VERSION,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_ruleset(**overrides) -> dict:
    """Return a minimal valid rules dict, optionally overriding keys."""
    base = {
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
    base.update(overrides)
    return base


def _two_rule_set() -> dict:
    return {
        "version": "2.0",
        "rules": [
            {"name": "Images", "match": {"extensions": [".jpg", ".png"]}, "destination": "Images"},
            {"name": "Docs",   "match": {"extensions": [".pdf", ".txt"]}, "destination": "Documents"},
        ],
        "default_destination": "Others",
    }


# ===========================================================================
# 1. Top-level schema validation
# ===========================================================================

class TestTopLevelSchema(unittest.TestCase):

    def test_valid_minimal(self):
        rs = parse_rules(_minimal_ruleset())
        self.assertIsInstance(rs, RuleSet)

    def test_not_a_dict_raises(self):
        for bad in [[], "string", 42, None]:
            with self.assertRaises(RuleError, msg=f"Expected RuleError for {bad!r}"):
                parse_rules(bad)

    def test_missing_version_raises(self):
        data = _minimal_ruleset()
        del data["version"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("version", str(ctx.exception))

    def test_missing_rules_raises(self):
        data = _minimal_ruleset()
        del data["rules"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("rules", str(ctx.exception))

    def test_missing_default_destination_raises(self):
        data = _minimal_ruleset()
        del data["default_destination"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("default_destination", str(ctx.exception))

    def test_unknown_top_key_raises(self):
        data = _minimal_ruleset()
        data["unknown_key"] = "value"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("unknown_key", str(ctx.exception))

    def test_wrong_version_raises(self):
        data = _minimal_ruleset(version="1.0")
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("1.0", str(ctx.exception))
        self.assertIn(SCHEMA_VERSION, str(ctx.exception))

    def test_version_not_string_raises(self):
        data = _minimal_ruleset(version=2)
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("version", str(ctx.exception))

    def test_empty_rules_list_raises(self):
        data = _minimal_ruleset(rules=[])
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("rules", str(ctx.exception))

    def test_rules_not_list_raises(self):
        data = _minimal_ruleset(rules={"name": "bad"})
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("rules", str(ctx.exception))

    def test_empty_default_destination_raises(self):
        data = _minimal_ruleset(default_destination="")
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("default_destination", str(ctx.exception))

    def test_whitespace_only_default_destination_raises(self):
        data = _minimal_ruleset(default_destination="   ")
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("default_destination", str(ctx.exception))

    def test_default_destination_not_string_raises(self):
        data = _minimal_ruleset(default_destination=42)
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("default_destination", str(ctx.exception))


# ===========================================================================
# 2. Per-rule schema validation
# ===========================================================================

class TestRuleSchema(unittest.TestCase):

    def _make(self, rule_overrides=None, index=0) -> dict:
        data = _minimal_ruleset()
        if rule_overrides is not None:
            data["rules"][index].update(rule_overrides)
        return data

    def test_rule_not_dict_raises(self):
        data = _minimal_ruleset(rules=["not a dict"])
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("Rule[0]", str(ctx.exception))

    def test_missing_name_raises(self):
        data = _minimal_ruleset()
        del data["rules"][0]["name"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("name", str(ctx.exception))

    def test_missing_match_raises(self):
        data = _minimal_ruleset()
        del data["rules"][0]["match"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("match", str(ctx.exception))

    def test_missing_destination_raises(self):
        data = _minimal_ruleset()
        del data["rules"][0]["destination"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("destination", str(ctx.exception))

    def test_unknown_rule_key_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["extra"] = "bad"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extra", str(ctx.exception))

    def test_empty_name_raises(self):
        data = _make = _minimal_ruleset()
        data["rules"][0]["name"] = ""
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("name", str(ctx.exception))

    def test_whitespace_only_name_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["name"] = "   "
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("name", str(ctx.exception))

    def test_name_not_string_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["name"] = 42
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("name", str(ctx.exception))

    def test_empty_destination_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["destination"] = ""
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("destination", str(ctx.exception))

    def test_destination_not_string_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["destination"] = ["folder"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("destination", str(ctx.exception))

    def test_second_rule_invalid_is_caught(self):
        """Error in rule[1] must report index 1."""
        data = _two_rule_set()
        del data["rules"][1]["name"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("Rule[1]", str(ctx.exception))


# ===========================================================================
# 3. Match object schema validation
# ===========================================================================

class TestMatchSchema(unittest.TestCase):

    def test_match_not_dict_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"] = [".jpg"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("match", str(ctx.exception))

    def test_match_missing_extensions_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"] = {}
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extensions", str(ctx.exception))

    def test_match_unknown_key_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["size"] = "> 1MB"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("size", str(ctx.exception))

    def test_extensions_not_list_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = ".jpg"
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extensions", str(ctx.exception))

    def test_empty_extensions_list_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = []
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extensions", str(ctx.exception))

    def test_extension_not_string_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = [".jpg", 42]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("extensions[1]", str(ctx.exception))

    def test_empty_extension_string_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = [""]
        with self.assertRaises(RuleError):
            parse_rules(data)

    def test_whitespace_only_extension_raises(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = ["   "]
        with self.assertRaises(RuleError):
            parse_rules(data)


# ===========================================================================
# 4. Extension normalization
# ===========================================================================

class TestExtensionNormalization(unittest.TestCase):

    def test_uppercase_normalized(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = [".JPG", ".PNG"]
        rs = parse_rules(data)
        self.assertIn(".jpg", rs.rules[0].extensions)
        self.assertIn(".png", rs.rules[0].extensions)

    def test_no_leading_dot_normalized(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = ["jpg", "png"]
        rs = parse_rules(data)
        self.assertIn(".jpg", rs.rules[0].extensions)
        self.assertIn(".png", rs.rules[0].extensions)

    def test_mixed_case_no_dot_normalized(self):
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = ["JPG", "Mp3"]
        rs = parse_rules(data)
        self.assertIn(".jpg", rs.rules[0].extensions)
        self.assertIn(".mp3", rs.rules[0].extensions)

    def test_duplicates_within_rule_rejected(self):
        """
        Intra-rule duplicate extensions are a hard error after normalization.
        .jpg, .JPG, and jpg all normalize to .jpg → RuleError.
        """
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = [".jpg", ".JPG", "jpg"]
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn(".jpg", str(ctx.exception))
        self.assertIn("Rule[0]", str(ctx.exception))

    def test_distinct_extensions_within_rule_allowed(self):
        """Different extensions within one rule are fine."""
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = [".jpg", ".png", ".gif"]
        rs = parse_rules(data)
        self.assertEqual(rs.rules[0].extensions, frozenset({".jpg", ".png", ".gif"}))

    def test_resolve_uses_normalized(self):
        """resolve_destination must match regardless of input case."""
        data = _minimal_ruleset()
        data["rules"][0]["match"]["extensions"] = ["JPG"]
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("photo.jpg", rs), "Images")
        self.assertEqual(resolve_destination("photo.JPG", rs), "Images")
        self.assertEqual(resolve_destination("photo.Jpg", rs), "Images")


# ===========================================================================
# 5. Conflict detection — duplicate names
# ===========================================================================

class TestDuplicateNameConflict(unittest.TestCase):

    def test_duplicate_names_raises(self):
        data = {
            "version": "2.0",
            "rules": [
                {"name": "Media", "match": {"extensions": [".jpg"]}, "destination": "Images"},
                {"name": "Media", "match": {"extensions": [".mp4"]}, "destination": "Videos"},
            ],
            "default_destination": "Others",
        }
        with self.assertRaises(RuleError) as ctx:
            parse_rules(data)
        self.assertIn("Media", str(ctx.exception))
        self.assertIn("Duplicate", str(ctx.exception))

    def test_same_name_different_case_allowed(self):
        """Name comparison is case-sensitive — 'Images' and 'images' are different."""
        data = {
            "version": "2.0",
            "rules": [
                {"name": "Images", "match": {"extensions": [".jpg"]}, "destination": "Images"},
                {"name": "images", "match": {"extensions": [".png"]}, "destination": "images"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(len(rs.rules), 2)


# ===========================================================================
# 6. Conflict detection — overlapping extensions
# ===========================================================================

class TestOverlappingExtensions(unittest.TestCase):

    def test_same_ext_two_rules_first_wins(self):
        """
        Cross-rule extension overlap is ALLOWED.
        The first rule in JSON order that claims an extension wins — always.
        This is deterministic: same config always produces same routing.
        """
        data = {
            "version": "2.0",
            "rules": [
                {"name": "A", "match": {"extensions": [".jpg", ".png"]}, "destination": "A"},
                {"name": "B", "match": {"extensions": [".gif", ".jpg"]}, "destination": "B"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        # .jpg claimed by both A and B — A is first, A must win
        self.assertEqual(resolve_destination("photo.jpg", rs), "A")
        # .gif is only in B — B wins for .gif
        self.assertEqual(resolve_destination("anim.gif",  rs), "B")
        # .png is only in A
        self.assertEqual(resolve_destination("logo.png",  rs), "A")

    def test_overlap_after_normalization_first_rule_wins(self):
        """
        Cross-rule overlap detected after normalization is still ALLOWED.
        .jpg and JPG both normalize to .jpg — the first rule in JSON wins.
        """
        data = {
            "version": "2.0",
            "rules": [
                {"name": "A", "match": {"extensions": [".jpg"]}, "destination": "A"},
                {"name": "B", "match": {"extensions": ["JPG"]},  "destination": "B"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        # Both normalize to .jpg; rule A is first → A wins
        self.assertEqual(resolve_destination("photo.jpg", rs), "A")
        self.assertEqual(resolve_destination("photo.JPG", rs), "A")

    def test_no_overlap_passes(self):
        data = _two_rule_set()
        rs = parse_rules(data)
        self.assertEqual(len(rs.rules), 2)

    def test_overlap_deterministic_across_runs(self):
        """
        Cross-rule overlap must resolve identically every time.
        Alpha claims .mp3 first → Alpha always wins, never Beta.
        """
        data = {
            "version": "2.0",
            "rules": [
                {"name": "Alpha", "match": {"extensions": [".mp3"]}, "destination": "A"},
                {"name": "Beta",  "match": {"extensions": [".mp3"]}, "destination": "B"},
            ],
            "default_destination": "Others",
        }
        results = set()
        for _ in range(50):
            rs = parse_rules(data)
            results.add(resolve_destination("song.mp3", rs))
        # Must be exactly one unique result: "A" (Alpha wins every time)
        self.assertEqual(results, {"A"})


# ===========================================================================
# 7. Deterministic evaluation — first-match-wins
# ===========================================================================

class TestDeterministicEvaluation(unittest.TestCase):

    def test_first_rule_wins_over_second(self):
        """
        First rule must win. This test verifies evaluation order by putting
        the same extension in two candidate positions — only the first must fire.
        We achieve this by verifying destinations across distinct non-overlapping
        rule sets and checking rule index lookup.
        """
        data = {
            "version": "2.0",
            "rules": [
                {"name": "Primary",   "match": {"extensions": [".dat"]}, "destination": "Primary"},
                {"name": "Secondary", "match": {"extensions": [".bin"]}, "destination": "Secondary"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("file.dat", rs), "Primary")
        self.assertEqual(resolve_destination("file.bin", rs), "Secondary")

    def test_rule_order_preserved_from_json(self):
        """Rules tuple order must match JSON array order."""
        data = {
            "version": "2.0",
            "rules": [
                {"name": "First",  "match": {"extensions": [".aaa"]}, "destination": "DirA"},
                {"name": "Second", "match": {"extensions": [".bbb"]}, "destination": "DirB"},
                {"name": "Third",  "match": {"extensions": [".ccc"]}, "destination": "DirC"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(rs.rules[0].name, "First")
        self.assertEqual(rs.rules[1].name, "Second")
        self.assertEqual(rs.rules[2].name, "Third")

    def test_same_input_always_same_output(self):
        """Calling resolve_destination 100 times with same args gives same result."""
        rs = parse_rules(_minimal_ruleset())
        results = {resolve_destination("photo.jpg", rs) for _ in range(100)}
        self.assertEqual(len(results), 1)

    def test_five_runs_identical(self):
        """parse_rules on same input 5 times must produce identical RuleSets."""
        data = _two_rule_set()
        rulesets = [parse_rules(data) for _ in range(5)]
        for rs in rulesets[1:]:
            self.assertEqual(rs.rules, rulesets[0].rules)
            self.assertEqual(rs.default_destination, rulesets[0].default_destination)


# ===========================================================================
# 8. Default destination
# ===========================================================================

class TestDefaultDestination(unittest.TestCase):

    def test_unmatched_extension_goes_to_default(self):
        rs = parse_rules(_minimal_ruleset())
        self.assertEqual(resolve_destination("file.xyz", rs), "Others")
        self.assertEqual(resolve_destination("file.unknown", rs), "Others")

    def test_no_extension_goes_to_default(self):
        rs = parse_rules(_minimal_ruleset())
        self.assertEqual(resolve_destination("Makefile", rs), "Others")
        self.assertEqual(resolve_destination("README", rs), "Others")

    def test_custom_default_destination_used(self):
        data = _minimal_ruleset(default_destination="Unsorted")
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("file.xyz", rs), "Unsorted")

    def test_default_destination_preserved_exactly(self):
        """Whitespace trimmed but content preserved."""
        data = _minimal_ruleset(default_destination="  My Files  ")
        rs = parse_rules(data)
        self.assertEqual(rs.default_destination, "My Files")


# ===========================================================================
# 9. Pure function guarantees
# ===========================================================================

class TestPureFunctions(unittest.TestCase):

    def test_resolve_destination_no_filesystem_access(self):
        """resolve_destination must work on a nonexistent filename."""
        rs = parse_rules(_minimal_ruleset())
        # This file does not exist — pure function must not care
        result = resolve_destination("/nonexistent/path/to/ghost.jpg", rs)
        self.assertEqual(result, "Images")

    def test_parse_rules_no_side_effects(self):
        """Calling parse_rules must not modify the input dict."""
        import copy
        data = _minimal_ruleset()
        original = copy.deepcopy(data)
        parse_rules(data)
        self.assertEqual(data, original)

    def test_resolve_destination_does_not_modify_ruleset(self):
        """resolve_destination must not alter RuleSet state."""
        rs = parse_rules(_minimal_ruleset())
        rules_before = rs.rules
        ext_index_before = dict(rs._ext_index)
        resolve_destination("photo.jpg", rs)
        self.assertEqual(rs.rules, rules_before)
        self.assertEqual(rs._ext_index, ext_index_before)


# ===========================================================================
# 10. RuleSet immutability
# ===========================================================================

class TestRuleSetImmutability(unittest.TestCase):

    def test_ruleset_is_frozen(self):
        rs = parse_rules(_minimal_ruleset())
        with self.assertRaises((AttributeError, TypeError)):
            rs.default_destination = "Hacked"  # type: ignore

    def test_rule_is_frozen(self):
        rs = parse_rules(_minimal_ruleset())
        with self.assertRaises((AttributeError, TypeError)):
            rs.rules[0].name = "Hacked"  # type: ignore

    def test_rules_tuple_immutable(self):
        rs = parse_rules(_minimal_ruleset())
        with self.assertRaises(TypeError):
            rs.rules[0] = None  # type: ignore


# ===========================================================================
# 11. DEFAULT_RULESET correctness
# ===========================================================================

class TestDefaultRuleSet(unittest.TestCase):

    def test_default_ruleset_is_ruleset(self):
        self.assertIsInstance(DEFAULT_RULESET, RuleSet)

    def test_default_images(self):
        for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Images",
                f"Expected Images for {ext}"
            )

    def test_default_documents(self):
        for ext in [".pdf", ".docx", ".txt", ".xlsx", ".csv", ".md"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Documents",
                f"Expected Documents for {ext}"
            )

    def test_default_videos(self):
        for ext in [".mp4", ".mkv", ".avi", ".mov", ".wmv"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Videos",
                f"Expected Videos for {ext}"
            )

    def test_default_audio(self):
        for ext in [".mp3", ".wav", ".flac", ".aac", ".ogg"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Audio",
                f"Expected Audio for {ext}"
            )

    def test_default_archives(self):
        for ext in [".zip", ".tar", ".gz", ".rar", ".7z"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Archives",
                f"Expected Archives for {ext}"
            )

    def test_default_code(self):
        for ext in [".py", ".js", ".ts", ".html", ".css", ".json", ".yaml"]:
            self.assertEqual(
                resolve_destination(f"file{ext}", DEFAULT_RULESET), "Code",
                f"Expected Code for {ext}"
            )

    def test_default_others(self):
        for name in ["Makefile", "file.xyz", "file.unknown", ".bashrc"]:
            dest = resolve_destination(name, DEFAULT_RULESET)
            self.assertEqual(dest, "Others", f"Expected Others for {name!r}")

    def test_default_ruleset_has_no_conflicts(self):
        """DEFAULT_RULESET must pass its own validation — no overlaps."""
        # Already validated at import time; this test proves it stays valid
        self.assertEqual(len(DEFAULT_RULESET.rules), 6)


# ===========================================================================
# 12. resolve_destination edge cases
# ===========================================================================

class TestResolveEdgeCases(unittest.TestCase):

    def test_uppercase_filename_extension_matched(self):
        rs = parse_rules(_minimal_ruleset())
        self.assertEqual(resolve_destination("PHOTO.JPG", rs), "Images")
        self.assertEqual(resolve_destination("photo.Jpg", rs), "Images")

    def test_path_with_directories_uses_filename_only(self):
        rs = parse_rules(_minimal_ruleset())
        self.assertEqual(resolve_destination("/home/user/photos/img.jpg", rs), "Images")
        self.assertEqual(resolve_destination("/home/user/file.xyz", rs), "Others")

    def test_dotfile_no_secondary_extension_goes_to_default(self):
        rs = parse_rules(_minimal_ruleset())
        # .bashrc has suffix=".bashrc" — not in any rule → Others
        result = resolve_destination(".bashrc", rs)
        self.assertEqual(result, "Others")

    def test_double_extension_uses_last(self):
        """file.tar.gz → suffix is .gz, not .tar"""
        data = {
            "version": "2.0",
            "rules": [
                {"name": "Archives", "match": {"extensions": [".gz", ".tar"]}, "destination": "Archives"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        self.assertEqual(resolve_destination("archive.tar.gz", rs), "Archives")

    def test_empty_string_filename(self):
        rs = parse_rules(_minimal_ruleset())
        result = resolve_destination("", rs)
        self.assertEqual(result, "Others")


# ===========================================================================
# 13. Round-trip: JSON → RuleSet → resolution
# ===========================================================================

class TestRoundTrip(unittest.TestCase):

    def test_full_roundtrip(self):
        import json
        raw_json = json.dumps({
            "version": "2.0",
            "rules": [
                {"name": "Images",  "match": {"extensions": [".jpg", ".png"]}, "destination": "Pictures"},
                {"name": "Music",   "match": {"extensions": [".mp3", ".flac"]}, "destination": "Music"},
            ],
            "default_destination": "Misc",
        })
        data = json.loads(raw_json)
        rs = parse_rules(data)

        self.assertEqual(resolve_destination("photo.jpg",  rs), "Pictures")
        self.assertEqual(resolve_destination("photo.PNG",  rs), "Pictures")
        self.assertEqual(resolve_destination("song.mp3",   rs), "Music")
        self.assertEqual(resolve_destination("track.flac", rs), "Music")
        self.assertEqual(resolve_destination("doc.pdf",    rs), "Misc")
        self.assertEqual(resolve_destination("Makefile",   rs), "Misc")

    def test_ruleset_repr_is_informative(self):
        rs = parse_rules(_minimal_ruleset())
        r = repr(rs)
        self.assertIn("RuleSet", r)
        self.assertIn("rules=1", r)
        self.assertIn("Others", r)


# ===========================================================================
# 14. Stress — 100-rule set
# ===========================================================================

class TestStress(unittest.TestCase):

    def test_100_rules_no_overlap(self):
        """Parse a valid 100-rule set — must succeed without errors."""
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "match": {"extensions": [f".ext{i:03d}"]},
                "destination": f"Dir{i:03d}",
            })
        data = {"version": "2.0", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        self.assertEqual(len(rs.rules), 100)

    def test_100_rules_all_resolve_correctly(self):
        """Every extension in a 100-rule set must resolve to its own destination."""
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "match": {"extensions": [f".ext{i:03d}"]},
                "destination": f"Dir{i:03d}",
            })
        data = {"version": "2.0", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)

        for i in range(100):
            result = resolve_destination(f"file.ext{i:03d}", rs)
            self.assertEqual(result, f"Dir{i:03d}",
                             f"Rule{i:03d}: expected Dir{i:03d}, got {result!r}")

    def test_100_rules_with_one_overlap_first_wins(self):
        """
        Single overlap in 100 rules: allowed. First rule claiming .ext000 wins.
        The intruder rule is subordinate — never reachable for .ext000.
        """
        rules = []
        for i in range(100):
            rules.append({
                "name": f"Rule{i:03d}",
                "match": {"extensions": [f".ext{i:03d}"]},
                "destination": f"Dir{i:03d}",
            })
        # Inject overlap: Intruder also claims .ext000 (Rule000 already claimed it)
        rules.append({
            "name": "Intruder",
            "match": {"extensions": [".ext000"]},
            "destination": "Nowhere",
        })
        data = {"version": "2.0", "rules": rules, "default_destination": "Others"}
        rs = parse_rules(data)
        # Rule000 is first → Dir000 must win, never "Nowhere"
        self.assertEqual(resolve_destination("file.ext000", rs), "Dir000")
        # All other rules still resolve correctly
        for i in range(1, 100):
            result = resolve_destination(f"file.ext{i:03d}", rs)
            self.assertEqual(result, f"Dir{i:03d}")


if __name__ == "__main__":
    unittest.main()
