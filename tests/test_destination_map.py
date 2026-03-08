"""
CleanSweep v2.3.0 — Destination Mapping Layer Tests.

Covers every requirement in the v2.3.0 completion definition:

  1.  Schema validation — version, required keys, unknown keys, type errors
  2.  Destination key validation — empty, path separators, null bytes
  3.  Template value validation — unknown variables, empty, null bytes
  4.  Conflict policy validation — valid values, type error
  5.  Template fallback validation — empty, type error
  6.  Base dir resolution — explicit, default_base_dir, cwd fallback
  7.  Template expansion — {extension}, {year}, {month}, {size_bucket}
  8.  Template fallback for missing variables — no extension, no mtime_ns
  9.  Size bucket thresholds — exact boundary values
 10.  resolve_destination_path — unknown key error, relative vs absolute
 11.  validate_ruleset_destinations — missing keys rejected early
 12.  Conflict policy: rename — FileOperationManager handles collisions
 13.  Conflict policy: skip   — 10 collision files, all correctly skipped
 14.  Conflict policy: error  — organize aborts on first collision
 15.  Nested auto-create — deep directory trees created idempotently
 16.  Organizer integration — organize() with dest_map, without dest_map (v2.2 compat)
 17.  Determinism — 3 identical runs produce identical output tree
 18.  Large directory stress — 200-file mixed-extension stress test
 19.  No regression — v2.2 organize() behavior unchanged when dest_map=None
 20.  dest_map_active flag in organize() return value
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from destination_map import (
    DestinationMap,
    DestinationMapError,
    SCHEMA_VERSION,
    _SIZE_SMALL_CEILING,
    _SIZE_MEDIUM_CEILING,
    _resolve_size_bucket,
    _resolve_template,
    parse_destination_map,
    resolve_destination_path,
    validate_ruleset_destinations,
)
from organizer import organize
from rules import parse_rules, DEFAULT_RULESET


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_map_data(**overrides) -> dict:
    """Return a minimal valid destination map dict."""
    base = {
        "version": "2.3",
        "destinations": {
            "Images":    "media/images",
            "Documents": "docs",
            "Others":    "misc",
        },
    }
    base.update(overrides)
    return base


def _make_temp_file(directory: Path, name: str, content: bytes = b"x") -> Path:
    """Create a temp file with given name and content."""
    p = directory / name
    p.write_bytes(content)
    return p


def _collect_tree(root: Path) -> list[str]:
    """
    Return sorted list of relative posix paths for all files under root.
    Used to compare output trees across runs.
    """
    result = []
    for p in root.rglob("*"):
        if p.is_file():
            result.append(p.relative_to(root).as_posix())
    return sorted(result)


# ---------------------------------------------------------------------------
# 1. Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation(unittest.TestCase):

    def test_version_required(self):
        data = {"destinations": {"A": "a"}}
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("version", str(ctx.exception))

    def test_destinations_required(self):
        data = {"version": "2.3"}
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("destinations", str(ctx.exception))

    def test_unsupported_version(self):
        data = _minimal_map_data(version="1.0")
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("1.0", str(ctx.exception))

    def test_version_not_string(self):
        data = _minimal_map_data(version=2)
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("version", str(ctx.exception))

    def test_unknown_top_key(self):
        data = _minimal_map_data()
        data["nonexistent_key"] = "oops"
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("nonexistent_key", str(ctx.exception))

    def test_destinations_not_dict(self):
        data = _minimal_map_data(destinations=["Images", "Videos"])
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("destinations", str(ctx.exception))

    def test_destinations_empty_dict(self):
        data = _minimal_map_data(destinations={})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("at least one", str(ctx.exception))

    def test_not_a_dict(self):
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(["not", "a", "dict"])
        self.assertIn("JSON object", str(ctx.exception))

    def test_valid_minimal(self):
        dm = parse_destination_map(_minimal_map_data())
        self.assertEqual(dm.schema_version, "2.3")
        self.assertEqual(dm.conflict_policy, "rename")
        self.assertEqual(dm.template_fallback, "misc")

    def test_valid_full(self):
        data = {
            "version": "2.3",
            "destinations": {"Videos": "media/{year}"},
            "conflict_policy": "skip",
            "template_fallback": "unknown",
            "base_dir": "/tmp",
        }
        dm = parse_destination_map(data)
        self.assertEqual(dm.conflict_policy, "skip")
        self.assertEqual(dm.template_fallback, "unknown")
        self.assertEqual(dm.base_dir, Path("/tmp"))


# ---------------------------------------------------------------------------
# 2. Destination key validation
# ---------------------------------------------------------------------------

class TestDestinationKeyValidation(unittest.TestCase):

    def test_empty_key(self):
        data = _minimal_map_data(destinations={"": "path"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_whitespace_only_key(self):
        data = _minimal_map_data(destinations={"   ": "path"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_key_with_forward_slash(self):
        data = _minimal_map_data(destinations={"a/b": "path"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("path separator", str(ctx.exception))

    def test_key_with_backslash(self):
        data = _minimal_map_data(destinations={"a\\b": "path"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("path separator", str(ctx.exception))

    def test_key_with_null_byte(self):
        data = _minimal_map_data(destinations={"a\x00b": "path"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("null byte", str(ctx.exception))

    def test_valid_key_names(self):
        data = _minimal_map_data(destinations={
            "Images": "images",
            "My Files": "myfiles",
            "2024_Archive": "archive",
        })
        dm = parse_destination_map(data)
        self.assertIn("Images", dm.destinations)


# ---------------------------------------------------------------------------
# 3. Template value validation
# ---------------------------------------------------------------------------

class TestTemplateValueValidation(unittest.TestCase):

    def test_unknown_template_variable(self):
        data = _minimal_map_data(destinations={"A": "path/{unknown_var}"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("unknown_var", str(ctx.exception))

    def test_empty_template_value(self):
        data = _minimal_map_data(destinations={"A": ""})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_whitespace_template_value(self):
        data = _minimal_map_data(destinations={"A": "   "})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_null_byte_in_template(self):
        data = _minimal_map_data(destinations={"A": "path\x00here"})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("null byte", str(ctx.exception))

    def test_template_not_string(self):
        data = _minimal_map_data(destinations={"A": 42})
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("string", str(ctx.exception))

    def test_all_known_template_vars_accepted(self):
        data = _minimal_map_data(destinations={
            "A": "{extension}/{year}/{month}/{size_bucket}",
        })
        dm = parse_destination_map(data)
        self.assertIn("A", dm.destinations)


# ---------------------------------------------------------------------------
# 4. Conflict policy validation
# ---------------------------------------------------------------------------

class TestConflictPolicyValidation(unittest.TestCase):

    def test_invalid_policy(self):
        data = _minimal_map_data(conflict_policy="overwrite")
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("overwrite", str(ctx.exception))

    def test_policy_not_string(self):
        data = _minimal_map_data(conflict_policy=1)
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("string", str(ctx.exception))

    def test_rename_policy(self):
        dm = parse_destination_map(_minimal_map_data(conflict_policy="rename"))
        self.assertEqual(dm.conflict_policy, "rename")

    def test_skip_policy(self):
        dm = parse_destination_map(_minimal_map_data(conflict_policy="skip"))
        self.assertEqual(dm.conflict_policy, "skip")

    def test_error_policy(self):
        dm = parse_destination_map(_minimal_map_data(conflict_policy="error"))
        self.assertEqual(dm.conflict_policy, "error")


# ---------------------------------------------------------------------------
# 5. Template fallback validation
# ---------------------------------------------------------------------------

class TestTemplateFallbackValidation(unittest.TestCase):

    def test_empty_fallback(self):
        data = _minimal_map_data(template_fallback="")
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_whitespace_fallback(self):
        data = _minimal_map_data(template_fallback="   ")
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))

    def test_fallback_not_string(self):
        data = _minimal_map_data(template_fallback=None)
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("string", str(ctx.exception))

    def test_valid_fallback(self):
        dm = parse_destination_map(_minimal_map_data(template_fallback="unknown"))
        self.assertEqual(dm.template_fallback, "unknown")

    def test_fallback_stripped(self):
        dm = parse_destination_map(_minimal_map_data(template_fallback="  other  "))
        self.assertEqual(dm.template_fallback, "other")


# ---------------------------------------------------------------------------
# 6. Base dir resolution
# ---------------------------------------------------------------------------

class TestBaseDirResolution(unittest.TestCase):

    def test_explicit_base_dir(self):
        data = _minimal_map_data(base_dir="/organized")
        dm = parse_destination_map(data)
        self.assertEqual(dm.base_dir, Path("/organized"))

    def test_default_base_dir_used(self):
        data = _minimal_map_data()  # no base_dir key
        dm = parse_destination_map(data, default_base_dir=Path("/custom"))
        self.assertEqual(dm.base_dir, Path("/custom"))

    def test_cwd_fallback(self):
        data = _minimal_map_data()  # no base_dir, no default
        dm = parse_destination_map(data, default_base_dir=None)
        self.assertEqual(dm.base_dir, Path.cwd())

    def test_explicit_overrides_default(self):
        data = _minimal_map_data(base_dir="/explicit")
        dm = parse_destination_map(data, default_base_dir=Path("/default"))
        self.assertEqual(dm.base_dir, Path("/explicit"))

    def test_base_dir_not_string(self):
        data = _minimal_map_data(base_dir=123)
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("string", str(ctx.exception))

    def test_base_dir_empty(self):
        data = _minimal_map_data(base_dir="")
        with self.assertRaises(DestinationMapError) as ctx:
            parse_destination_map(data)
        self.assertIn("empty", str(ctx.exception))


# ---------------------------------------------------------------------------
# 7. Template expansion
# ---------------------------------------------------------------------------

class TestTemplateExpansion(unittest.TestCase):

    MTIME_NS_2024_03 = 1_709_251_200_000_000_000  # 2024-03-01 00:00:00 UTC

    def test_no_template_fast_path(self):
        result = _resolve_template("media/images", "photo.jpg", 0, None, "misc")
        self.assertEqual(result, "media/images")

    def test_extension_lowercase(self):
        result = _resolve_template("{extension}", "PHOTO.JPG", 0, None, "misc")
        self.assertEqual(result, "jpg")

    def test_extension_no_dot(self):
        result = _resolve_template("{extension}", "photo.mp4", 0, None, "misc")
        self.assertEqual(result, "mp4")

    def test_extension_nested_path(self):
        result = _resolve_template("media/{extension}", "report.pdf", 0, None, "misc")
        self.assertEqual(result, "media/pdf")

    def test_year_from_mtime(self):
        result = _resolve_template("{year}", "f.txt", 0, self.MTIME_NS_2024_03, "misc")
        self.assertEqual(result, "2024")

    def test_month_from_mtime(self):
        result = _resolve_template("{month}", "f.txt", 0, self.MTIME_NS_2024_03, "misc")
        self.assertEqual(result, "03")

    def test_combined_year_month(self):
        result = _resolve_template(
            "archive/{year}/{month}", "f.txt", 0, self.MTIME_NS_2024_03, "misc"
        )
        self.assertEqual(result, "archive/2024/03")

    def test_size_bucket_small(self):
        result = _resolve_template("{size_bucket}", "f.bin", 500_000, None, "misc")
        self.assertEqual(result, "small")

    def test_size_bucket_medium(self):
        result = _resolve_template(
            "{size_bucket}", "f.bin", _SIZE_SMALL_CEILING, None, "misc"
        )
        self.assertEqual(result, "medium")

    def test_size_bucket_large(self):
        result = _resolve_template(
            "{size_bucket}", "f.bin", _SIZE_MEDIUM_CEILING, None, "misc"
        )
        self.assertEqual(result, "large")

    def test_all_vars_combined(self):
        result = _resolve_template(
            "{extension}/{year}/{month}/{size_bucket}",
            "video.mp4",
            _SIZE_MEDIUM_CEILING + 1,
            self.MTIME_NS_2024_03,
            "misc",
        )
        self.assertEqual(result, "mp4/2024/03/large")


# ---------------------------------------------------------------------------
# 8. Template fallback for missing variables
# ---------------------------------------------------------------------------

class TestTemplateFallback(unittest.TestCase):

    def test_extension_no_suffix(self):
        # Files with no extension (e.g. "Makefile") use fallback
        result = _resolve_template("{extension}", "Makefile", 0, None, "no_ext")
        self.assertEqual(result, "no_ext")

    def test_year_no_mtime(self):
        result = _resolve_template("{year}", "f.txt", 0, None, "unknown")
        self.assertEqual(result, "unknown")

    def test_month_no_mtime(self):
        result = _resolve_template("{month}", "f.txt", 0, None, "unknown")
        self.assertEqual(result, "unknown")

    def test_both_year_month_no_mtime(self):
        result = _resolve_template("{year}/{month}", "f.txt", 0, None, "X")
        self.assertEqual(result, "X/X")

    def test_custom_fallback_value(self):
        result = _resolve_template("{extension}", "README", 0, None, "other")
        self.assertEqual(result, "other")


# ---------------------------------------------------------------------------
# 9. Size bucket thresholds — exact boundary values
# ---------------------------------------------------------------------------

class TestSizeBucketThresholds(unittest.TestCase):

    def test_zero_bytes(self):
        self.assertEqual(_resolve_size_bucket(0), "small")

    def test_one_byte(self):
        self.assertEqual(_resolve_size_bucket(1), "small")

    def test_just_below_small_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_SMALL_CEILING - 1), "small")

    def test_at_small_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_SMALL_CEILING), "medium")

    def test_just_above_small_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_SMALL_CEILING + 1), "medium")

    def test_just_below_medium_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_MEDIUM_CEILING - 1), "medium")

    def test_at_medium_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_MEDIUM_CEILING), "large")

    def test_just_above_medium_ceiling(self):
        self.assertEqual(_resolve_size_bucket(_SIZE_MEDIUM_CEILING + 1), "large")

    def test_huge_file(self):
        self.assertEqual(_resolve_size_bucket(10 * _SIZE_MEDIUM_CEILING), "large")


# ---------------------------------------------------------------------------
# 10. resolve_destination_path
# ---------------------------------------------------------------------------

class TestResolveDestinationPath(unittest.TestCase):

    def _make_dm(self, **kwargs) -> DestinationMap:
        data = {
            "version": "2.3",
            "destinations": {
                "Images": "media/images",
                "Videos": "media/videos/{year}",
                "Others": "misc",
            },
            "base_dir": "/organized",
        }
        data.update(kwargs)
        return parse_destination_map(data)

    def test_unknown_dest_key_raises(self):
        dm = self._make_dm()
        with self.assertRaises(DestinationMapError) as ctx:
            resolve_destination_path("Documents", "file.pdf", 0, None, dm)
        self.assertIn("Documents", str(ctx.exception))
        self.assertIn("not defined", str(ctx.exception))

    def test_relative_template_resolves_to_base_dir(self):
        dm = self._make_dm()
        path = resolve_destination_path("Images", "photo.jpg", 0, None, dm)
        self.assertTrue(path.is_absolute())
        self.assertIn("media", str(path))
        self.assertIn("images", str(path))

    def test_absolute_template_used_directly(self):
        data = {
            "version": "2.3",
            "destinations": {"Images": "/absolute/path/images"},
            "base_dir": "/organized",
        }
        dm = parse_destination_map(data)
        path = resolve_destination_path("Images", "photo.jpg", 0, None, dm)
        self.assertEqual(path, Path("/absolute/path/images"))

    def test_template_expansion_in_path(self):
        dm = self._make_dm()
        mtime = 1_709_251_200_000_000_000  # 2024-03-01 UTC
        path = resolve_destination_path("Videos", "v.mp4", 0, mtime, dm)
        self.assertIn("2024", str(path))

    def test_missing_key_error_mentions_known_keys(self):
        dm = self._make_dm()
        with self.assertRaises(DestinationMapError) as ctx:
            resolve_destination_path("Unknown", "f.txt", 0, None, dm)
        msg = str(ctx.exception)
        self.assertIn("Images", msg)
        self.assertIn("Others", msg)
        self.assertIn("Videos", msg)


# ---------------------------------------------------------------------------
# 11. validate_ruleset_destinations
# ---------------------------------------------------------------------------

class TestValidateRulesetDestinations(unittest.TestCase):

    def _make_dm(self) -> DestinationMap:
        return parse_destination_map(_minimal_map_data())

    def test_all_present_no_error(self):
        dm = self._make_dm()
        validate_ruleset_destinations({"Images", "Documents", "Others"}, dm)

    def test_missing_single_key(self):
        dm = self._make_dm()
        with self.assertRaises(DestinationMapError) as ctx:
            validate_ruleset_destinations({"Images", "Videos", "Others"}, dm)
        self.assertIn("Videos", str(ctx.exception))

    def test_missing_multiple_keys(self):
        dm = self._make_dm()
        with self.assertRaises(DestinationMapError) as ctx:
            validate_ruleset_destinations({"Videos", "Audio", "Images"}, dm)
        msg = str(ctx.exception)
        self.assertIn("Videos", msg)
        self.assertIn("Audio", msg)

    def test_extra_keys_in_map_ok(self):
        # Map can define more destinations than ruleset uses — that's fine
        data = _minimal_map_data(destinations={
            "Images": "images", "Documents": "docs",
            "Others": "misc",  "Unused": "unused",
        })
        dm = parse_destination_map(data)
        validate_ruleset_destinations({"Images", "Documents"}, dm)

    def test_empty_ruleset_destinations(self):
        dm = self._make_dm()
        validate_ruleset_destinations(set(), dm)  # should not raise


# ---------------------------------------------------------------------------
# 12–14. Conflict policies via organizer integration
# ---------------------------------------------------------------------------

class TestConflictPolicies(unittest.TestCase):
    """
    Tests conflict policy enforcement through organize().
    Uses real filesystem via tempfile.TemporaryDirectory.
    """

    def _make_rule_data(self) -> dict:
        return {
            "version": "2.0",
            "rules": [
                {"name": "Text", "match": {"extensions": [".txt"]}, "destination": "Text"},
            ],
            "default_destination": "Others",
        }

    def _make_dest_map(self, policy: str, base_dir: Path) -> DestinationMap:
        return parse_destination_map({
            "version": "2.3",
            "destinations": {
                "Text":   "texts",
                "Others": "others",
            },
            "conflict_policy": policy,
        }, default_base_dir=base_dir)

    def test_rename_policy_10_collisions(self):
        """
        Conflict policy 'rename': organize 10 files with the same name into
        same destination. Each should get a unique collision-suffixed name.
        Tests the completion requirement: "Conflict resolution tested with
        10 duplicates."
        """
        ruleset = parse_rules(self._make_rule_data())

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            # Create 10 files with content-distinct data but same name
            # Simulate sequential organize calls (same-name files in different dirs)
            dest_map = self._make_dest_map("rename", src)
            texts_dir = src / "texts"
            texts_dir.mkdir(parents=True, exist_ok=True)

            # Pre-plant the original file in destination
            (texts_dir / "note.txt").write_bytes(b"original")

            # Organize 10 more files named note.txt
            for i in range(10):
                subfolder = src / f"run_{i}"
                subfolder.mkdir()
                (subfolder / "note.txt").write_bytes(f"copy {i}".encode())
                sub_dm = parse_destination_map({
                    "version": "2.3",
                    "destinations": {"Text": str(texts_dir), "Others": str(src / "others")},
                    "conflict_policy": "rename",
                }, default_base_dir=subfolder)
                result = organize(subfolder, dry_run=False, ruleset=ruleset, dest_map=sub_dm)
                self.assertEqual(result["total"], 1)

            # texts_dir should now have 11 files: note.txt + note (1).txt … note (10).txt
            txt_files = list(texts_dir.glob("note*.txt"))
            self.assertEqual(len(txt_files), 11)

    def test_skip_policy_10_collisions(self):
        """
        Conflict policy 'skip': when destination already has same filename,
        the source file is left in place (skipped) — not overwritten, not renamed.
        """
        ruleset = parse_rules(self._make_rule_data())

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            dest_map = self._make_dest_map("skip", src)

            texts_dir = src / "texts"
            texts_dir.mkdir(parents=True, exist_ok=True)

            # Plant 10 pre-existing files in destination
            for i in range(10):
                (texts_dir / f"note{i}.txt").write_bytes(b"existing")
                (src / f"note{i}.txt").write_bytes(b"new")  # same name in src

            result = organize(src, dry_run=False, ruleset=ruleset, dest_map=dest_map)

            # All 10 source files should be skipped — destination untouched
            skipped = [r for r in result["results"] if r["status"] == "skipped"]
            self.assertEqual(len(skipped), 10)

            # Destinations still have original content
            for i in range(10):
                self.assertEqual((texts_dir / f"note{i}.txt").read_bytes(), b"existing")

            # Source files remain in src
            for i in range(10):
                self.assertTrue((src / f"note{i}.txt").exists())

    def test_error_policy_aborts_on_collision(self):
        """
        Conflict policy 'error': organize aborts immediately when any destination
        file collision is detected. No moves should have occurred.
        """
        ruleset = parse_rules(self._make_rule_data())

        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            dest_map = self._make_dest_map("error", src)

            texts_dir = src / "texts"
            texts_dir.mkdir(parents=True, exist_ok=True)
            (texts_dir / "blocked.txt").write_bytes(b"existing")
            (src / "blocked.txt").write_bytes(b"new")
            (src / "safe.txt").write_bytes(b"safe")

            result = organize(src, dry_run=False, ruleset=ruleset, dest_map=dest_map)

            # Result should contain an error key
            self.assertIn("error", result)
            self.assertIn("conflict_error", result["error"])


# ---------------------------------------------------------------------------
# 15. Nested auto-create
# ---------------------------------------------------------------------------

class TestNestedAutoCreate(unittest.TestCase):

    def test_deep_destination_created(self):
        """
        Organize files into deeply nested destination paths (6 levels).
        All parent directories must be created automatically.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "video.mp4").write_bytes(b"video")

            deep_dest = src / "a" / "b" / "c" / "d" / "e" / "videos"

            ruleset = parse_rules({
                "version": "2.0",
                "rules": [
                    {"name": "Videos",
                     "match": {"extensions": [".mp4"]},
                     "destination": "Videos"},
                ],
                "default_destination": "Others",
            })
            dest_map = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Videos": str(deep_dest),
                    "Others": str(src / "others"),
                },
            }, default_base_dir=src)

            result = organize(src, dry_run=False, ruleset=ruleset, dest_map=dest_map)

            self.assertTrue(deep_dest.exists())
            self.assertTrue((deep_dest / "video.mp4").exists())
            moved = [r for r in result["results"] if r["status"] == "moved"]
            self.assertEqual(len(moved), 1)

    def test_idempotent_dir_creation(self):
        """Running organize twice does not crash on already-existing dirs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()

            ruleset = parse_rules({
                "version": "2.0",
                "rules": [{"name": "T",
                            "match": {"extensions": [".txt"]},
                            "destination": "Text"}],
                "default_destination": "Others",
            })
            dest_map = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Text":   str(src / "texts"),
                    "Others": str(src / "others"),
                },
                "conflict_policy": "rename",
            }, default_base_dir=src)

            # First run
            (src / "a.txt").write_bytes(b"a")
            result1 = organize(src, dry_run=False, ruleset=ruleset, dest_map=dest_map)
            self.assertEqual(result1["total"], 1)

            # Second run — dir already exists; should not crash
            (src / "b.txt").write_bytes(b"b")
            result2 = organize(src, dry_run=False, ruleset=ruleset, dest_map=dest_map)
            self.assertEqual(result2["total"], 1)


# ---------------------------------------------------------------------------
# 16. Organizer integration
# ---------------------------------------------------------------------------

class TestOrganizerIntegration(unittest.TestCase):

    def _ruleset(self) -> object:
        return parse_rules({
            "version": "2.0",
            "rules": [
                {"name": "Images",
                 "match": {"extensions": [".jpg", ".png"]},
                 "destination": "Images"},
                {"name": "Documents",
                 "match": {"extensions": [".pdf", ".txt"]},
                 "destination": "Documents"},
            ],
            "default_destination": "Others",
        })

    def test_dest_map_active_flag_true(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir)
            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images": "img", "Documents": "docs", "Others": "misc"
                },
            }, default_base_dir=src)
            result = organize(src, dry_run=True, ruleset=self._ruleset(), dest_map=dm)
            self.assertTrue(result["dest_map_active"])

    def test_dest_map_active_flag_false_without_map(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = organize(Path(tmpdir), dry_run=True)
            self.assertFalse(result["dest_map_active"])

    def test_organize_with_map_moves_files_correctly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "photo.jpg").write_bytes(b"img")
            (src / "report.pdf").write_bytes(b"doc")
            (src / "unknown.xyz").write_bytes(b"unk")

            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images":    "organized/images",
                    "Documents": "organized/docs",
                    "Others":    "organized/misc",
                },
            }, default_base_dir=src)

            result = organize(
                src, dry_run=False, ruleset=self._ruleset(), dest_map=dm
            )
            self.assertEqual(result["total"], 3)
            moved = [r for r in result["results"] if r["status"] == "moved"]
            self.assertEqual(len(moved), 3)

            self.assertTrue((src / "organized" / "images" / "photo.jpg").exists())
            self.assertTrue((src / "organized" / "docs" / "report.pdf").exists())
            self.assertTrue((src / "organized" / "misc" / "unknown.xyz").exists())

    def test_unknown_dest_key_caught_early(self):
        """dest_map missing a key used by ruleset → DestinationMapError before any moves."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "photo.jpg").write_bytes(b"img")

            # dest_map missing "Documents" key
            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images": "images",
                    # "Documents" is absent — but ruleset uses it
                    "Others": "misc",
                },
            }, default_base_dir=src)

            with self.assertRaises(DestinationMapError) as ctx:
                organize(src, dry_run=False, ruleset=self._ruleset(), dest_map=dm)
            self.assertIn("Documents", str(ctx.exception))

            # photo.jpg must still be in src (no moves occurred)
            self.assertTrue((src / "photo.jpg").exists())

    def test_dry_run_with_map_no_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "photo.jpg").write_bytes(b"img")

            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images": "images", "Documents": "docs", "Others": "misc"
                },
            }, default_base_dir=src)

            result = organize(src, dry_run=True, ruleset=self._ruleset(), dest_map=dm)

            # Nothing moved
            self.assertTrue((src / "photo.jpg").exists())
            dry = [r for r in result["results"] if r["status"] == "dry_run"]
            self.assertEqual(len(dry), 1)


# ---------------------------------------------------------------------------
# 17. Determinism — 3 identical runs produce identical output tree
# ---------------------------------------------------------------------------

class TestDeterminism(unittest.TestCase):

    def _make_ruleset(self):
        return parse_rules({
            "version": "2.0",
            "rules": [
                {"name": "Images",
                 "match": {"extensions": [".jpg", ".png", ".gif"]},
                 "destination": "Images"},
                {"name": "Documents",
                 "match": {"extensions": [".pdf", ".txt", ".md"]},
                 "destination": "Documents"},
                {"name": "Videos",
                 "match": {"extensions": [".mp4", ".avi"]},
                 "destination": "Videos"},
            ],
            "default_destination": "Others",
        })

    def _run_organize(self, src: Path, dest_root: Path, ruleset) -> list[str]:
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {
                "Images":    str(dest_root / "images"),
                "Documents": str(dest_root / "docs"),
                "Videos":    str(dest_root / "videos"),
                "Others":    str(dest_root / "misc"),
            },
            "conflict_policy": "rename",
        }, default_base_dir=src)
        organize(src, dry_run=False, ruleset=ruleset, dest_map=dm)
        return _collect_tree(dest_root)

    def test_three_runs_identical_output_tree(self):
        """
        Three independent organize runs on identical input produce identical
        output file trees. Satisfies the determinism completion requirement.
        """
        FILES = [
            ("photo1.jpg", b"img1"),
            ("photo2.png", b"img2"),
            ("report.pdf", b"doc1"),
            ("notes.txt",  b"doc2"),
            ("clip.mp4",   b"vid1"),
            ("unknown.xyz", b"other"),
        ]
        ruleset = self._make_ruleset()
        trees = []

        for run_idx in range(3):
            with tempfile.TemporaryDirectory() as tmpdir:
                src      = Path(tmpdir) / "src"
                dest     = Path(tmpdir) / "dest"
                src.mkdir()
                dest.mkdir()

                for name, content in FILES:
                    (src / name).write_bytes(content)

                tree = self._run_organize(src, dest, ruleset)
                trees.append(tree)

        # All three trees must be identical
        self.assertEqual(trees[0], trees[1], "Run 1 and Run 2 differ")
        self.assertEqual(trees[1], trees[2], "Run 2 and Run 3 differ")

        # Spot-check expected structure
        combined = trees[0]
        self.assertIn("images/photo1.jpg", combined)
        self.assertIn("images/photo2.png", combined)
        self.assertIn("docs/report.pdf",   combined)
        self.assertIn("docs/notes.txt",    combined)
        self.assertIn("videos/clip.mp4",   combined)
        self.assertIn("misc/unknown.xyz",  combined)


# ---------------------------------------------------------------------------
# 18. Large directory stress test
# ---------------------------------------------------------------------------

class TestLargeDirectoryStress(unittest.TestCase):

    def test_200_files_mixed_extensions(self):
        """
        Stress test: organize 200 files across multiple extension categories.
        Verifies correctness, no crashes, and all files accounted for.
        """
        EXTENSIONS = [".jpg", ".png", ".pdf", ".txt", ".mp4", ".zip", ".xyz"]
        ruleset = parse_rules({
            "version": "2.0",
            "rules": [
                {"name": "Images",
                 "match": {"extensions": [".jpg", ".png"]},
                 "destination": "Images"},
                {"name": "Documents",
                 "match": {"extensions": [".pdf", ".txt"]},
                 "destination": "Documents"},
                {"name": "Videos",
                 "match": {"extensions": [".mp4"]},
                 "destination": "Videos"},
                {"name": "Archives",
                 "match": {"extensions": [".zip"]},
                 "destination": "Archives"},
            ],
            "default_destination": "Others",
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            src  = Path(tmpdir) / "src"
            dest = Path(tmpdir) / "dest"
            src.mkdir()
            dest.mkdir()

            # Create 200 files with deterministic, varied names
            file_count = 200
            for i in range(file_count):
                ext  = EXTENSIONS[i % len(EXTENSIONS)]
                name = f"file_{i:04d}{ext}"
                (src / name).write_bytes(f"content_{i}".encode())

            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images":    str(dest / "images"),
                    "Documents": str(dest / "docs"),
                    "Videos":    str(dest / "videos"),
                    "Archives":  str(dest / "archives"),
                    "Others":    str(dest / "misc"),
                },
                "conflict_policy": "rename",
            }, default_base_dir=src)

            result = organize(src, dry_run=False, ruleset=ruleset, dest_map=dm)

            self.assertEqual(result["total"], file_count)
            moved   = [r for r in result["results"] if r["status"] == "moved"]
            failed  = [r for r in result["results"] if r["status"] == "failed"]
            self.assertEqual(len(failed), 0)
            self.assertEqual(len(moved), file_count)

            # Total files in dest tree equals file_count
            all_dest = list(dest.rglob("*"))
            dest_files = [p for p in all_dest if p.is_file()]
            self.assertEqual(len(dest_files), file_count)

            # src is now empty (all files moved)
            src_files = [p for p in src.iterdir() if p.is_file()]
            self.assertEqual(len(src_files), 0)


# ---------------------------------------------------------------------------
# 19. v2.2 regression — dest_map=None behavior unchanged
# ---------------------------------------------------------------------------

class TestV22Regression(unittest.TestCase):

    def test_organize_without_dest_map_unchanged(self):
        """
        organize() with dest_map=None must behave identically to v2.2:
        files placed in folder/destination_key subdirectories.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            (folder / "photo.jpg").write_bytes(b"img")
            (folder / "report.pdf").write_bytes(b"doc")

            result = organize(folder, dry_run=False)

            self.assertFalse(result["dest_map_active"])
            self.assertEqual(result["total"], 2)
            moved = [r for r in result["results"] if r["status"] == "moved"]
            self.assertEqual(len(moved), 2)

            # Files land in folder-relative subdirs (v2.2 behavior)
            self.assertTrue((folder / "Images" / "photo.jpg").exists())
            self.assertTrue((folder / "Documents" / "report.pdf").exists())

    def test_resolve_destination_unchanged(self):
        """rules.resolve_destination() returns same results as before v2.3."""
        from rules import resolve_destination
        self.assertEqual(resolve_destination("photo.jpg", DEFAULT_RULESET), "Images")
        self.assertEqual(resolve_destination("report.pdf", DEFAULT_RULESET), "Documents")
        self.assertEqual(resolve_destination("video.mp4", DEFAULT_RULESET), "Videos")
        self.assertEqual(resolve_destination("music.mp3", DEFAULT_RULESET), "Audio")
        self.assertEqual(resolve_destination("data.zip", DEFAULT_RULESET), "Archives")
        self.assertEqual(resolve_destination("script.py", DEFAULT_RULESET), "Code")
        self.assertEqual(resolve_destination("unknown.xyz", DEFAULT_RULESET), "Others")


# ---------------------------------------------------------------------------
# 20. dest_map_active result key
# ---------------------------------------------------------------------------

class TestDestMapActiveKey(unittest.TestCase):

    def test_key_present_with_map(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir)
            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {
                    "Images": "img", "Documents": "docs",
                    "Videos": "vids", "Audio": "aud",
                    "Archives": "arc", "Code": "code", "Others": "misc",
                },
            }, default_base_dir=src)
            result = organize(src, dry_run=True, dest_map=dm)
            self.assertIn("dest_map_active", result)
            self.assertTrue(result["dest_map_active"])

    def test_key_present_without_map(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = organize(Path(tmpdir), dry_run=True)
            self.assertIn("dest_map_active", result)
            self.assertFalse(result["dest_map_active"])


# ---------------------------------------------------------------------------
# DestinationMap repr
# ---------------------------------------------------------------------------

class TestDestinationMapRepr(unittest.TestCase):

    def test_repr_contains_key_info(self):
        dm = parse_destination_map(_minimal_map_data(), default_base_dir=Path("/base"))
        r = repr(dm)
        self.assertIn("DestinationMap", r)
        self.assertIn("rename", r)   # default conflict_policy
        self.assertIn("2.3", r)


if __name__ == "__main__":
    unittest.main(verbosity=2)

# ---------------------------------------------------------------------------
# E1 — 10,000-file scale stress test
# ---------------------------------------------------------------------------

class TestTenThousandFileScale(unittest.TestCase):
    """
    E1 requirement: tested with 10,000+ files.

    Verifies:
      - No crash, no OOM, no O(N^2) hang for 10K files.
      - All files accounted for (moved).
      - Two runs produce identical sorted result lists (determinism).
    """

    _EXTENSIONS = [
        ".jpg", ".png", ".pdf", ".txt", ".mp4",
        ".zip", ".py",  ".mp3", ".avi", ".md",
    ]

    def _make_ruleset(self):
        return parse_rules({
            "version": "2.0",
            "rules": [
                {"name": "Images",    "match": {"extensions": [".jpg", ".png"]},        "destination": "Images"},
                {"name": "Documents", "match": {"extensions": [".pdf", ".txt", ".md"]}, "destination": "Documents"},
                {"name": "Videos",    "match": {"extensions": [".mp4", ".avi"]},        "destination": "Videos"},
                {"name": "Archives",  "match": {"extensions": [".zip"]},                "destination": "Archives"},
                {"name": "Code",      "match": {"extensions": [".py"]},                 "destination": "Code"},
                {"name": "Audio",     "match": {"extensions": [".mp3"]},                "destination": "Audio"},
            ],
            "default_destination": "Others",
        })

    def _run(self, src, dest, ruleset) -> list:
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {
                "Images": str(dest / "images"), "Documents": str(dest / "docs"),
                "Videos": str(dest / "videos"), "Archives": str(dest / "archives"),
                "Code": str(dest / "code"),     "Audio": str(dest / "audio"),
                "Others": str(dest / "misc"),
            },
            "conflict_policy": "rename",
        }, default_base_dir=src)
        result = organize(src, dry_run=False, ruleset=ruleset, dest_map=dm)
        return sorted(f"{r['file']}:{r['status']}" for r in result["results"])

    def test_10000_files_all_moved(self):
        FILE_COUNT = 10_000
        ruleset = self._make_ruleset()
        with tempfile.TemporaryDirectory() as tmpdir:
            src  = Path(tmpdir) / "src"
            dest = Path(tmpdir) / "dest"
            src.mkdir(); dest.mkdir()
            for i in range(FILE_COUNT):
                ext  = self._EXTENSIONS[i % len(self._EXTENSIONS)]
                (src / f"file_{i:05d}{ext}").write_bytes(b"x")
            result_list = self._run(src, dest, ruleset)
            self.assertEqual(len(result_list), FILE_COUNT)
            failures = [r for r in result_list if ":failed" in r]
            self.assertEqual(failures, [])
            dest_files = [p for p in dest.rglob("*") if p.is_file()]
            self.assertEqual(len(dest_files), FILE_COUNT)

    def test_10000_files_deterministic_two_runs(self):
        FILE_COUNT = 10_000
        ruleset = self._make_ruleset()
        runs = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as tmpdir:
                src  = Path(tmpdir) / "src"
                dest = Path(tmpdir) / "dest"
                src.mkdir(); dest.mkdir()
                for i in range(FILE_COUNT):
                    ext = self._EXTENSIONS[i % len(self._EXTENSIONS)]
                    (src / f"file_{i:05d}{ext}").write_bytes(b"x")
                runs.append(self._run(src, dest, ruleset))
        self.assertEqual(runs[0], runs[1], "Two 10K runs produced different result lists")


# ---------------------------------------------------------------------------
# D10 — Path traversal boundary enforcement (patch test)
# ---------------------------------------------------------------------------

class TestPathTraversalPrevention(unittest.TestCase):

    def test_relative_traversal_escaping_base_dir_raises(self):
        """D10: Template '../../../tmp/evil' must not escape base_dir."""
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {"Others": "../../../tmp/evil"},
            "base_dir": "/safe/base",
        })
        with self.assertRaises(DestinationMapError) as ctx:
            resolve_destination_path("Others", "file.txt", 0, None, dm)
        msg = str(ctx.exception)
        self.assertIn("escapes", msg)
        self.assertIn("base_dir", msg)

    def test_relative_path_inside_base_dir_allowed(self):
        """D10: A well-formed relative path inside base_dir works normally."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = parse_destination_map({
                "version": "2.3",
                "destinations": {"Others": "subdir/nested"},
            }, default_base_dir=Path(tmpdir))
            result = resolve_destination_path("Others", "file.txt", 0, None, dm)
            # Must be inside tmpdir
            result.relative_to(Path(tmpdir))  # raises ValueError if not

    def test_absolute_template_not_restricted(self):
        """D10: Absolute templates are trusted as-is (explicit user intent)."""
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {"Others": "/absolute/target"},
            "base_dir": "/safe/base",
        })
        result = resolve_destination_path("Others", "file.txt", 0, None, dm)
        self.assertEqual(result, Path("/absolute/target"))


# ---------------------------------------------------------------------------
# E7 — DestinationMap true immutability (patch test)
# ---------------------------------------------------------------------------

class TestDestinationMapImmutability(unittest.TestCase):

    def test_destinations_dict_is_read_only(self):
        """E7: destinations must be a MappingProxyType — mutation raises TypeError."""
        import types as _types
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {"A": "path/a"},
        })
        self.assertIsInstance(dm.destinations, _types.MappingProxyType)
        with self.assertRaises(TypeError):
            dm.destinations["B"] = "injected"  # type: ignore[index]

    def test_field_reassignment_blocked(self):
        """E7: frozen dataclass blocks field reassignment."""
        from dataclasses import FrozenInstanceError
        dm = parse_destination_map({
            "version": "2.3",
            "destinations": {"A": "path/a"},
        })
        with self.assertRaises(FrozenInstanceError):
            dm.conflict_policy = "overwrite"  # type: ignore[misc]
