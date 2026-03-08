"""
CleanSweep v2.8.0 — Policy Enforcement Mode Tests.

Full coverage of the policy layer, conflict detection, mode behaviors,
metrics aggregation, config integration, and CLI wiring.

Test groups:
  1.  find_all_matching_rules — zero, one, many matches; sorted order
  2.  resolve_with_policy — no conflict path (all modes)
  3.  PolicyResult structure — fields, immutability
  4.  Strict mode — first conflict aborts; PolicyConflictError content
  5.  Safe mode   — skip semantics; engine continues; metrics
  6.  Warn mode   — first-rule wins; override logged; metrics
  7.  Determinism — stable rule order in conflicts; repeated calls identical
  8.  PolicyMetrics — fields, conflict_details ordering, empty run
  9.  plan_with_policy — aborted flag, skipped exclusion, metrics
  10. BatchEngine.run_from_files — policy_mode wiring, policy_metrics on report
  11. Config — policy_mode field validation, parse, defaults
  12. CLI flag — --policy overrides config; choices enforced
  13. Default policy — "safe" everywhere (config, engine, planner)
  14. Edge cases — empty ruleset files, default_destination, no extensions
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from rules import parse_rules, resolve_destination, RuleSet, Rule, find_all_matching_rules
from policy import (
    resolve_with_policy,
    PolicyResult,
    PolicyConflict,
    PolicyMetrics,
    PolicyConflictError,
    STRICT, SAFE, WARN,
    VALID_POLICY_MODES,
    DEFAULT_POLICY_MODE,
)
from planner import plan_with_policy, PolicyPlanResult, FileMetadata
from config import parse_config, AppConfig, ConfigError, CONFIG_VERSION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ruleset_two_overlapping(priority_a: int = 0, priority_b: int = 0) -> RuleSet:
    """
    Build a RuleSet where .jpg matches both Rule_A and Rule_B.
    Rule_A is config-index 0, Rule_B is config-index 1.
    With equal priority Rule_A always wins (stable sort).
    """
    data = {
        "version": "2.2",
        "rules": [
            {
                "name": "Rule_A",
                "priority": priority_a,
                "match": {"extensions": [".jpg"]},
                "destination": "DestA",
            },
            {
                "name": "Rule_B",
                "priority": priority_b,
                "match": {"extensions": [".jpg"]},
                "destination": "DestB",
            },
        ],
        "default_destination": "Others",
    }
    return parse_rules(data)


def _ruleset_no_overlap() -> RuleSet:
    """RuleSet where .jpg → Images, .mp4 → Videos (no overlap)."""
    data = {
        "version": "2.2",
        "rules": [
            {
                "name": "Images",
                "match": {"extensions": [".jpg", ".png"]},
                "destination": "Images",
            },
            {
                "name": "Videos",
                "match": {"extensions": [".mp4"]},
                "destination": "Videos",
            },
        ],
        "default_destination": "Others",
    }
    return parse_rules(data)


def _make_metadata(filenames: list[str], sizes: list[int] | None = None) -> tuple[FileMetadata, ...]:
    """Create FileMetadata tuples for testing (path components are fake)."""
    if sizes is None:
        sizes = [0] * len(filenames)
    return tuple(
        FileMetadata(path=Path(f"/fake/{name}"), size=s, mtime_ns=None)
        for name, s in zip(filenames, sizes)
    )


def _base_config(**overrides) -> dict:
    base = {
        "version": CONFIG_VERSION,
        "scan": {
            "recursive": True,
            "symlink_policy": "ignore",
        },
    }
    base.update(overrides)
    return base


# ===========================================================================
# 1. find_all_matching_rules
# ===========================================================================

class TestFindAllMatchingRules(unittest.TestCase):

    def test_no_match_returns_empty_list(self):
        rs = _ruleset_no_overlap()
        result = find_all_matching_rules(rs, "document.pdf", ".pdf", 0)
        self.assertEqual(result, [])

    def test_single_match_returns_one_element(self):
        rs = _ruleset_no_overlap()
        result = find_all_matching_rules(rs, "photo.jpg", ".jpg", 0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Images")

    def test_two_overlapping_rules_returns_both(self):
        rs = _ruleset_two_overlapping()
        result = find_all_matching_rules(rs, "photo.jpg", ".jpg", 0)
        self.assertEqual(len(result), 2)
        names = [r.name for r in result]
        self.assertIn("Rule_A", names)
        self.assertIn("Rule_B", names)

    def test_order_follows_ruleset_sort(self):
        """Lower priority wins → appears first in result list."""
        rs = _ruleset_two_overlapping(priority_a=0, priority_b=1)
        result = find_all_matching_rules(rs, "photo.jpg", ".jpg", 0)
        self.assertEqual(result[0].name, "Rule_A")
        self.assertEqual(result[1].name, "Rule_B")

    def test_size_constraint_reduces_matches(self):
        """Rule with min_size only matches when file is large enough."""
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "AnyVideo",
                    "match": {"extensions": [".mp4"]},
                    "destination": "Videos",
                },
                {
                    "name": "LargeVideo",
                    "match": {"extensions": [".mp4"], "min_size": 1000},
                    "destination": "LargeVideos",
                },
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        small = find_all_matching_rules(rs, "clip.mp4", ".mp4", 500)
        large = find_all_matching_rules(rs, "clip.mp4", ".mp4", 2000)
        self.assertEqual(len(small), 1)
        self.assertEqual(small[0].name, "AnyVideo")
        self.assertEqual(len(large), 2)

    def test_returns_rules_not_names(self):
        """Return type is list[Rule], not list[str]."""
        rs = _ruleset_no_overlap()
        result = find_all_matching_rules(rs, "photo.jpg", ".jpg", 0)
        self.assertIsInstance(result[0], Rule)

    def test_case_insensitive_extension(self):
        rs = _ruleset_no_overlap()
        result = find_all_matching_rules(rs, "PHOTO.JPG", ".jpg", 0)
        self.assertEqual(len(result), 1)


# ===========================================================================
# 2. resolve_with_policy — no-conflict path
# ===========================================================================

class TestResolveWithPolicyNoConflict(unittest.TestCase):

    def _call(self, filename: str, mode: str, rs: RuleSet | None = None) -> PolicyResult:
        if rs is None:
            rs = _ruleset_no_overlap()
        return resolve_with_policy(filename, rs, mode)

    def test_no_match_returns_default_destination(self):
        for mode in (STRICT, SAFE, WARN):
            with self.subTest(mode=mode):
                result = self._call("unknown.xyz", mode)
                self.assertEqual(result.destination, "Others")
                self.assertFalse(result.skipped)
                self.assertFalse(result.override)
                self.assertIsNone(result.conflict)

    def test_single_match_returns_destination(self):
        for mode in (STRICT, SAFE, WARN):
            with self.subTest(mode=mode):
                result = self._call("photo.jpg", mode)
                self.assertEqual(result.destination, "Images")
                self.assertFalse(result.skipped)
                self.assertFalse(result.override)
                self.assertIsNone(result.conflict)

    def test_result_immutable(self):
        result = self._call("photo.jpg", SAFE)
        with self.assertRaises(Exception):
            result.destination = "Hacked"  # type: ignore[misc]


# ===========================================================================
# 3. Strict mode
# ===========================================================================

class TestStrictMode(unittest.TestCase):

    def _conflicting_ruleset(self) -> RuleSet:
        return _ruleset_two_overlapping()

    def test_raises_policy_conflict_error_on_conflict(self):
        rs = self._conflicting_ruleset()
        with self.assertRaises(PolicyConflictError):
            resolve_with_policy("photo.jpg", rs, STRICT)

    def test_error_carries_conflict_attribute(self):
        rs = self._conflicting_ruleset()
        try:
            resolve_with_policy("photo.jpg", rs, STRICT)
            self.fail("Expected PolicyConflictError")
        except PolicyConflictError as exc:
            self.assertIsInstance(exc.conflict, PolicyConflict)
            self.assertEqual(exc.conflict.filename, "photo.jpg")

    def test_error_message_contains_filename(self):
        rs = self._conflicting_ruleset()
        with self.assertRaises(PolicyConflictError) as ctx:
            resolve_with_policy("photo.jpg", rs, STRICT)
        msg = str(ctx.exception)
        self.assertIn("photo.jpg", msg)

    def test_error_message_contains_rule_names(self):
        rs = self._conflicting_ruleset()
        with self.assertRaises(PolicyConflictError) as ctx:
            resolve_with_policy("photo.jpg", rs, STRICT)
        msg = str(ctx.exception)
        self.assertIn("Rule_A", msg)
        self.assertIn("Rule_B", msg)

    def test_error_message_contains_policy_label(self):
        rs = self._conflicting_ruleset()
        with self.assertRaises(PolicyConflictError) as ctx:
            resolve_with_policy("photo.jpg", rs, STRICT)
        msg = str(ctx.exception).lower()
        self.assertIn("strict", msg)

    def test_no_conflict_no_error(self):
        rs = _ruleset_no_overlap()
        # Must not raise
        result = resolve_with_policy("photo.jpg", rs, STRICT)
        self.assertEqual(result.destination, "Images")

    def test_error_aborted_message_present(self):
        rs = self._conflicting_ruleset()
        with self.assertRaises(PolicyConflictError) as ctx:
            resolve_with_policy("photo.jpg", rs, STRICT)
        self.assertIn("aborted", str(ctx.exception).lower())


# ===========================================================================
# 4. Safe mode
# ===========================================================================

class TestSafeMode(unittest.TestCase):

    def test_conflict_returns_skipped_true(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertTrue(result.skipped)

    def test_conflict_destination_is_none(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertIsNone(result.destination)

    def test_conflict_carries_conflict_object(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertIsNotNone(result.conflict)
        self.assertEqual(result.conflict.filename, "photo.jpg")
        self.assertIn("Rule_A", result.conflict.matching_rules)
        self.assertIn("Rule_B", result.conflict.matching_rules)

    def test_no_conflict_file_not_skipped(self):
        rs = _ruleset_no_overlap()
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertFalse(result.skipped)
        self.assertEqual(result.destination, "Images")

    def test_override_false_in_safe_mode(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertFalse(result.override)


# ===========================================================================
# 5. Warn mode
# ===========================================================================

class TestWarnMode(unittest.TestCase):

    def test_conflict_returns_first_rule_destination(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertEqual(result.destination, "DestA")  # Rule_A first

    def test_override_flag_set(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertTrue(result.override)

    def test_not_skipped(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertFalse(result.skipped)

    def test_conflict_object_present(self):
        rs = _ruleset_two_overlapping()
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertIsNotNone(result.conflict)

    def test_first_rule_determined_by_priority(self):
        """Lower priority value → appears first → wins in warn mode."""
        rs = _ruleset_two_overlapping(priority_a=10, priority_b=1)
        result = resolve_with_policy("photo.jpg", rs, WARN)
        # Rule_B has priority=1 < Rule_A priority=10, so Rule_B sorts first
        self.assertEqual(result.destination, "DestB")

    def test_no_conflict_no_override(self):
        rs = _ruleset_no_overlap()
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertFalse(result.override)
        self.assertIsNone(result.conflict)


# ===========================================================================
# 6. Determinism
# ===========================================================================

class TestPolicyDeterminism(unittest.TestCase):

    def test_repeated_calls_identical_result(self):
        rs = _ruleset_two_overlapping()
        for mode in (SAFE, WARN):
            results = [
                resolve_with_policy("photo.jpg", rs, mode)
                for _ in range(5)
            ]
            # All destinations identical
            dests = [r.destination for r in results]
            self.assertEqual(len(set(str(d) for d in dests)), 1, msg=f"mode={mode}")

    def test_strict_always_raises_same_conflict(self):
        rs = _ruleset_two_overlapping()
        conflicts = []
        for _ in range(3):
            try:
                resolve_with_policy("photo.jpg", rs, STRICT)
            except PolicyConflictError as exc:
                conflicts.append(exc.conflict)
        self.assertEqual(len(conflicts), 3)
        # All conflicts identical
        self.assertEqual(conflicts[0].matching_rules, conflicts[1].matching_rules)
        self.assertEqual(conflicts[1].matching_rules, conflicts[2].matching_rules)

    def test_conflict_matching_rules_order_stable(self):
        """matching_rules tuple must always list Rule_A before Rule_B."""
        rs = _ruleset_two_overlapping(priority_a=0, priority_b=0)
        for _ in range(5):
            result = resolve_with_policy("photo.jpg", rs, SAFE)
            self.assertEqual(result.conflict.matching_rules[0], "Rule_A")
            self.assertEqual(result.conflict.matching_rules[1], "Rule_B")


# ===========================================================================
# 7. PolicyConflict and PolicyMetrics structure
# ===========================================================================

class TestPolicyDataStructures(unittest.TestCase):

    def test_policy_conflict_immutable(self):
        c = PolicyConflict(filename="a.jpg", matching_rules=("R1", "R2"))
        with self.assertRaises(Exception):
            c.filename = "b.jpg"  # type: ignore[misc]

    def test_policy_metrics_immutable(self):
        m = PolicyMetrics(
            mode="safe",
            conflicts_detected=0,
            files_skipped=0,
            overrides_applied=0,
            conflict_details=(),
        )
        with self.assertRaises(Exception):
            m.conflicts_detected = 99  # type: ignore[misc]

    def test_policy_conflict_error_carries_conflict(self):
        c = PolicyConflict("x.jpg", ("R1", "R2"))
        exc = PolicyConflictError("test msg", c)
        self.assertIs(exc.conflict, c)
        self.assertEqual(str(exc), "test msg")

    def test_valid_policy_modes_constant(self):
        self.assertIn(STRICT, VALID_POLICY_MODES)
        self.assertIn(SAFE,   VALID_POLICY_MODES)
        self.assertIn(WARN,   VALID_POLICY_MODES)
        self.assertEqual(len(VALID_POLICY_MODES), 3)

    def test_default_policy_mode_is_safe(self):
        self.assertEqual(DEFAULT_POLICY_MODE, SAFE)


# ===========================================================================
# 8. plan_with_policy
# ===========================================================================

class TestPlanWithPolicy(unittest.TestCase):

    def _scan_root(self) -> Path:
        return Path("/fake/root")

    def test_empty_metadata_returns_empty_actions(self):
        rs = _ruleset_no_overlap()
        result = plan_with_policy((), rs, None, self._scan_root(), SAFE)
        self.assertIsInstance(result, PolicyPlanResult)
        self.assertEqual(result.actions, ())
        self.assertEqual(result.metrics.conflicts_detected, 0)

    def test_no_conflict_all_files_planned(self):
        rs = _ruleset_no_overlap()
        meta = _make_metadata(["photo.jpg", "clip.mp4", "data.xyz"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), SAFE)
        self.assertFalse(result.aborted)
        self.assertEqual(len(result.actions), 3)
        self.assertEqual(result.metrics.files_skipped, 0)
        self.assertEqual(result.metrics.conflicts_detected, 0)

    def test_safe_mode_excludes_conflicting_file(self):
        rs = _ruleset_two_overlapping()
        meta = _make_metadata(["photo.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), SAFE)
        self.assertFalse(result.aborted)
        self.assertEqual(len(result.actions), 0)
        self.assertEqual(result.metrics.files_skipped, 1)
        self.assertEqual(result.metrics.conflicts_detected, 1)

    def test_safe_mode_keeps_non_conflicting_files(self):
        rs = _ruleset_two_overlapping()
        # .jpg conflicts, .mp4 has no match → goes to default
        data = {
            "version": "2.2",
            "rules": [
                {
                    "name": "Rule_A",
                    "match": {"extensions": [".jpg"]},
                    "destination": "DestA",
                },
                {
                    "name": "Rule_B",
                    "match": {"extensions": [".jpg"]},
                    "destination": "DestB",
                },
            ],
            "default_destination": "Others",
        }
        rs2 = parse_rules(data)
        meta = _make_metadata(["photo.jpg", "readme.txt"])
        result = plan_with_policy(meta, rs2, None, self._scan_root(), SAFE)
        # photo.jpg skipped; readme.txt → Others (default, no conflict)
        self.assertEqual(len(result.actions), 1)
        self.assertEqual(result.actions[0].src.name, "readme.txt")
        self.assertEqual(result.metrics.files_skipped, 1)

    def test_warn_mode_includes_conflicting_file(self):
        rs = _ruleset_two_overlapping()
        meta = _make_metadata(["photo.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), WARN)
        self.assertFalse(result.aborted)
        self.assertEqual(len(result.actions), 1)
        self.assertEqual(result.metrics.files_skipped, 0)
        self.assertEqual(result.metrics.overrides_applied, 1)
        self.assertEqual(result.metrics.conflicts_detected, 1)

    def test_strict_mode_aborts_on_conflict(self):
        rs = _ruleset_two_overlapping()
        meta = _make_metadata(["photo.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), STRICT)
        self.assertTrue(result.aborted)
        self.assertEqual(result.actions, ())
        self.assertIsNotNone(result.abort_conflict)
        self.assertIn("photo.jpg", result.abort_detail)

    def test_strict_aborts_on_first_conflict(self):
        """Multiple conflicting files — strict aborts after the first."""
        rs = _ruleset_two_overlapping()
        meta = _make_metadata(["a.jpg", "b.jpg", "c.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), STRICT)
        self.assertTrue(result.aborted)
        self.assertEqual(result.metrics.conflicts_detected, 1)  # first conflict only

    def test_conflict_details_sorted_by_filename(self):
        """conflict_details must be sorted alphabetically by filename."""
        rs = _ruleset_two_overlapping()
        meta = _make_metadata(["z.jpg", "a.jpg", "m.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), SAFE)
        names = [c.filename for c in result.metrics.conflict_details]
        self.assertEqual(names, sorted(names))

    def test_result_is_policy_plan_result_instance(self):
        rs = _ruleset_no_overlap()
        meta = _make_metadata(["photo.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), SAFE)
        self.assertIsInstance(result, PolicyPlanResult)
        self.assertIsInstance(result.metrics, PolicyMetrics)

    def test_actions_sorted_by_filename(self):
        rs = _ruleset_no_overlap()
        meta = _make_metadata(["z.jpg", "a.jpg", "m.jpg"])
        result = plan_with_policy(meta, rs, None, self._scan_root(), SAFE)
        src_names = [a.src.name for a in result.actions]
        self.assertEqual(src_names, sorted(src_names))


# ===========================================================================
# 9. BatchEngine integration (using real temp dirs)
# ===========================================================================

class TestBatchEnginePolicy(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._root = Path(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _make_files(self, names: list[str]) -> list[Path]:
        paths = []
        for name in names:
            p = self._root / name
            p.write_text("x")
            paths.append(p)
        return paths

    def _conflicting_rules_data(self) -> dict:
        return {
            "version": "2.2",
            "rules": [
                {
                    "name": "Rule_A",
                    "match": {"extensions": [".jpg"]},
                    "destination": "DestA",
                },
                {
                    "name": "Rule_B",
                    "match": {"extensions": [".jpg"]},
                    "destination": "DestB",
                },
            ],
            "default_destination": "Others",
        }

    def test_policy_metrics_present_on_report(self):
        from batch_engine import BatchEngine
        rs = _ruleset_no_overlap()
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=SAFE)
        self.assertIsNotNone(report.policy_metrics)

    def test_safe_mode_skips_conflict_file(self):
        from batch_engine import BatchEngine
        rs = parse_rules(self._conflicting_rules_data())
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=SAFE)
        self.assertEqual(report.policy_metrics.files_skipped, 1)
        self.assertEqual(report.total_planned, 0)

    def test_warn_mode_processes_conflict_file(self):
        from batch_engine import BatchEngine
        rs = parse_rules(self._conflicting_rules_data())
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=WARN)
        self.assertEqual(report.policy_metrics.overrides_applied, 1)
        self.assertEqual(report.total_planned, 1)

    def test_strict_mode_aborts_with_fail_index(self):
        from batch_engine import BatchEngine
        rs = parse_rules(self._conflicting_rules_data())
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=STRICT)
        self.assertIsNotNone(report.fail_index)
        self.assertEqual(report.total_planned, 0)
        self.assertIsNotNone(report.policy_metrics)

    def test_default_policy_mode_is_safe(self):
        """run_from_files() default policy_mode must be 'safe'."""
        from batch_engine import BatchEngine
        rs = parse_rules(self._conflicting_rules_data())
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        # Call without policy_mode keyword
        report = engine.run_from_files(files, rs, None, dry_run=True)
        # With safe mode the conflict file is skipped
        self.assertEqual(report.policy_metrics.files_skipped, 1)

    def test_no_conflict_metrics_zero(self):
        from batch_engine import BatchEngine
        rs = _ruleset_no_overlap()
        files = self._make_files(["photo.jpg", "clip.mp4"])
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=SAFE)
        self.assertEqual(report.policy_metrics.conflicts_detected, 0)
        self.assertEqual(report.policy_metrics.files_skipped, 0)
        self.assertEqual(report.policy_metrics.overrides_applied, 0)

    def test_policy_metrics_mode_reflects_input(self):
        from batch_engine import BatchEngine
        rs = _ruleset_no_overlap()
        files = self._make_files(["photo.jpg"])
        engine = BatchEngine(scan_root=self._root)
        for mode in (STRICT, SAFE, WARN):
            report = engine.run_from_files(files, rs, None, dry_run=True, policy_mode=mode)
            self.assertEqual(report.policy_metrics.mode, mode, msg=f"mode={mode}")

    def test_empty_files_list_returns_policy_metrics(self):
        from batch_engine import BatchEngine
        rs = _ruleset_no_overlap()
        engine = BatchEngine(scan_root=self._root)
        report = engine.run_from_files([], rs, None, dry_run=True, policy_mode=SAFE)
        self.assertIsNotNone(report.policy_metrics)
        self.assertEqual(report.policy_metrics.conflicts_detected, 0)


# ===========================================================================
# 10. Config — policy_mode field
# ===========================================================================

class TestConfigPolicyMode(unittest.TestCase):

    def test_default_policy_mode_is_safe(self):
        cfg = parse_config(_base_config())
        self.assertEqual(cfg.policy_mode, "safe")

    def test_valid_strict(self):
        cfg = parse_config(_base_config(policy_mode="strict"))
        self.assertEqual(cfg.policy_mode, "strict")

    def test_valid_safe(self):
        cfg = parse_config(_base_config(policy_mode="safe"))
        self.assertEqual(cfg.policy_mode, "safe")

    def test_valid_warn(self):
        cfg = parse_config(_base_config(policy_mode="warn"))
        self.assertEqual(cfg.policy_mode, "warn")

    def test_invalid_policy_mode_raises_config_error(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base_config(policy_mode="aggressive"))
        self.assertIn("policy_mode", str(ctx.exception).lower())

    def test_invalid_policy_mode_message_includes_valid_choices(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base_config(policy_mode="invalid"))
        msg = str(ctx.exception)
        self.assertIn("safe", msg)
        self.assertIn("strict", msg)
        self.assertIn("warn", msg)

    def test_config_is_immutable(self):
        cfg = parse_config(_base_config())
        with self.assertRaises(Exception):
            cfg.policy_mode = "strict"  # type: ignore[misc]

    def test_policy_mode_absent_uses_default(self):
        """Config with no policy_mode key should default to 'safe'."""
        data = _base_config()
        data.pop("policy_mode", None)
        cfg = parse_config(data)
        self.assertEqual(cfg.policy_mode, "safe")


# ===========================================================================
# 11. CLI — --policy flag (argument parsing layer)
# ===========================================================================

class TestCLIPolicyFlag(unittest.TestCase):

    def _parse(self, argv: list[str]):
        from main import build_parser
        return build_parser().parse_args(argv)

    def test_policy_strict_parsed(self):
        args = self._parse(["organize", "/tmp", "--policy", "strict"])
        self.assertEqual(args.policy, "strict")

    def test_policy_safe_parsed(self):
        args = self._parse(["organize", "/tmp", "--policy", "safe"])
        self.assertEqual(args.policy, "safe")

    def test_policy_warn_parsed(self):
        args = self._parse(["organize", "/tmp", "--policy", "warn"])
        self.assertEqual(args.policy, "warn")

    def test_policy_default_none(self):
        """Without --policy flag, args.policy must be None (so config wins)."""
        args = self._parse(["organize", "/tmp"])
        self.assertIsNone(args.policy)

    def test_invalid_policy_rejected(self):
        """argparse must reject values outside {strict, safe, warn}."""
        import argparse
        with self.assertRaises(SystemExit) as ctx:
            self._parse(["organize", "/tmp", "--policy", "yolo"])
        self.assertEqual(ctx.exception.code, 2)


# ===========================================================================
# 12. Backward compatibility — existing tests must not break
# ===========================================================================

class TestBackwardCompat(unittest.TestCase):
    """
    Ensure that v2.8 additions don't break the existing public API surface.
    """

    def test_resolve_destination_unchanged(self):
        """The v2.2 resolve_destination still returns a single string."""
        rs = _ruleset_no_overlap()
        dest = resolve_destination("photo.jpg", rs, 0)
        self.assertIsInstance(dest, str)
        self.assertEqual(dest, "Images")

    def test_parse_rules_unchanged(self):
        """parse_rules still returns a RuleSet."""
        rs = _ruleset_no_overlap()
        self.assertIsInstance(rs, RuleSet)

    def test_run_from_files_no_policy_mode_kwarg(self):
        """BatchEngine.run_from_files without policy_mode still works."""
        import tempfile, shutil
        from batch_engine import BatchEngine
        tmp = Path(tempfile.mkdtemp())
        try:
            (tmp / "x.jpg").write_text("x")
            files = [tmp / "x.jpg"]
            rs = _ruleset_no_overlap()
            engine = BatchEngine(scan_root=tmp)
            report = engine.run_from_files(files, rs, None, dry_run=True)
            # No exception — backward compat preserved
            self.assertIsNotNone(report)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ===========================================================================
# 13. Edge cases
# ===========================================================================

class TestPolicyEdgeCases(unittest.TestCase):

    def test_file_matching_no_rules_goes_to_default(self):
        """File with no matching rule always goes to default_destination."""
        rs = _ruleset_no_overlap()
        for mode in (STRICT, SAFE, WARN):
            with self.subTest(mode=mode):
                result = resolve_with_policy("mystery.xyz", rs, mode)
                self.assertEqual(result.destination, "Others")
                self.assertFalse(result.skipped)

    def test_three_overlapping_rules_conflict_lists_all(self):
        data = {
            "version": "2.2",
            "rules": [
                {"name": "R1", "match": {"extensions": [".jpg"]}, "destination": "D1"},
                {"name": "R2", "match": {"extensions": [".jpg"]}, "destination": "D2"},
                {"name": "R3", "match": {"extensions": [".jpg"]}, "destination": "D3"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        result = resolve_with_policy("photo.jpg", rs, SAFE)
        self.assertEqual(len(result.conflict.matching_rules), 3)

    def test_warn_mode_with_three_overlapping_uses_first(self):
        data = {
            "version": "2.2",
            "rules": [
                {"name": "R1", "priority": 1, "match": {"extensions": [".jpg"]}, "destination": "D1"},
                {"name": "R2", "priority": 0, "match": {"extensions": [".jpg"]}, "destination": "D2"},
                {"name": "R3", "priority": 2, "match": {"extensions": [".jpg"]}, "destination": "D3"},
            ],
            "default_destination": "Others",
        }
        rs = parse_rules(data)
        # R2 has lowest priority value (0) → sorted first → wins in warn mode
        result = resolve_with_policy("photo.jpg", rs, WARN)
        self.assertEqual(result.destination, "D2")

    def test_policy_conflict_error_is_exception(self):
        """PolicyConflictError must be catchable as Exception."""
        rs = _ruleset_two_overlapping()
        with self.assertRaises(Exception):
            resolve_with_policy("photo.jpg", rs, STRICT)

    def test_policy_result_fields_all_present(self):
        r = PolicyResult()
        self.assertIsNone(r.destination)
        self.assertFalse(r.skipped)
        self.assertFalse(r.override)
        self.assertIsNone(r.conflict)


if __name__ == "__main__":
    unittest.main(verbosity=2)
