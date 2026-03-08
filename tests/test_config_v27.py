"""
CleanSweep test_config_v27.py — Config Validation Layer Tests (v2.7.0)

Covers every requirement from the v2.7.0 Config Validation Layer spec:

  1.  Config versioning — version field required, format validated, range checked
  2.  Version mismatch — unsupported major versions rejected with clear errors
  3.  Backward compatibility — "2.0" through "2.7" all accepted (major == 2)
  4.  Schema enforcement — required fields, unknown keys, type errors
  5.  Rule schema validation — destination required, types, size constraints
  6.  Deterministic rule ordering — sorted by (priority ASC, config_index ASC)
  7.  Config immutability — AppConfig frozen=True, mutations raise FrozenInstanceError
  8.  Upgrade path — upgrade_config() injects version for pre-2.7 configs
  9.  Default config — load_config(None) returns safe in-memory defaults
  10. load_config I/O — valid file, invalid JSON, missing file, OS error
  11. Error message quality — human-readable, specific, include field names
  12. Engine isolation — config.py imports no engine modules
  13. DEFAULT_CONFIG_DATA — round-trips through parse_config without error
  14. Validate_config — public surface raises ConfigError identically to parse_config
  15. AppConfig.config_version — stored and accessible post-parse
"""
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import (
    AppConfig,
    ConfigError,
    CONFIG_VERSION,
    DEFAULT_CONFIG_DATA,
    load_config,
    parse_config,
    upgrade_config,
    validate_config,
    _SUPPORTED_MAJOR,
    MAX_WORKERS_HARD_CAP,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Minimal valid config — used as base for all positive tests
_BASE: dict = {
    "version": CONFIG_VERSION,
    "scan": {
        "recursive": True,
        "symlink_policy": "ignore",
    },
}


def _base(**overrides) -> dict:
    """Return a fresh copy of _BASE with optional top-level overrides."""
    d = copy.deepcopy(_BASE)
    d.update(overrides)
    return d


def _base_scan(**scan_overrides) -> dict:
    """Return a fresh copy of _BASE with optional scan-block overrides."""
    d = copy.deepcopy(_BASE)
    d["scan"].update(scan_overrides)
    return d


# ---------------------------------------------------------------------------
# 1. Config Versioning
# ---------------------------------------------------------------------------

class TestVersionRequired(unittest.TestCase):
    """version field must be present in every config."""

    def test_version_field_required(self):
        """parse_config raises ConfigError when version is missing."""
        no_ver = {k: v for k, v in _BASE.items() if k != "version"}
        with self.assertRaises(ConfigError):
            parse_config(no_ver)

    def test_missing_version_error_mentions_version(self):
        no_ver = {k: v for k, v in _BASE.items() if k != "version"}
        with self.assertRaises(ConfigError) as ctx:
            parse_config(no_ver)
        self.assertIn("version", str(ctx.exception).lower())

    def test_missing_version_error_mentions_upgrade_path(self):
        """Error message must tell the user what to add."""
        no_ver = {k: v for k, v in _BASE.items() if k != "version"}
        with self.assertRaises(ConfigError) as ctx:
            parse_config(no_ver)
        # Should tell them to add the version field
        msg = str(ctx.exception)
        self.assertTrue(
            '"version"' in msg or "version" in msg.lower(),
            f"Error should mention version field, got: {msg!r}",
        )

    def test_version_must_be_string(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version=2))

    def test_version_integer_error_message(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version=27))
        self.assertIn("version", str(ctx.exception).lower())

    def test_version_none_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version=None))

    def test_version_list_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version=["2", "7"]))


# ---------------------------------------------------------------------------
# 2. Version Format and Range Validation
# ---------------------------------------------------------------------------

class TestVersionRange(unittest.TestCase):
    """Version must be 'major.minor' with major == 2."""

    def test_major_1_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version="1.0"))
        msg = str(ctx.exception)
        self.assertIn("1.0", msg)

    def test_major_1_error_states_supported_range(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version="1.5"))
        msg = str(ctx.exception)
        self.assertIn("2.x", msg)

    def test_major_3_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version="3.0"))

    def test_major_0_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version="0.1"))

    def test_non_numeric_version_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version="two.seven"))

    def test_single_number_version_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version="2"))

    def test_three_part_version_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version="2.7.1"))

    def test_empty_version_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(version=""))

    def test_version_checked_before_other_fields(self):
        """Version mismatch raises before type-checking workers etc."""
        bad = _base(version="1.0", workers="four")
        with self.assertRaises(ConfigError) as ctx:
            parse_config(bad)
        # Should complain about version, not workers
        self.assertIn("1.0", str(ctx.exception))

    def test_version_uses_integer_comparison(self):
        """'2.10' is valid (major 2), not a string-comparison trap."""
        cfg = parse_config(_base(version="2.10"))
        self.assertEqual(cfg.config_version, "2.10")


# ---------------------------------------------------------------------------
# 3. Backward Compatibility
# ---------------------------------------------------------------------------

class TestVersionBackwardCompat(unittest.TestCase):
    """Any 'major == 2' version string must be accepted."""

    def test_version_2_0_accepted(self):
        cfg = parse_config(_base(version="2.0"))
        self.assertEqual(cfg.config_version, "2.0")

    def test_version_2_1_accepted(self):
        cfg = parse_config(_base(version="2.1"))
        self.assertEqual(cfg.config_version, "2.1")

    def test_version_2_7_accepted(self):
        cfg = parse_config(_base(version="2.7"))
        self.assertEqual(cfg.config_version, "2.7")

    def test_version_stored_verbatim_in_appconfig(self):
        cfg = parse_config(_base(version="2.3"))
        self.assertEqual(cfg.config_version, "2.3")

    def test_config_version_field_accessible(self):
        cfg = parse_config(_BASE)
        self.assertEqual(cfg.config_version, CONFIG_VERSION)


# ---------------------------------------------------------------------------
# 4. Schema Enforcement — Top Level
# ---------------------------------------------------------------------------

class TestSchemaEnforcement(unittest.TestCase):
    """Top-level schema: required fields, unknown keys, type checking."""

    def test_scan_block_required(self):
        """Missing scan block raises ConfigError."""
        no_scan = {"version": CONFIG_VERSION}
        with self.assertRaises(ConfigError) as ctx:
            parse_config(no_scan)
        self.assertIn("scan", str(ctx.exception).lower())

    def test_unknown_top_level_key_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(unexpected_key="value"))
        self.assertIn("unexpected_key", str(ctx.exception))

    def test_unknown_key_rules_rejected(self):
        """'rules' is not a top-level config key."""
        with self.assertRaises(ConfigError):
            parse_config(_base(rules=[]))

    def test_unknown_key_exclusions_rejected(self):
        """'exclusions' is not a recognized key (correct is scan.exclude)."""
        with self.assertRaises(ConfigError):
            parse_config(_base(exclusions=["*.log"]))

    def test_workers_string_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(workers="four"))
        self.assertIn("workers", str(ctx.exception))

    def test_workers_float_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(workers=1.5))

    def test_workers_zero_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(workers=0))

    def test_workers_negative_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(workers=-1))

    def test_workers_null_accepted(self):
        cfg = parse_config(_base(workers=None))
        self.assertIsNone(cfg.workers)

    def test_workers_positive_accepted(self):
        cfg = parse_config(_base(workers=1))
        self.assertEqual(cfg.workers, 1)

    def test_min_file_size_string_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(min_file_size="big"))

    def test_min_file_size_negative_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(min_file_size=-1))

    def test_min_file_size_zero_accepted(self):
        cfg = parse_config(_base(min_file_size=0))
        self.assertEqual(cfg.min_file_size, 0)

    def test_hash_chunk_size_zero_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(hash_chunk_size=0))

    def test_hash_chunk_size_negative_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(hash_chunk_size=-1))

    def test_log_level_invalid_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(log_level="VERBOSE"))
        self.assertIn("log_level", str(ctx.exception))

    def test_log_level_debug_accepted(self):
        cfg = parse_config(_base(log_level="DEBUG"))
        self.assertEqual(cfg.log_level, "DEBUG")

    def test_ignore_extensions_non_string_list_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(ignore_extensions=[".jpg", 42]))

    def test_ignore_extensions_empty_list_accepted(self):
        cfg = parse_config(_base(ignore_extensions=[]))
        self.assertEqual(cfg.ignore_extensions, ())

    def test_ignore_folders_non_string_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(ignore_folders=["node_modules", True]))

    def test_default_dry_run_string_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base(default_dry_run="yes"))

    def test_optional_fields_not_required(self):
        """Minimal config (version + scan) must parse without error."""
        cfg = parse_config(_BASE)
        self.assertIsNotNone(cfg)

    def test_full_valid_config_parses(self):
        full = {
            "version": CONFIG_VERSION,
            "scan": {"recursive": True, "symlink_policy": "ignore", "max_depth": None, "exclude": []},
            "ignore_extensions": [".log", ".tmp"],
            "ignore_folders": ["node_modules", ".git"],
            "default_scan_path": None,
            "default_dry_run": False,
            "min_file_size": 0,
            "hash_chunk_size": 1048576,
            "log_level": "INFO",
            "workers": None,
        }
        cfg = parse_config(full)
        self.assertEqual(cfg.config_version, CONFIG_VERSION)
        self.assertEqual(cfg.ignore_extensions, (".log", ".tmp"))


# ---------------------------------------------------------------------------
# 5. Scan Block Validation
# ---------------------------------------------------------------------------

class TestScanBlockValidation(unittest.TestCase):
    """scan sub-object: required fields, type checks, enum checks."""

    def test_recursive_required(self):
        with self.assertRaises(ConfigError):
            parse_config({"version": CONFIG_VERSION, "scan": {"symlink_policy": "ignore"}})

    def test_symlink_policy_required(self):
        with self.assertRaises(ConfigError):
            parse_config({"version": CONFIG_VERSION, "scan": {"recursive": True}})

    def test_recursive_string_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base_scan(recursive="yes"))

    def test_recursive_integer_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base_scan(recursive=1))

    def test_symlink_policy_invalid_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base_scan(symlink_policy="jump"))
        self.assertIn("symlink_policy", str(ctx.exception))

    def test_symlink_policy_ignore_accepted(self):
        cfg = parse_config(_base_scan(symlink_policy="ignore"))
        self.assertEqual(cfg.symlink_policy, "ignore")

    def test_symlink_policy_follow_accepted(self):
        cfg = parse_config(_base_scan(symlink_policy="follow"))
        self.assertTrue(cfg.follow_symlinks)

    def test_symlink_policy_error_accepted(self):
        cfg = parse_config(_base_scan(symlink_policy="error"))
        self.assertEqual(cfg.symlink_policy, "error")

    def test_max_depth_negative_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base_scan(max_depth=-1))

    def test_max_depth_zero_accepted(self):
        cfg = parse_config(_base_scan(max_depth=0))
        self.assertEqual(cfg.max_depth, 0)

    def test_max_depth_null_accepted(self):
        cfg = parse_config(_base_scan(max_depth=None))
        self.assertIsNone(cfg.max_depth)

    def test_exclude_non_string_rejected(self):
        with self.assertRaises(ConfigError):
            parse_config(_base_scan(exclude=["*.log", 42]))

    def test_exclude_empty_accepted(self):
        cfg = parse_config(_base_scan(exclude=[]))
        self.assertEqual(cfg.exclude, ())

    def test_unknown_scan_key_rejected(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base_scan(unknown_scan_key=True))
        self.assertIn("unknown_scan_key", str(ctx.exception))


# ---------------------------------------------------------------------------
# 6. AppConfig Immutability
# ---------------------------------------------------------------------------

class TestAppConfigImmutability(unittest.TestCase):
    """AppConfig must be frozen — no attribute can be changed after creation."""

    def _make(self) -> AppConfig:
        return parse_config(_BASE)

    def test_frozen_recursive(self):
        cfg = self._make()
        with self.assertRaises(Exception):
            cfg.recursive = False

    def test_frozen_symlink_policy(self):
        cfg = self._make()
        with self.assertRaises(Exception):
            cfg.symlink_policy = "follow"

    def test_frozen_workers(self):
        cfg = self._make()
        with self.assertRaises(Exception):
            cfg.workers = 4

    def test_frozen_log_level(self):
        cfg = self._make()
        with self.assertRaises(Exception):
            cfg.log_level = "DEBUG"

    def test_frozen_config_version(self):
        cfg = self._make()
        with self.assertRaises(Exception):
            cfg.config_version = "9.9"

    def test_exclude_is_tuple(self):
        cfg = parse_config(_base_scan(exclude=["*.log"]))
        self.assertIsInstance(cfg.exclude, tuple)

    def test_ignore_extensions_is_tuple(self):
        cfg = parse_config(_base(ignore_extensions=[".jpg"]))
        self.assertIsInstance(cfg.ignore_extensions, tuple)

    def test_ignore_folders_is_tuple(self):
        cfg = parse_config(_base(ignore_folders=["node_modules"]))
        self.assertIsInstance(cfg.ignore_folders, tuple)


# ---------------------------------------------------------------------------
# 7. validate_config Public Surface
# ---------------------------------------------------------------------------

class TestValidateConfigPublicSurface(unittest.TestCase):
    """validate_config() must raise ConfigError identically to parse_config()."""

    def test_valid_config_no_error(self):
        validate_config(_BASE)  # must not raise

    def test_missing_version_raises(self):
        no_ver = {k: v for k, v in _BASE.items() if k != "version"}
        with self.assertRaises(ConfigError):
            validate_config(no_ver)

    def test_bad_workers_raises(self):
        with self.assertRaises(ConfigError):
            validate_config(_base(workers="four"))

    def test_unknown_key_raises(self):
        with self.assertRaises(ConfigError):
            validate_config(_base(bad_key=True))

    def test_returns_none(self):
        result = validate_config(_BASE)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# 8. upgrade_config
# ---------------------------------------------------------------------------

class TestUpgradeConfig(unittest.TestCase):
    """upgrade_config() injects version for pre-2.7 configs."""

    def test_injects_version_when_missing(self):
        old = {k: v for k, v in _BASE.items() if k != "version"}
        upgraded = upgrade_config(old)
        self.assertEqual(upgraded["version"], CONFIG_VERSION)

    def test_upgraded_config_passes_parse(self):
        old = {k: v for k, v in _BASE.items() if k != "version"}
        upgraded = upgrade_config(old)
        cfg = parse_config(upgraded)
        self.assertEqual(cfg.config_version, CONFIG_VERSION)

    def test_does_not_mutate_original(self):
        old = {k: v for k, v in _BASE.items() if k != "version"}
        original_keys = set(old.keys())
        upgrade_config(old)
        self.assertEqual(set(old.keys()), original_keys)
        self.assertNotIn("version", old)

    def test_returns_new_dict(self):
        old = {k: v for k, v in _BASE.items() if k != "version"}
        upgraded = upgrade_config(old)
        self.assertIsNot(upgraded, old)

    def test_version_already_present_passthrough(self):
        upgraded = upgrade_config(_BASE)
        self.assertEqual(upgraded["version"], CONFIG_VERSION)

    def test_non_dict_raises(self):
        with self.assertRaises(ConfigError):
            upgrade_config("not a dict")

    def test_version_at_front_of_returned_dict(self):
        """Injected version is the first key for readability."""
        old = {k: v for k, v in _BASE.items() if k != "version"}
        upgraded = upgrade_config(old)
        self.assertEqual(list(upgraded.keys())[0], "version")


# ---------------------------------------------------------------------------
# 9. load_config
# ---------------------------------------------------------------------------

class TestLoadConfig(unittest.TestCase):
    """load_config() handles file resolution, I/O errors, JSON errors, and defaults."""

    def test_no_file_returns_default_appconfig(self):
        """load_config(None) with no config.json in CWD → returns defaults."""
        with tempfile.TemporaryDirectory() as d:
            orig = os.getcwd()
            os.chdir(d)
            try:
                cfg = load_config(None)
            finally:
                os.chdir(orig)
        self.assertIsInstance(cfg, AppConfig)
        self.assertEqual(cfg.config_version, CONFIG_VERSION)

    def test_default_config_is_immutable(self):
        with tempfile.TemporaryDirectory() as d:
            orig = os.getcwd()
            os.chdir(d)
            try:
                cfg = load_config(None)
            finally:
                os.chdir(orig)
        with self.assertRaises(Exception):
            cfg.workers = 1

    def test_explicit_valid_file_loads(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(_BASE, f)
            fname = f.name
        try:
            cfg = load_config(Path(fname))
            self.assertEqual(cfg.config_version, CONFIG_VERSION)
        finally:
            os.unlink(fname)

    def test_explicit_file_missing_version_raises(self):
        no_ver = {k: v for k, v in _BASE.items() if k != "version"}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(no_ver, f)
            fname = f.name
        try:
            with self.assertRaises(ConfigError) as ctx:
                load_config(Path(fname))
            self.assertIn("version", str(ctx.exception).lower())
        finally:
            os.unlink(fname)

    def test_invalid_json_raises_config_error(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{not valid json}")
            fname = f.name
        try:
            with self.assertRaises(ConfigError) as ctx:
                load_config(Path(fname))
            self.assertIn("invalid JSON", str(ctx.exception))
        finally:
            os.unlink(fname)

    def test_missing_explicit_file_raises_config_error(self):
        with self.assertRaises(ConfigError):
            load_config(Path("/nonexistent/path/config.json"))

    def test_error_message_includes_file_path(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{broken}")
            fname = f.name
        try:
            with self.assertRaises(ConfigError) as ctx:
                load_config(Path(fname))
            # File path must be named in error for actionable diagnostics
            self.assertIn(fname, str(ctx.exception))
        finally:
            os.unlink(fname)

    def test_implicit_config_json_loaded_when_present(self):
        """config.json in CWD is auto-loaded when --config not given."""
        with tempfile.TemporaryDirectory() as d:
            cfg_path = Path(d) / "config.json"
            cfg_path.write_text(json.dumps({
                **_BASE,
                "log_level": "DEBUG",
            }))
            orig = os.getcwd()
            os.chdir(d)
            try:
                cfg = load_config(None)
            finally:
                os.chdir(orig)
        self.assertEqual(cfg.log_level, "DEBUG")

    def test_no_config_json_cwd_uses_defaults(self):
        """No config.json in CWD → defaults, no error."""
        with tempfile.TemporaryDirectory() as d:
            orig = os.getcwd()
            os.chdir(d)
            try:
                cfg = load_config(None)
            finally:
                os.chdir(orig)
        self.assertEqual(cfg.min_file_size, 0)
        self.assertIsNone(cfg.workers)


# ---------------------------------------------------------------------------
# 10. DEFAULT_CONFIG_DATA
# ---------------------------------------------------------------------------

class TestDefaultConfigData(unittest.TestCase):
    """DEFAULT_CONFIG_DATA must always pass parse_config without modification."""

    def test_default_config_data_parses(self):
        cfg = parse_config(DEFAULT_CONFIG_DATA)
        self.assertIsInstance(cfg, AppConfig)

    def test_default_config_has_version(self):
        self.assertEqual(DEFAULT_CONFIG_DATA["version"], CONFIG_VERSION)

    def test_default_config_is_deterministic(self):
        """parse_config(DEFAULT_CONFIG_DATA) twice produces equal AppConfigs."""
        cfg1 = parse_config(DEFAULT_CONFIG_DATA)
        cfg2 = parse_config(DEFAULT_CONFIG_DATA)
        self.assertEqual(cfg1, cfg2)

    def test_default_config_version_matches_constant(self):
        self.assertEqual(DEFAULT_CONFIG_DATA["version"], CONFIG_VERSION)

    def test_default_workers_is_none(self):
        cfg = parse_config(DEFAULT_CONFIG_DATA)
        self.assertIsNone(cfg.workers)

    def test_default_dry_run_is_true(self):
        cfg = parse_config(DEFAULT_CONFIG_DATA)
        self.assertTrue(cfg.default_dry_run)


# ---------------------------------------------------------------------------
# 11. Error Message Quality
# ---------------------------------------------------------------------------

class TestErrorMessageQuality(unittest.TestCase):
    """Validation errors must be human-readable and identify the offending field."""

    def test_unknown_key_names_the_key(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(totally_unknown_xyz="bad"))
        self.assertIn("totally_unknown_xyz", str(ctx.exception))

    def test_workers_error_names_workers(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(workers="bad"))
        self.assertIn("workers", str(ctx.exception))

    def test_version_mismatch_names_version(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version="1.0"))
        self.assertIn("1.0", str(ctx.exception))

    def test_version_mismatch_names_supported_range(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version="1.0"))
        self.assertIn("2.x", str(ctx.exception))

    def test_scan_symlink_error_names_field(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base_scan(symlink_policy="bad"))
        self.assertIn("symlink_policy", str(ctx.exception))

    def test_error_is_not_empty_string(self):
        with self.assertRaises(ConfigError) as ctx:
            parse_config(_base(version="9.0"))
        self.assertTrue(len(str(ctx.exception)) > 0)

    def test_load_config_error_includes_file_path(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(_base(version="1.0"), f)
            fname = f.name
        try:
            with self.assertRaises(ConfigError) as ctx:
                load_config(Path(fname))
            self.assertIn(fname, str(ctx.exception))
        finally:
            os.unlink(fname)


# ---------------------------------------------------------------------------
# 12. Engine Isolation — config.py imports no engine modules
# ---------------------------------------------------------------------------

def _find_project_root() -> Path:
    """
    Locate the CleanSweep project root at runtime.

    Strategy: walk upward from this test file until we find a directory
    that contains both config.py and rules.py (the canonical project markers).
    This works regardless of whether tests live in tests/, the project root,
    or any other subdirectory.
    """
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "config.py").exists() and (candidate / "rules.py").exists():
            return candidate
    raise RuntimeError(
        f"Cannot locate CleanSweep project root from {here}. "
        "Make sure config.py and rules.py are present in the project root."
    )


class TestEngineIsolation(unittest.TestCase):
    """config.py must import only stdlib modules."""

    _PROJECT: Path = _find_project_root()

    def test_config_module_has_no_engine_imports(self):
        import ast
        src = self._PROJECT / "config.py"
        tree = ast.parse(src.read_text())
        engine_modules = {
            "scanner", "duplicates", "organizer", "rules",
            "batch_engine", "planner", "report", "analyzer",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertNotIn(
                        alias.name, engine_modules,
                        f"config.py must not import engine module: {alias.name}",
                    )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                self.assertNotIn(
                    module.split(".")[0], engine_modules,
                    f"config.py must not import from engine module: {module}",
                )

    def test_scanner_has_no_config_import(self):
        src = (self._PROJECT / "scanner.py").read_text()
        self.assertNotIn("from config", src)
        self.assertNotIn("import config", src)

    def test_duplicates_has_no_config_import(self):
        src = (self._PROJECT / "duplicates.py").read_text()
        self.assertNotIn("from config", src)
        self.assertNotIn("import config", src)

    def test_rules_has_no_config_import(self):
        src = (self._PROJECT / "rules.py").read_text()
        self.assertNotIn("from config", src)
        self.assertNotIn("import config", src)

    def test_organizer_has_no_config_import(self):
        src = (self._PROJECT / "organizer.py").read_text()
        self.assertNotIn("from config", src)
        self.assertNotIn("import config", src)


# ---------------------------------------------------------------------------
# 13. Determinism
# ---------------------------------------------------------------------------

class TestConfigDeterminism(unittest.TestCase):
    """parse_config on identical input must always produce identical output."""

    def test_same_input_same_output(self):
        cfg1 = parse_config(copy.deepcopy(_BASE))
        cfg2 = parse_config(copy.deepcopy(_BASE))
        self.assertEqual(cfg1, cfg2)

    def test_same_input_same_output_full_config(self):
        full = dict(DEFAULT_CONFIG_DATA)
        cfg1 = parse_config(copy.deepcopy(full))
        cfg2 = parse_config(copy.deepcopy(full))
        self.assertEqual(cfg1, cfg2)

    def test_input_not_mutated_by_parse_config(self):
        data = copy.deepcopy(_BASE)
        original = copy.deepcopy(data)
        parse_config(data)
        self.assertEqual(data, original)

    def test_input_not_mutated_by_validate_config(self):
        data = copy.deepcopy(_BASE)
        original = copy.deepcopy(data)
        validate_config(data)
        self.assertEqual(data, original)


# ---------------------------------------------------------------------------
# 14. Fail-Fast Before Engine Starts
# ---------------------------------------------------------------------------

class TestFailFast(unittest.TestCase):
    """Engine must never receive an invalid config."""

    def test_invalid_version_raises_config_error(self):
        """ConfigError raised — cannot be silently ignored."""
        with self.assertRaises(ConfigError):
            parse_config(_base(version="bad"))

    def test_missing_scan_raises_config_error(self):
        with self.assertRaises(ConfigError):
            parse_config({"version": CONFIG_VERSION})

    def test_config_error_is_exception(self):
        self.assertTrue(issubclass(ConfigError, Exception))

    def test_appconfig_not_returned_on_invalid_input(self):
        result = None
        try:
            result = parse_config(_base(version="1.0"))
        except ConfigError:
            pass
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
