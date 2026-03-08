"""
v1.2.0 — Observability test suite.

Validates that logging and timing are purely side-channel:
- JSON export byte-identical across all verbosity modes
- Timing shown only in verbose/debug
- Quiet mode suppresses INFO/DEBUG
- Phase order stable
- No timing data in JSON
"""

import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logger
import timer
from duplicates import scan_duplicates, export_json


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dataset(base: Path) -> None:
    content = b"shared duplicate content"
    for i in range(3):
        (base / f"dup_{i}.txt").write_bytes(content)
    (base / "unique.txt").write_bytes(b"i am unique xyz")


def _run_and_export(data_dir: Path, out_path: Path, log_level: str) -> bytes:
    """Run scan at a given log level, export JSON, return bytes."""
    logger.set_log_level(log_level)
    r = scan_duplicates(data_dir, keep="first")
    export_json(r["duplicates"], out_path)
    return out_path.read_bytes()


# ---------------------------------------------------------------------------
# Test 1 — Output Stability: JSON identical across all verbosity modes
# ---------------------------------------------------------------------------

class TestOutputStability(unittest.TestCase):
    def test_json_identical_across_all_verbosity_modes(self):
        """JSON export must be byte-identical under ERROR, WARN, INFO, DEBUG."""
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()
            _make_dataset(data)

            outputs = {}
            for level in ("ERROR", "WARN", "INFO", "DEBUG"):
                out = out_dir / f"{level}.json"
                outputs[level] = _run_and_export(data, out, level)

            reference = outputs["INFO"]
            for level, content in outputs.items():
                self.assertEqual(
                    reference, content,
                    f"JSON differs between INFO and {level} mode"
                )

        # Restore default
        logger.set_log_level("INFO")


# ---------------------------------------------------------------------------
# Test 2 — Timing Presence: timings populated after scan
# ---------------------------------------------------------------------------

class TestTimingPresence(unittest.TestCase):
    def test_timings_populated_after_scan(self):
        """After a scan, get_timings() must return non-empty dict."""
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            _make_dataset(data)
            timer.reset()
            scan_duplicates(data)
            timings = timer.get_timings()
            self.assertGreater(len(timings), 0)
            self.assertIn("total", timings)

    def test_timings_in_phase_order(self):
        """get_timings() keys must appear in PHASE_ORDER, never arbitrary order."""
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            _make_dataset(data)
            timer.reset()
            scan_duplicates(data)
            timings = timer.get_timings()
            keys = list(timings.keys())
            # Verify order matches PHASE_ORDER
            phase_order = [p for p in timer.PHASE_ORDER if p in timings]
            self.assertEqual(keys, phase_order)


# ---------------------------------------------------------------------------
# Test 3 — Quiet Mode: only errors emitted
# ---------------------------------------------------------------------------

class TestQuietMode(unittest.TestCase):
    def test_quiet_suppresses_info_and_debug(self):
        """In ERROR mode, INFO and DEBUG messages must not be printed."""
        logger.set_log_level("ERROR")
        captured = StringIO()
        original_print = __builtins__.__dict__["print"] if hasattr(__builtins__, "__dict__") else None

        # Patch print inside logger module
        import unittest.mock as mock
        with mock.patch("builtins.print") as mock_print:
            logger.log_info("this should not appear")
            logger.log_debug("this should not appear either")
            logger.log_error("this should appear")

        calls = mock_print.call_args_list
        printed = [str(c) for c in calls]

        info_printed  = any("this should not appear" in s for s in printed)
        error_printed = any("this should appear" in s for s in printed)

        self.assertFalse(info_printed, "INFO emitted in ERROR-only mode")
        self.assertTrue(error_printed, "ERROR not emitted in ERROR-only mode")

        logger.set_log_level("INFO")

    def test_quiet_format(self):
        """Error messages must use [ERROR] prefix."""
        import unittest.mock as mock
        logger.set_log_level("ERROR")
        with mock.patch("builtins.print") as mock_print:
            logger.log_error("disk full")
        args = mock_print.call_args[0][0]
        self.assertTrue(args.startswith("[ERROR]"), f"Bad format: {args!r}")
        logger.set_log_level("INFO")


# ---------------------------------------------------------------------------
# Test 4 — No Timing in JSON
# ---------------------------------------------------------------------------

class TestNoTimingInJSON(unittest.TestCase):
    def test_json_contains_no_timing_fields(self):
        """Exported JSON must not contain any timing or duration fields."""
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            out_dir = Path(tmp) / "out"
            out_dir.mkdir()
            _make_dataset(data)

            import json
            out = out_dir / "report.json"
            r = scan_duplicates(data, keep="first")
            export_json(r["duplicates"], out)
            parsed = json.loads(out.read_text())

            timing_keys = {"duration", "time", "elapsed", "seconds",
                           "scan_duration", "timestamp"}
            for group in parsed:
                for key in group:
                    self.assertNotIn(
                        key.lower(), timing_keys,
                        f"Timing field found in JSON export: {key!r}"
                    )


# ---------------------------------------------------------------------------
# Test 5 — Phase Order Stability
# ---------------------------------------------------------------------------

class TestPhaseOrderStability(unittest.TestCase):
    def test_phase_order_is_fixed(self):
        """PHASE_ORDER tuple must be immutable and in correct sequence."""
        expected = ("scan", "size_group", "partial_hash", "full_hash", "grouping", "total")
        self.assertEqual(timer.PHASE_ORDER, expected)

    def test_get_timings_order_stable_across_calls(self):
        """Calling get_timings() twice returns same key order."""
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "data"
            data.mkdir()
            _make_dataset(data)
            timer.reset()
            scan_duplicates(data)
            keys1 = list(timer.get_timings().keys())
            keys2 = list(timer.get_timings().keys())
            self.assertEqual(keys1, keys2)


if __name__ == "__main__":
    unittest.main()
