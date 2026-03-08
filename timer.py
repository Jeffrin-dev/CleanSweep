"""
CleanSweep timer — v1.2.0 timing metrics layer.

Rules:
- Phase names are hardcoded — never dynamically generated
- Durations rounded to 3 decimal places, always
- Display order follows PHASE_ORDER — never dict order
- Timing data never enters JSON export
- Timer failures are swallowed — never crash the program
"""

from __future__ import annotations
import time

# ---------------------------------------------------------------------------
# Phase names — locked permanently, never extended without a version bump
# ---------------------------------------------------------------------------

PHASE_ORDER: tuple[str, ...] = (
    "scan",
    "size_group",
    "partial_hash",
    "full_hash",
    "grouping",
    "total",
)

# ---------------------------------------------------------------------------
# Module-level timer state
# ---------------------------------------------------------------------------

_starts:    dict[str, float] = {}
_durations: dict[str, float] = {}


def start_timer(label: str) -> None:
    """Record start time for a phase label."""
    try:
        _starts[label] = time.perf_counter()
    except Exception:
        pass


def end_timer(label: str) -> None:
    """Record elapsed duration for a phase label."""
    try:
        if label in _starts:
            _durations[label] = round(
                time.perf_counter() - _starts[label], 3
            )
    except Exception:
        pass


def get_timings() -> dict[str, float]:
    """
    Return completed timings in PHASE_ORDER.
    Only phases that have been started and ended are included.
    Order is always deterministic — never relies on dict insertion order.
    """
    try:
        return {
            phase: _durations[phase]
            for phase in PHASE_ORDER
            if phase in _durations
        }
    except Exception:
        return {}


def reset() -> None:
    """Clear all timing state. Used between scans and in tests."""
    try:
        _starts.clear()
        _durations.clear()
    except Exception:
        pass
