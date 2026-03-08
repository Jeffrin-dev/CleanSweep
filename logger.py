"""
CleanSweep logger — v1.2.0 observability layer.

Rules (locked in INVARIANTS.md):
- Engine modules call log_* functions only — never print directly
- Engine modules never check verbosity or log level
- Logging is purely side-channel — never modifies data or control flow
- Log level set once at startup via set_log_level()
- Thread-safe: no shared mutable state beyond _state dict (GIL-protected)
- Logging failures are swallowed — never crash the program
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Level definitions — ordered, fixed, never extended
# ---------------------------------------------------------------------------

_LEVELS: dict[str, int] = {
    "ERROR": 0,
    "WARN":  1,
    "INFO":  2,
    "DEBUG": 3,
}

# Module-level state — minimal, GIL-protected for single-threaded use
_state: dict = {
    "level": "INFO",   # default
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def set_log_level(level: str) -> None:
    """Set active log level. Called once at startup by main.py."""
    upper = level.upper()
    if upper not in _LEVELS:
        return  # silently ignore invalid level — never crash
    _state["level"] = upper


def is_debug() -> bool:
    """Guard for expensive debug log construction inside loops."""
    return _state["level"] == "DEBUG"


def log_error(message: str) -> None:
    _emit("ERROR", message)


def log_warn(message: str) -> None:
    _emit("WARN", message)


def log_info(message: str) -> None:
    _emit("INFO", message)


def log_debug(message: str) -> None:
    _emit("DEBUG", message)


# ---------------------------------------------------------------------------
# Internal emit — all formatting lives here, nowhere else
# ---------------------------------------------------------------------------

def _emit(level: str, message: str) -> None:
    """
    Emit a log line if level is within current threshold.

    Format: [LEVEL] message
    - Always uppercase level
    - Fixed bracket format
    - No timestamps (clean output; ISO-8601 timestamp opt-in via debug only)
    - No variable spacing
    - Failures swallowed — logging never crashes the program
    """
    try:
        if _LEVELS.get(level, 99) <= _LEVELS.get(_state["level"], 2):
            print(f"[{level}] {message}")
    except Exception:
        pass  # observability must never break core functionality
