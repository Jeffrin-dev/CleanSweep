"""
CleanSweep version declaration.

This is the single source of truth for the version string.
All other modules that need the version must import from here.

Version format: MAJOR.MINOR.PATCH  (Semantic Versioning 2.0.0)
  MAJOR — breaking changes (CLI flags removed, config format changed)
  MINOR — new features, backward-compatible
  PATCH — bug fixes, no behavior changes

Stability guarantees for v3.x:
  - config.json format (version field "2.x") remains parseable
  - All CLI flags defined in v3.0.0 remain available
  - All exit codes are frozen (see main.py Exit class)
  - No breaking changes until v4.0.0
"""

VERSION: str = "3.0.0"
