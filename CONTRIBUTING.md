# Contributing to CleanSweep

Thank you for considering a contribution. This guide explains everything you need
to get set up, write correct code, run the test suite, and submit a pull request.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Getting the code](#2-getting-the-code)
3. [Project layout](#3-project-layout)
4. [Code style](#4-code-style)
5. [Non-negotiable rules](#5-non-negotiable-rules)
6. [Running the tests](#6-running-the-tests)
7. [Writing tests](#7-writing-tests)
8. [Submitting a pull request](#8-submitting-a-pull-request)
9. [Release process](#9-release-process)
10. [Getting help](#10-getting-help)

---

## 1. Prerequisites

- Python **3.10 or higher** (check with `python3 --version`)
- Standard library only — no `pip install` required to run or develop CleanSweep
- Git

No virtual environment is needed. If you want one for IDE tooling:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows
```

---

## 2. Getting the code

```bash
git clone https://github.com/Jeffrin-dev/cleansweep.git
cd cleansweep
git checkout dev            # all new work goes here
```

Branch meanings:

| Branch | Purpose |
|---|---|
| `main` | Stable, tagged releases only |
| `release/v3` | Long-term support for v3.x |
| `dev` | Active development — base all PRs here |

---

## 3. Project layout

```
cleansweep/
├── version.py              ← single source of truth for VERSION
├── main.py                 ← CLI only; no business logic
├── config.py               ← config loading and validation
├── scanner.py              ← file traversal (read-only)
├── duplicates.py           ← duplicate detection pipeline
├── organizer.py            ← file move execution
├── rules.py                ← rule parsing and evaluation (pure)
├── destination_map.py      ← logical key → path mapping (pure)
├── planner.py              ← organizer plan construction
├── batch_engine.py         ← atomic batch executor
├── action_controller.py    ← all destructive actions gate here
├── file_operation_manager.py ← atomic move backend
├── trash_manager.py        ← OS trash backends (XDG/macOS/Windows)
├── report.py               ← all terminal output lives here
├── analyzer.py             ← pure summarisation helpers
├── logger.py               ← centralised logging
├── timer.py                ← phase timing
├── INVARIANTS.md           ← 20 locked architectural invariants
├── CHANGELOG.md
├── CONTRIBUTING.md         ← this file
├── README.md
└── test_*.py               ← test suite (standard unittest)
```

---

## 4. Code style

**Formatting**

- 4-space indentation, no tabs
- Max line length: 100 characters
- Trailing whitespace: none

**Naming**

- `snake_case` for variables, functions, modules
- `PascalCase` for classes and dataclasses
- `UPPER_SNAKE` for module-level constants
- Private helpers prefixed with `_`

**Type annotations**

- All public functions must have complete type annotations
- Return types are mandatory — no bare `def f():`
- Use `from __future__ import annotations` only when needed to avoid forward-reference issues
- Prefer `list[X]` over `List[X]`; `dict[K, V]` over `Dict[K, V]` (Python 3.10+)

**Docstrings**

- Module docstring required: describe responsibility and what the module must NOT do
- Public function docstring required for anything non-trivial
- No docstrings on single-line private helpers unless the logic is surprising

**Function size**

- Preferred ceiling: 60 lines. Hard ceiling: 80 lines.
- If a function exceeds 60 lines, split it before submitting.

**Nesting**

- Maximum depth: 3 levels. Flatten with early returns or helper functions.

---

## 5. Non-negotiable rules

These rules are locked. Any PR that violates them is rejected without discussion.

**No external packages.** Standard library only. No exceptions.

**No `print()` outside `report.py`.** Engine modules (`scanner`, `duplicates`,
`rules`, `organizer`, `analyzer`, `config`) return data. They never print.

**No circular imports.** The import graph in `INVARIANTS.md §8` is final.
If a change requires a new import edge, it requires explicit architectural review.

**No filesystem mutation outside the action layer.** Only `action_controller.py`,
`file_operation_manager.py`, and `trash_manager.py` may call `unlink()`, `rename()`,
or `shutil.move()`. See `INVARIANTS.md §16` for the complete permitted call-site list.

**No hidden global state.** Module-level mutable state is forbidden except in
`logger.py` and `timer.py`, which have explicit thread-safe accessors.

**Determinism is mandatory.** All file lists must be sorted before processing.
Any function that accepts a collection and produces a collection must preserve
or establish sorted order. See `INVARIANTS.md §1` and `§11`.

**Dry-run must always be preserved.** Any new operation that touches the filesystem
must respect the `dry_run` flag. No implicit side effects.

---

## 6. Running the tests

All tests use Python's built-in `unittest`. No test runner install required.

```bash
# Run the full suite
python3 -m unittest discover -s . -p "test_*.py" -v

# Run a single test file
python3 -m unittest test_duplicates -v

# Run a single test case
python3 -m unittest test_duplicates.TestDuplicateDetection.test_full_hash_confirms_match -v
```

**All tests must pass before a PR is submitted.** No exceptions.

Stress tests (large file trees) are gated behind environment variables so they
don't slow down the default run:

```bash
CS_STRESS_FILES=100000 python3 -m unittest test_memory -v
CS_STRESS_FILES=1000000 python3 -m unittest test_memory -v   # slow — ~3 min
```

---

## 7. Writing tests

**What must be tested**

Every PR that adds or changes behaviour must include tests covering:

- The happy path for the new behaviour
- At least one error or edge case (empty input, permission denied, etc.)
- Determinism: if the change touches output ordering, assert that shuffling input
  does not change output

**Test file naming**

Name test files after the module they test: `test_scanner.py`, `test_rules.py`, etc.
Integration tests go in `test_integration.py`.

**Test structure**

```python
import unittest
from pathlib import Path

class TestMyFeature(unittest.TestCase):

    def test_happy_path(self) -> None:
        ...

    def test_empty_input_returns_empty(self) -> None:
        ...

    def test_determinism(self) -> None:
        # Run twice, assert outputs are identical
        ...
```

**Temporary directories**

Use `tempfile.TemporaryDirectory` as a context manager so cleanup is guaranteed:

```python
import tempfile

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    # create test files, run function, assert results
```

**No real user files.** Tests must never read from or write to paths outside
a temporary directory they created.

---

## 8. Submitting a pull request

1. **Base your branch on `dev`**, never on `main` or `release/v3`:
   ```bash
   git checkout dev
   git pull origin dev
   git checkout -b feature/my-change
   ```

2. **Keep commits focused.** One logical change per commit. Write commit messages
   in the imperative mood: `Add --min-size flag to scan command`, not `Added...`

3. **Run the full test suite** before pushing:
   ```bash
   python3 -m unittest discover -s . -p "test_*.py" -v
   ```

4. **Check for print() leaks** in engine modules:
   ```bash
   grep -n "print(" scanner.py duplicates.py rules.py organizer.py analyzer.py config.py
   # Must produce no output
   ```

5. **Check for new circular imports**:
   ```bash
   python3 -c "import main" && echo "OK"
   ```

6. **Open a PR against `dev`.** Fill out the PR template:
   - What does this change?
   - Which invariants does it affect (if any)?
   - What tests were added?
   - Is this a breaking change?

7. **Breaking changes are blocked in v3.x.** If your change breaks a CLI flag,
   config key, or exit code, it must wait for v4.0. Discuss in an issue first.

---

## 9. Release process

Only maintainers tag releases.

```bash
# Update version.py
echo 'VERSION = "3.0.1"' > version.py

# Update CHANGELOG.md
# Update VERSION in main.py

# Commit
git commit -am "Release v3.0.1"

# Tag
git tag v3.0.1
git push origin main --tags
```

Patch releases (`3.0.x`) go directly to `main` and `release/v3`.  
Minor releases (`3.x.0`) are developed on `dev`, merged to `main`, then cherry-picked to `release/v3`.  
Major releases (`4.0.0`) require a new `release/v4` branch.

---

## 10. Getting help

- Open a [GitHub Discussion](../../discussions) for questions about the architecture
- Open a [GitHub Issue](../../issues) for bugs or feature requests using the provided templates
- Read `INVARIANTS.md` before proposing any architectural change — many things are
  locked permanently by design
