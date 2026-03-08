#!/usr/bin/env bash
# git_setup.sh — initialise the CleanSweep v3.0.0 git repository structure.
#
# Run once from the project root after cloning or unzipping:
#   bash git_setup.sh
#
# Creates:
#   - initial commit on main
#   - v3.0.0 annotated tag on main
#   - release/v3 branch (mirrors main — long-term support)
#   - dev branch (new development base)

set -euo pipefail

echo "[1/6] Initialising git repository..."
git init

echo "[2/6] Configuring .gitignore..."
cat > .gitignore << 'GITIGNORE'
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
.venv/
venv/
*.egg-info/
dist/
build/
.DS_Store
*.tmp
*.log
GITIGNORE

echo "[3/6] Staging all files..."
git add .

echo "[4/6] Creating initial commit on main..."
git commit -m "Release CleanSweep v3.0.0

Production release. Architecture frozen. Public interface stable.

- version.py: single source of truth for VERSION
- README.md: full CLI reference, config guide, architecture docs
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, CHANGELOG.md
- GitHub issue templates
- tests/ folder with 838 tests
- Batch rollback on partial failure (v3.0.0 addition)
- report subcommand added to CLI"

echo "[5/6] Tagging v3.0.0..."
git tag -a v3.0.0 -m "CleanSweep v3.0.0 — Production Release

Architecture frozen. All 838 tests passing.
CLI stable: scan / duplicates / organize / report
Config schema 2.x compatible.
Exit codes locked permanently."

echo "[6/6] Creating branches..."
git checkout -b release/v3
git checkout -b dev
git checkout main

echo ""
echo "Done. Repository structure:"
echo "  main       — stable, tagged v3.0.0"
echo "  release/v3 — long-term support branch"
echo "  dev        — active development base"
echo ""
echo "To push to a remote:"
echo "  git remote add origin <url>"
echo "  git push origin main release/v3 dev --tags"
