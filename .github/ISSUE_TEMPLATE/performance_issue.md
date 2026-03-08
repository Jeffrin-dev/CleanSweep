---
name: Performance issue
about: Something is slower than expected or uses too much memory
title: "[PERF] "
labels: performance
assignees: ''
---

## Description
What is slow or using too much memory?

## Command run
```bash
# Paste the exact command you ran
cleansweep ...
```

## Observed performance
- File count scanned:
- Time taken: (e.g. "40 minutes for 50K files")
- Memory used: (if known — e.g. from `top` or Task Manager)

## Expected performance
Based on the benchmarks in README.md §11, what did you expect?
- Expected time:
- Expected memory:

## Hardware
- CPU: (e.g. AMD Ryzen 5, Apple M2)
- RAM:
- Storage type: (HDD / SSD / NVMe / NAS / network drive)
- Filesystem: (e.g. ext4, APFS, NTFS, exFAT)

## Environment
- OS: (e.g. Ubuntu 22.04 / macOS 14 / Windows 11)
- Python version: (output of `python3 --version`)
- CleanSweep version: (output of `python3 main.py --version`)
- Workers used: (default or `--workers N`)

## Timing output
Run with `--verbose` and paste the timing summary:
```
# Paste --verbose output here
```

## File characteristics
- Approximate file count:
- Typical file size: (e.g. mostly small text files / large video files / mixed)
- Many duplicates or mostly unique?
- Deep directory tree or mostly flat?

## Additional context
Anything else that might be relevant — network mounts, antivirus, disk encryption, etc.
