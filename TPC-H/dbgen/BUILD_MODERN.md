# TPC-H dbgen/qgen Modern Linux Build Notes

This document captures a reproducible build flow for modern Linux toolchains while preserving legacy TPC-H behavior.

## What was changed

- Added a local `makefile` (from `makefile.suite`) with Linux defaults.
- Set:
  - `CC = gcc`
  - `MACHINE = LINUX`
  - `WORKLOAD = TPCH`
  - `DATABASE = ORACLE` (required so qgen database macros are defined)
- Added compatibility and warning flags:
  - `-std=gnu89` (required for legacy function declarations used in this codebase)
  - `-Wall -Wextra -Wpedantic`
  - Existing TPC-H defines preserved.

## Build steps

From `TPC-H/dbgen`:

```bash
# 1) Build tools
make clean
make -j1

# 2) Verify binaries
ls -lh dbgen qgen
```

Expected result: both `dbgen` and `qgen` are produced.

## Functional smoke tests

```bash
# dbgen small-scale generation
./dbgen -s 0.01 -f

# qgen requires template path
DSS_QUERY=queries ./qgen -d 1 > /tmp/q1.sql
```

Notes:
- `qgen` must be able to locate query templates (`DSS_QUERY=queries` in this tree).
- Without `DSS_QUERY`, qgen may fail with an open-file error for query templates.

## Why GNU89 is required

Modern GCC default C modes are stricter about old-style declarations used in this legacy source.
Without `-std=gnu89`, build fails in `driver.c` with incompatible function-pointer and argument errors.

## Current known warning areas (non-blocking)

- Format width mismatches in some `printf`/`sprintf` calls.
- Unbounded string operations in utility code paths.
- Legacy C90 pedantic warnings around `long long` usage.

These do not block successful Linux builds, but should be addressed incrementally in a hardening pass.

## Next hardening pass (recommended)

1. Replace high-risk `sprintf`/`strcpy`/`strcat` sites in `bm_utils.c` and `varsub.c` with bounded variants.
2. Normalize format specifiers for `DSS_HUGE` and related date/rowcount prints.
3. Add a second compile pass with stricter format-security checks after each small patch.
