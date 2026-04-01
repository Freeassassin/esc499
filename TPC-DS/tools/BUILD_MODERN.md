# TPC-DS Tools Modern GCC Build Notes

This document records the first implementation pass to make `TPC-DS/tools` compile on modern Linux GCC with minimal behavioral risk.

## What changed

1. `-std=gnu89` added to Linux CFLAGS.
- Reason: modern GCC defaults reject old K&R-style function definitions used in this codebase.

2. `-fcommon` added to Linux CFLAGS.
- Reason: modern GCC defaults (`-fno-common`) turn legacy common global definitions into linker errors.

3. `EXTRA_CFLAGS ?=` added and appended to `CFLAGS`.
- Reason: allows local/CI experiments without editing Makefiles again.

Files changed:
- `tools/makefile`
- `tools/Makefile.suite`

## Verified build commands

From `TPC-DS/tools`:

```bash
make clean
make OS=LINUX -j1 all
```

Build result in this environment: success (`build_exit:0`).

## Verified smoke checks

```bash
./distcomp -i tpcds.dst -o /tmp/tpcds.idx.test
./dsdgen -help
./dsqgen -help
```

Observed result in this environment:
- `distcomp_exit:0`
- `dsdgen_help_exit:0`
- `dsqgen_help_exit:0`

Built binaries:
- `distcomp`
- `dsdgen`
- `dsqgen`
- `checksum`

## Known remaining warnings

Compilation still emits warnings (format, deprecated APIs, and style warnings), but no hard failures.

Examples:
- format mismatch around `HUGE_FORMAT` in `config.h`
- deprecated `ftime` usage
- miscellaneous `-Wmisleading-indentation` and `-Wunused-*`

These are intentionally deferred to a follow-up hardening pass to keep this phase low risk.

## Optional stricter pass

Use this only for exploration in follow-up phases:

```bash
make clean
make OS=LINUX EXTRA_CFLAGS="-Wextra"
```

Do not enable `-Werror` until source-level warning cleanup is complete.
