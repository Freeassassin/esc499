#!/usr/bin/env python3
import argparse
import json
import re
import time
from pathlib import Path

import duckdb

QUERY_ID_RE = re.compile(r"query(\d+)", re.IGNORECASE)
DATE_INTERVAL_RE = re.compile(r"([+\-])\s*(\d+)\s+days\b", re.IGNORECASE)


def query_sort_key(path: Path) -> tuple[int, str]:
    m = QUERY_ID_RE.search(path.stem)
    if not m:
        return (10_000, path.name)
    return (int(m.group(1)), path.name)


def normalize_for_duckdb(sql_text: str) -> str:
    # TPC-DS templates use constructs like `+ 30 days`; DuckDB expects INTERVAL syntax.
    normalized = DATE_INTERVAL_RE.sub(r"\1 INTERVAL \2 DAYS", sql_text)

    # query41 alias uses reserved keyword `at` in some streams.
    normalized = re.sub(r"\)\s+at\s*,", ") at_tbl,", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bat\.", "at_tbl.", normalized, flags=re.IGNORECASE)

    # query78 references a non-existent column in this schema version.
    normalized = re.sub(
        r"\bc_last_review_date_sk\b",
        "c_last_review_date",
        normalized,
        flags=re.IGNORECASE,
    )

    # query81 can include a projection alias without AS that collides with keyword usage.
    normalized = re.sub(
        r"coalesce\(returns,\s*0\)\s+returns\b",
        "coalesce(returns, 0) as returns_amt",
        normalized,
        flags=re.IGNORECASE,
    )

    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute TPC-DS queries in DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database file path")
    parser.add_argument("--queries-dir", required=True, help="Directory containing generated SQL queries")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB worker threads")
    parser.add_argument("--summary-json", required=True, help="Path for execution summary JSON")
    args = parser.parse_args()

    queries_dir = Path(args.queries_dir)
    sql_files = sorted(queries_dir.glob("*.sql"), key=query_sort_key)
    if not sql_files:
        raise RuntimeError(f"No SQL files found in {queries_dir}")

    statements: list[tuple[int, str, str]] = []

    # Preferred: one SQL file per query with explicit query ID in filename.
    selected: dict[int, Path] = {}
    for sql_file in sql_files:
        m = QUERY_ID_RE.search(sql_file.stem)
        if not m:
            continue
        qid = int(m.group(1))
        if 1 <= qid <= 99 and qid not in selected:
            selected[qid] = sql_file

    if len(selected) == 99:
        for qid in range(1, 100):
            sql_file = selected[qid]
            statements.append((qid, sql_file.name, sql_file.read_text(encoding="utf-8")))
    else:
        # Fallback: dsqgen may emit a single stream file (for example query_0.sql)
        # that concatenates all queries separated by semicolons.
        merged = next((p for p in sql_files if p.name == "query_0.sql"), None)
        if merged is None:
            missing = [qid for qid in range(1, 100) if qid not in selected]
            raise RuntimeError(f"Missing generated query files for IDs: {missing}")

        raw = merged.read_text(encoding="utf-8")
        chunks = [chunk.strip() for chunk in raw.split(";") if chunk.strip()]
        if len(chunks) < 99:
            raise RuntimeError(
                f"query_0.sql parsing produced {len(chunks)} statements, expected at least 99"
            )
        for idx in range(99):
            qid = idx + 1
            statements.append((qid, merged.name, chunks[idx]))

    con = duckdb.connect(args.db)
    con.execute(f"PRAGMA threads={args.threads}")

    summary: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []

    for qid, source_file, sql_text in statements:
        start = time.perf_counter()
        try:
            # Materialize result to ensure full execution.
            rows = con.execute(normalize_for_duckdb(sql_text)).fetchall()
            elapsed = time.perf_counter() - start
            summary.append(
                {
                    "query_id": qid,
                    "file": source_file,
                    "status": "ok",
                    "elapsed_sec": round(elapsed, 6),
                    "row_count": len(rows),
                }
            )
            print(f"ok:q{qid}:rows={len(rows)}:sec={elapsed:.4f}")
        except Exception as exc:  # noqa: BLE001
            elapsed = time.perf_counter() - start
            item = {
                "query_id": qid,
                "file": source_file,
                "status": "error",
                "elapsed_sec": round(elapsed, 6),
                "error": str(exc),
            }
            summary.append(item)
            failures.append(item)
            print(f"error:q{qid}:sec={elapsed:.4f}:{exc}")

    con.close()

    out_path = Path(args.summary_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if failures:
        raise SystemExit(f"{len(failures)} query failures; see {out_path}")

    print(f"all_queries_ok:99:summary={out_path}")


if __name__ == "__main__":
    main()
