#!/usr/bin/env python3
"""Execute all 99 TPC-DS queries against CedarDB and write a JSON summary."""
import argparse
import json
import re
import time
from pathlib import Path

import psycopg

QUERY_ID_RE = re.compile(r"query(\d+)", re.IGNORECASE)
# TPC-DS templates emit `+ N days` / `- N days`; PostgreSQL requires quoted INTERVAL.
DATE_INTERVAL_RE = re.compile(r"([+\-])\s*(\d+)\s+days\b", re.IGNORECASE)


def conninfo(args: argparse.Namespace) -> str:
    return (
        f"host={args.host} port={args.port} "
        f"dbname={args.dbname} user={args.user} password={args.password}"
    )


def query_sort_key(path: Path) -> tuple[int, str]:
    m = QUERY_ID_RE.search(path.stem)
    if not m:
        return (10_000, path.name)
    return (int(m.group(1)), path.name)


def normalize_for_cedardb(sql_text: str) -> str:
    # `+ N days` / `- N days` → `+ INTERVAL 'N days'` (PostgreSQL quoted-string form).
    normalized = DATE_INTERVAL_RE.sub(r"\1 INTERVAL '\2 days'", sql_text)

    # query41: `at` is a reserved keyword in PostgreSQL (AT TIME ZONE).
    normalized = re.sub(r"\)\s+at\s*,", ") at_tbl,", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bat\.", "at_tbl.", normalized, flags=re.IGNORECASE)

    # query78: column c_last_review_date_sk does not exist in this schema version.
    normalized = re.sub(
        r"\bc_last_review_date_sk\b",
        "c_last_review_date",
        normalized,
        flags=re.IGNORECASE,
    )

    # query81: bare alias `returns` collides with reserved keyword without AS.
    normalized = re.sub(
        r"coalesce\(returns,\s*0\)\s+returns\b",
        "coalesce(returns, 0) as returns_amt",
        normalized,
        flags=re.IGNORECASE,
    )

    return normalized


def load_statements(queries_dir: Path) -> list[tuple[int, str, str]]:
    """Return list of (query_id, source_file_name, sql_text) for queries 1-99."""
    sql_files = sorted(queries_dir.glob("*.sql"), key=query_sort_key)
    if not sql_files:
        raise RuntimeError(f"No SQL files found in {queries_dir}")

    selected: dict[int, Path] = {}
    for sql_file in sql_files:
        m = QUERY_ID_RE.search(sql_file.stem)
        if not m:
            continue
        qid = int(m.group(1))
        if 1 <= qid <= 99 and qid not in selected:
            selected[qid] = sql_file

    if len(selected) == 99:
        return [
            (qid, selected[qid].name, selected[qid].read_text(encoding="utf-8"))
            for qid in range(1, 100)
        ]

    # Fallback: single stream file query_0.sql produced by dsqgen -streams 1.
    merged = next((p for p in sql_files if p.name == "query_0.sql"), None)
    if merged is None:
        missing = [qid for qid in range(1, 100) if qid not in selected]
        raise RuntimeError(f"Missing generated query files for IDs: {missing}")

    raw = merged.read_text(encoding="utf-8")
    chunks = [chunk.strip() for chunk in raw.split(";") if chunk.strip()]
    if len(chunks) < 99:
        raise RuntimeError(
            f"query_0.sql parsing yielded {len(chunks)} statements, expected at least 99"
        )
    return [(idx + 1, merged.name, chunks[idx]) for idx in range(99)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute TPC-DS queries in CedarDB")
    parser.add_argument("--queries-dir", required=True, help="Directory with generated SQL")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="db")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--summary-json", required=True, help="Path for execution summary JSON")
    args = parser.parse_args()

    statements = load_statements(Path(args.queries_dir))

    summary: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []

    with psycopg.connect(conninfo(args)) as conn:
        conn.autocommit = True
        for qid, source_file, sql_text in statements:
            start = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    cur.execute(normalize_for_cedardb(sql_text))
                    rows = cur.fetchall()
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

    out_path = Path(args.summary_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if failures:
        raise SystemExit(f"{len(failures)} query failures; see {out_path}")

    print(f"all_queries_ok:99:summary={out_path}")


if __name__ == "__main__":
    main()
