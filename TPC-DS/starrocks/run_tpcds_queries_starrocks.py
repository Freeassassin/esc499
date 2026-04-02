#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from common import default_config, mysql_conn

QUERY_ID_RE = re.compile(r"query(\d+)", re.IGNORECASE)
DATE_INTERVAL_RE = re.compile(r"([+\-])\s*(\d+)\s+days\b", re.IGNORECASE)


def query_sort_key(path: Path) -> tuple[int, str]:
    match = QUERY_ID_RE.search(path.stem)
    if not match:
        return (10_000, path.name)
    return (int(match.group(1)), path.name)


def normalize_for_starrocks(sql_text: str) -> str:
    normalized = DATE_INTERVAL_RE.sub(r"\1 interval \2 day", sql_text)
    normalized = re.sub(r"\)\s+at\s*,", ") at_tbl,", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bat\.", "at_tbl.", normalized, flags=re.IGNORECASE)
    normalized = re.sub(
        r"\bc_last_review_date_sk\b",
        "c_last_review_date",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(
        r"coalesce\(returns,\s*0\)\s+returns\b",
        "coalesce(returns, 0) as returns_amt",
        normalized,
        flags=re.IGNORECASE,
    )

    normalized = normalized.replace(
        "\n )\n order by 1,4,5,2\n  limit 100",
        "\n ) starrocks_q49\n order by 1,4,5,2\n  limit 100",
    )
    normalized = re.sub(
        r"(ws_bill_customer_sk in \(select c_customer_sk from best_ss_customer\)\))\s+limit 100",
        r"\1 starrocks_q69\n  limit 100",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(
        r"(group by c_last_name,c_first_name\))\s+order by c_last_name,c_first_name,sales",
        r"\1 starrocks_q70\n     order by c_last_name,c_first_name,sales",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(
        r"(d3\.d_year between 2000 AND 2000 \+ 2\))\s+where i_brand_id = brand_id",
        r"\1 cross_sales\n where i_brand_id = brand_id",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = normalized.replace(
        "        from catalog_sales)),\n wswscs as",
        "        from catalog_sales) wscs_src),\n wswscs as",
    )
    normalized = normalized.replace(
        "group by c_customer_sk)),",
        "group by c_customer_sk) max_store_sales_src),",
    )
    return normalized


def load_statements(queries_dir: Path) -> list[tuple[int, str, str]]:
    sql_files = sorted(queries_dir.glob("*.sql"), key=query_sort_key)
    if not sql_files:
        raise RuntimeError(f"No SQL files found in {queries_dir}")

    selected: dict[int, Path] = {}
    for sql_file in sql_files:
        match = QUERY_ID_RE.search(sql_file.stem)
        if not match:
            continue
        query_id = int(match.group(1))
        if 1 <= query_id <= 99 and query_id not in selected:
            selected[query_id] = sql_file

    if len(selected) == 99:
        return [
            (query_id, selected[query_id].name, selected[query_id].read_text(encoding="utf-8"))
            for query_id in range(1, 100)
        ]

    merged = next((path for path in sql_files if path.name == "query_0.sql"), None)
    if merged is None:
        missing = [query_id for query_id in range(1, 100) if query_id not in selected]
        raise RuntimeError(f"Missing generated query files for IDs: {missing}")

    chunks = [chunk.strip() for chunk in merged.read_text(encoding="utf-8").split(";") if chunk.strip()]
    if len(chunks) < 99:
        raise RuntimeError(f"query_0.sql parsing produced {len(chunks)} statements, expected at least 99")
    return [(index + 1, merged.name, chunks[index]) for index in range(99)]


def main() -> None:
    defaults = default_config()
    parser = argparse.ArgumentParser(description="Execute TPC-DS queries in StarRocks")
    parser.add_argument("--queries-dir", required=True, help="Directory containing generated SQL queries")
    parser.add_argument("--host", default=defaults["mysql_host"])
    parser.add_argument("--port", type=int, default=defaults["mysql_port"])
    parser.add_argument("--user", default=defaults["user"])
    parser.add_argument("--password", default=defaults["password"])
    parser.add_argument("--dbname", default=defaults["database"])
    parser.add_argument("--summary-json", required=True, help="Path for execution summary JSON")
    args = parser.parse_args()

    statements = load_statements(Path(args.queries_dir))
    summary: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []

    with mysql_conn(args.host, args.port, args.user, args.password, database=args.dbname) as conn:
        with conn.cursor() as cur:
            cur.execute("SET query_timeout = 3600")
        for query_id, source_file, sql_text in statements:
            start = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    cur.execute(normalize_for_starrocks(sql_text))
                    rows = list(cur.fetchall()) if cur.description else []
                elapsed = time.perf_counter() - start
                summary.append(
                    {
                        "query_id": query_id,
                        "file": source_file,
                        "status": "ok",
                        "elapsed_sec": round(elapsed, 6),
                        "row_count": len(rows),
                    }
                )
                print(f"ok:q{query_id}:rows={len(rows)}:sec={elapsed:.4f}")
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - start
                item = {
                    "query_id": query_id,
                    "file": source_file,
                    "status": "error",
                    "elapsed_sec": round(elapsed, 6),
                    "error": str(exc),
                }
                summary.append(item)
                failures.append(item)
                print(f"error:q{query_id}:sec={elapsed:.4f}:{exc}")

    out_path = Path(args.summary_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if failures:
        raise SystemExit(f"{len(failures)} query failures; see {out_path}")

    print(f"all_queries_ok:99:summary={out_path}")


if __name__ == "__main__":
    main()