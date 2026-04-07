#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

QUERY_ID_RE = re.compile(r"query(\d+)", re.IGNORECASE)
DATE_INTERVAL_RE = re.compile(r"([+\-])\s*(\d+)\s+days\b", re.IGNORECASE)


def query_sort_key(path: Path) -> tuple[int, str]:
    match = QUERY_ID_RE.search(path.stem)
    if not match:
        return (10_000, path.name)
    return (int(match.group(1)), path.name)


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


def _fix_postgresql_lochierarchy_order_by(sql_text: str) -> str:
    """Replace alias-in-expression ORDER BY forms that PostgreSQL rejects."""
    fixed = re.sub(
        r"case\s+when\s+lochierarchy\s*=\s*0\s+then\s+i_category\s+end",
        "case when grouping(i_category)+grouping(i_class) = 0 then i_category end",
        sql_text,
        flags=re.IGNORECASE,
    )
    fixed = re.sub(
        r"case\s+when\s+lochierarchy\s*=\s*0\s+then\s+s_state\s+end",
        "case when grouping(s_state)+grouping(s_county) = 0 then s_state end",
        fixed,
        flags=re.IGNORECASE,
    )
    return fixed


def normalize_sql(engine: str, sql_text: str) -> str:
    if engine == "duckdb":
        normalized = DATE_INTERVAL_RE.sub(r"\1 INTERVAL \2 DAYS", sql_text)
    elif engine == "cedardb":
        normalized = DATE_INTERVAL_RE.sub(r"\1 INTERVAL '\2 days'", sql_text)
        normalized = re.sub(
            r"cast\(amc as decimal\(15,4\)\)\s*/\s*cast\(pmc as decimal\(15,4\)\)",
            "cast(amc as decimal(15,4))/nullif(cast(pmc as decimal(15,4)), 0)",
            normalized,
            flags=re.IGNORECASE,
        )
    elif engine == "postgresql":
        normalized = DATE_INTERVAL_RE.sub(r"\1 INTERVAL '\2 days'", sql_text)
        normalized = re.sub(
            r"cast\(amc as decimal\(15,4\)\)\s*/\s*cast\(pmc as decimal\(15,4\)\)",
            "cast(amc as decimal(15,4))/nullif(cast(pmc as decimal(15,4)), 0)",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = _fix_postgresql_lochierarchy_order_by(normalized)
    elif engine == "starrocks":
        normalized = DATE_INTERVAL_RE.sub(r"\1 interval \2 day", sql_text)
    else:
        raise ValueError(f"Unsupported engine: {engine}")

    normalized = re.sub(r"\)\s+at\s*,", ") at_tbl,", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bat\.", "at_tbl.", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bc_last_review_date_sk\b", "c_last_review_date", normalized, flags=re.IGNORECASE)
    normalized = re.sub(
        r"coalesce\(returns,\s*0\)\s+returns\b",
        "coalesce(returns, 0) as returns_amt",
        normalized,
        flags=re.IGNORECASE,
    )

    if engine == "starrocks":
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


def write_summary(summary_path: Path, summary: list[dict[str, object]]) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
