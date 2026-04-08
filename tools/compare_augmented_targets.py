#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUG_END = "-- AUGMENTED_WORKLOAD_BLOCK_END"

AGG_FUNC_RE = re.compile(r"\b(sum|count|avg|min|max|stddev|variance|any_value|anyvalue)\s*\(", re.IGNORECASE)
AGG_TARGET_RE = re.compile(r"\b(?:sum|count|avg|min|max|stddev|variance|any_value|anyvalue)\s*\(\s*(?:distinct\s+)?(.+?)\)", re.IGNORECASE)
JOIN_RE = re.compile(
    r"\b(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|JOIN)\b",
    re.IGNORECASE,
)
ON_CLAUSE_RE = re.compile(
    r"\bON\b\s+(.+?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bFULL\b|\bJOIN\b|$)",
    re.IGNORECASE | re.DOTALL,
)
METADATA_RE = re.compile(
    r"\b(show|describe|desc\s+|information_schema|pg_catalog|duckdb_tables|duckdb_columns|duckdb_schemas)\b",
    re.IGNORECASE,
)
MAINT_RE = re.compile(r"\b(vacuum|analyze|reindex|checkpoint|pragma|maintenance_op)\b", re.IGNORECASE)
NULL_RE = re.compile(r"\b(is\s+null|is\s+not\s+null|coalesce|nullif|ifnull)\b", re.IGNORECASE)
TEXT_HINT_RE = re.compile(r"(_name|_type|_status|_flag|_desc|varchar|text|string|char)", re.IGNORECASE)
TIMESTAMP_RE = re.compile(r"\b(timestamp|current_timestamp)\b", re.IGNORECASE)
BOOLEAN_RE = re.compile(r"\b(true|false|boolean)\b", re.IGNORECASE)


def split_statements(sql_text: str) -> list[str]:
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def normalize_stmt(stmt: str) -> str:
    compact = re.sub(r"\s+", " ", stmt.strip())
    compact = re.sub(r"'[^']*'", "'?'", compact)
    compact = re.sub(r"\b\d+\b", "?", compact)
    return compact.lower()


def load_tpcds_queries() -> list[str]:
    qdir = ROOT / "TPC-DS" / "queries" / "duckdb" / "sf1" / "stream1"
    per_query = sorted(qdir.glob("query[0-9]*.sql"), key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)))
    if len(per_query) >= 99:
        return [p.read_text(encoding="utf-8") for p in per_query[:99]]

    merged = qdir / "query_0.sql"
    if not merged.exists():
        raise FileNotFoundError(f"Missing TPC-DS query source in {qdir}")
    text = merged.read_text(encoding="utf-8")
    if AUG_END in text:
        chunks: list[str] = []
        acc: list[str] = []
        for line in text.splitlines():
            acc.append(line)
            if line.strip() == AUG_END:
                chunks.append("\n".join(acc).strip())
                acc = []
        if len(chunks) < 99:
            raise RuntimeError(f"Expected at least 99 grouped TPC-DS queries, found {len(chunks)}")
        return chunks[:99]

    chunks = [s.strip() for s in text.split(";") if s.strip()]
    if len(chunks) < 99:
        raise RuntimeError(f"Expected at least 99 statements in query_0.sql, found {len(chunks)}")
    return chunks[:99]


def load_tpch_queries() -> list[str]:
    qdir = ROOT / "TPC-H" / "queries" / "duckdb" / "1"
    files = sorted(qdir.glob("*.sql"), key=lambda p: int(p.stem))
    if len(files) < 22:
        raise RuntimeError(f"Expected 22 TPC-H query files in {qdir}, found {len(files)}")
    return [p.read_text(encoding="utf-8") for p in files[:22]]


def tree_depth(node: dict) -> int:
    return 1 + max((tree_depth(c) for c in node.get("children", [])), default=0)


def count_operators(node: dict) -> int:
    here = 1 if node.get("operator_type") else 0
    return here + sum(count_operators(c) for c in node.get("children", []))


def load_profile_rows(profile_dir: Path, total_queries: int) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, total_queries + 1):
        path = profile_dir / f"q{i}.json"
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        op_count = sum(count_operators(c) for c in data.get("children", []))
        expr_depth = max((tree_depth(c) for c in data.get("children", [])), default=0)
        rows.append({"op_count": op_count, "expr_depth": expr_depth})
    return rows


def statement_metrics(statement_texts: list[str]) -> dict[str, float | int]:
    total = len(statement_texts)
    if total == 0:
        raise RuntimeError("No statements available for metric computation")

    write = 0
    ctas = 0
    metadata = 0
    maintenance = 0
    join_total = 0
    join_outer = 0
    text_join_keys = 0
    agg_total = 0
    agg_anyvalue = 0
    agg_text_targets = 0
    null_stmt = 0
    str_stmt = 0
    ts_stmt = 0
    bool_stmt = 0

    normalized = [normalize_stmt(s) for s in statement_texts]
    dup_ratio = (len(normalized) - len(set(normalized))) / len(normalized) * 100

    for sql in statement_texts:
        upper = sql.lstrip().upper()
        if upper.startswith(("INSERT", "UPDATE", "DELETE", "COPY", "MERGE")):
            write += 1
        if re.search(r"\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\b.*\bAS\s+SELECT\b", sql, re.IGNORECASE | re.DOTALL):
            ctas += 1
        if METADATA_RE.search(sql):
            metadata += 1
        if MAINT_RE.search(sql):
            maintenance += 1

        joins = JOIN_RE.findall(sql)
        for j in joins:
            join_total += 1
            ju = j.upper()
            if ju.startswith("LEFT") or ju.startswith("RIGHT") or ju.startswith("FULL"):
                join_outer += 1

        for clause in ON_CLAUSE_RE.findall(sql):
            if re.search(r"CAST\s*\(.+?\bAS\s+(?:VARCHAR|TEXT|CHAR|STRING)\b", clause, re.IGNORECASE) or TEXT_HINT_RE.search(clause):
                text_join_keys += 1

        for fn in AGG_FUNC_RE.findall(sql):
            agg_total += 1
            if fn.upper().replace("_", "") == "ANYVALUE":
                agg_anyvalue += 1
        if "ANYVALUE_" in sql.upper() or "ANYVALUE_SURROGATE" in sql.upper():
            agg_anyvalue += 1

        for tgt in AGG_TARGET_RE.findall(sql):
            if TEXT_HINT_RE.search(tgt):
                agg_text_targets += 1

        if NULL_RE.search(sql):
            null_stmt += 1
        if TEXT_HINT_RE.search(sql):
            str_stmt += 1
        if TIMESTAMP_RE.search(sql):
            ts_stmt += 1
        if BOOLEAN_RE.search(sql):
            bool_stmt += 1

    read = total - write
    return {
        "total_statements": total,
        "read_pct": read / total * 100,
        "write_pct": write / total * 100,
        "ctas_pct": ctas / total * 100,
        "metadata_pct": metadata / total * 100,
        "maintenance_pct": maintenance / total * 100,
        "duplicate_pct": dup_ratio,
        "outer_join_pct": (join_outer / join_total * 100) if join_total else 0.0,
        "text_join_key_pct": (text_join_keys / join_total * 100) if join_total else 0.0,
        "agg_anyvalue_pct": (agg_anyvalue / agg_total * 100) if agg_total else 0.0,
        "agg_text_target_pct": (agg_text_targets / agg_total * 100) if agg_total else 0.0,
        "null_stmt_pct": null_stmt / total * 100,
        "string_stmt_pct": str_stmt / total * 100,
        "timestamp_stmt_pct": ts_stmt / total * 100,
        "boolean_stmt_pct": bool_stmt / total * 100,
    }


def profile_metrics(profile_rows: list[dict]) -> dict[str, float]:
    if not profile_rows:
        return {"op_101_1000_pct": 0.0, "expr_11_100_pct": 0.0}
    total = len(profile_rows)
    op_bucket = sum(1 for r in profile_rows if 101 <= r["op_count"] <= 1000)
    expr_bucket = sum(1 for r in profile_rows if 11 <= r["expr_depth"] <= 100)
    return {
        "op_101_1000_pct": op_bucket / total * 100,
        "expr_11_100_pct": expr_bucket / total * 100,
    }


def metric_line(label: str, observed: float, target: str) -> str:
    return f"| {label} | {observed:.2f}% | {target} |"


def main() -> None:
    tpcds_queries = load_tpcds_queries()
    tpch_queries = load_tpch_queries()

    statements: list[str] = []
    for q in tpcds_queries + tpch_queries:
        statements.extend(split_statements(q))

    stmt_m = statement_metrics(statements)

    tpcds_profile = load_profile_rows(ROOT / "TPC-DS" / "logs" / "duckdb" / "sf1" / "profile", 99)
    tpch_profile = load_profile_rows(ROOT / "TPC-H" / "logs" / "duckdb" / "sf1" / "profile", 22)
    prof_m = profile_metrics(tpcds_profile + tpch_profile)

    out_dir = ROOT / "logs" / "combined"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "target-comparison.md"
    json_path = out_dir / "target-comparison.json"

    lines: list[str] = []
    lines.append("# Combined TPC-DS + TPC-H Target Comparison")
    lines.append("")
    lines.append("| Characteristic | Observed | Target |")
    lines.append("|:--|--:|:--|")
    lines.append(metric_line("Read workload share", stmt_m["read_pct"], "40-60% read"))
    lines.append(metric_line("Write workload share", stmt_m["write_pct"], "40-60% write"))
    lines.append(metric_line("CTAS share", stmt_m["ctas_pct"], "~1.9%"))
    lines.append(metric_line("Metadata query share", stmt_m["metadata_pct"], "~31%"))
    lines.append(metric_line("System maintenance share", stmt_m["maintenance_pct"], "~10%"))
    lines.append(metric_line("Exact repetition share", stmt_m["duplicate_pct"], "up to 80%"))
    lines.append(metric_line("Outer join share", stmt_m["outer_join_pct"], "~37%"))
    lines.append(metric_line("Text join key share", stmt_m["text_join_key_pct"], "~46%"))
    lines.append(metric_line("Aggregation on text share", stmt_m["agg_text_target_pct"], "~34%"))
    lines.append(metric_line("ANYVALUE aggregation share", stmt_m["agg_anyvalue_pct"], "~58%"))
    lines.append(metric_line("Operator-count bucket (101-1000)", prof_m["op_101_1000_pct"], "~13% of queries"))
    lines.append(metric_line("Expression-depth bucket (11-100)", prof_m["expr_11_100_pct"], "~12% of queries"))
    lines.append(metric_line("Statements with null handling", stmt_m["null_stmt_pct"], "high frequency"))
    lines.append(metric_line("Statements using string types", stmt_m["string_stmt_pct"], "varchar-dominant"))
    lines.append(metric_line("Statements using timestamp", stmt_m["timestamp_stmt_pct"], "frequent"))
    lines.append(metric_line("Statements using boolean", stmt_m["boolean_stmt_pct"], "frequent"))
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Data-skew and real null-density targets depend on physical data distribution; this report evaluates query-level proxies only.")
    lines.append("- Metrics are computed from augmented generated SQL statement streams and DuckDB profile operator trees at SF1.")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    payload = {
        "statement_metrics": stmt_m,
        "profile_metrics": prof_m,
        "total_queries": {"tpcds": 99, "tpch": 22},
        "total_statements": len(statements),
        "report_path": str(out_path),
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Report written to {out_path}")
    print(f"JSON written to {json_path}")


if __name__ == "__main__":
    main()
