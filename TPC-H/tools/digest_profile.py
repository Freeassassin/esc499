#!/usr/bin/env python3
"""Digest DuckDB JSON profiling output for TPC-H into a Markdown report."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

TPCH_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Operator-tree helpers
# ---------------------------------------------------------------------------

def collect_operators(node: dict) -> list[tuple[str, float, int]]:
    """Walk the operator tree and collect (type, timing, cardinality) tuples."""
    ops: list[tuple[str, float, int]] = []
    otype = node.get("operator_type", "")
    timing = node.get("operator_timing", 0.0)
    card = node.get("operator_cardinality", 0)
    if otype:
        ops.append((otype, timing, card))
    for child in node.get("children", []):
        ops.extend(collect_operators(child))
    return ops


def count_operators(node: dict) -> int:
    """Count all operators in the tree."""
    n = 1 if node.get("operator_type") else 0
    for child in node.get("children", []):
        n += count_operators(child)
    return n


def tree_depth(node: dict) -> int:
    """Max depth of the operator tree."""
    child_depths = [tree_depth(c) for c in node.get("children", [])]
    return 1 + max(child_depths, default=0)


def collect_join_info(node: dict) -> list[dict]:
    """Collect join type + conditions from the operator tree."""
    results: list[dict] = []
    otype = node.get("operator_type", "")
    extra = node.get("extra_info", {})
    if "JOIN" in otype:
        results.append({
            "operator": otype,
            "join_type": extra.get("Join Type", ""),
            "conditions": extra.get("Conditions", ""),
        })
    for child in node.get("children", []):
        results.extend(collect_join_info(child))
    return results


def collect_tables(node: dict) -> list[str]:
    """Collect scanned table names."""
    tables: list[str] = []
    if node.get("operator_type") == "TABLE_SCAN":
        tbl = node.get("extra_info", {}).get("Table", "")
        if tbl:
            tables.append(tbl.split(".")[-1])
    for child in node.get("children", []):
        tables.extend(collect_tables(child))
    return tables


# ---------------------------------------------------------------------------
# SQL-level analysis helpers
# ---------------------------------------------------------------------------

AGG_RE = re.compile(r"\b(sum|count|avg|min|max|stddev|variance|count\s*\(\s*distinct)\s*\(", re.IGNORECASE)
GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
HAVING_RE = re.compile(r"\bHAVING\b", re.IGNORECASE)
WINDOW_RE = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
DISTINCT_RE = re.compile(r"\bDISTINCT\b", re.IGNORECASE)
EXISTS_RE = re.compile(r"\bEXISTS\b", re.IGNORECASE)
UNION_RE = re.compile(r"\bUNION\b", re.IGNORECASE)
SUBQUERY_RE = re.compile(r"\(\s*SELECT\b", re.IGNORECASE)
CREATE_RE = re.compile(r"\bCREATE\b", re.IGNORECASE)
CTAS_RE = re.compile(r"\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\b.*?\bAS\s+SELECT\b", re.IGNORECASE | re.DOTALL)
CREATE_VIEW_RE = re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\b", re.IGNORECASE)
DROP_RE = re.compile(r"\bDROP\b", re.IGNORECASE)
INSERT_RE = re.compile(r"\bINSERT\b", re.IGNORECASE)
UPDATE_RE = re.compile(r"\bUPDATE\b", re.IGNORECASE)
DELETE_RE = re.compile(r"\bDELETE\b", re.IGNORECASE)
NULL_RE = re.compile(r"\b(IS\s+NULL|IS\s+NOT\s+NULL|COALESCE|NULLIF|IFNULL)\b", re.IGNORECASE)
CASE_RE = re.compile(r"\bCASE\b", re.IGNORECASE)
LIKE_RE = re.compile(r"\bLIKE\b", re.IGNORECASE)
IN_RE = re.compile(r"\bIN\s*\(", re.IGNORECASE)
BETWEEN_RE = re.compile(r"\bBETWEEN\b", re.IGNORECASE)
CAST_RE = re.compile(r"\bCAST\s*\(", re.IGNORECASE)
DATE_RE = re.compile(r"\b(DATE|TIMESTAMP|INTERVAL)\b", re.IGNORECASE)
DECIMAL_RE = re.compile(r"\b(DECIMAL|NUMERIC|REAL|FLOAT|DOUBLE)\b", re.IGNORECASE)
VARCHAR_RE = re.compile(r"\b(VARCHAR|CHAR|TEXT|STRING)\b", re.IGNORECASE)
INTEGER_RE = re.compile(r"\b(INT|INTEGER|BIGINT|SMALLINT|TINYINT)\b", re.IGNORECASE)

JOIN_SQL_RE = re.compile(
    r"\b(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|"
    r"FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|(?:LEFT\s+)?SEMI\s+JOIN|"
    r"(?:LEFT\s+)?ANTI\s+JOIN|NATURAL\s+JOIN|JOIN)\b",
    re.IGNORECASE,
)

SYSTEM_MAINT_RE = re.compile(r"\b(VACUUM|ANALYZE|REINDEX|CHECKPOINT|PRAGMA)\b", re.IGNORECASE)
METADATA_RE = re.compile(
    r"\b(information_schema|pg_catalog|pg_class|pg_tables|pg_stat|"
    r"duckdb_tables|duckdb_columns|duckdb_schemas)\b",
    re.IGNORECASE,
)


def classify_workload(sql: str) -> str:
    """Classify the query workload type."""
    upper = sql.strip().upper()
    if upper.startswith("INSERT"):
        return "DML-Insert"
    if upper.startswith("UPDATE"):
        return "DML-Update"
    if upper.startswith("DELETE"):
        return "DML-Delete"
    if CTAS_RE.search(sql):
        return "CTAS"
    if CREATE_VIEW_RE.search(sql):
        return "DDL+Analytical"
    if CREATE_RE.search(sql) or DROP_RE.search(sql):
        return "DDL"
    if WINDOW_RE.search(sql) and GROUP_BY_RE.search(sql):
        return "Analytical+Aggregation"
    if WINDOW_RE.search(sql):
        return "Analytical"
    if GROUP_BY_RE.search(sql):
        return "Aggregation"
    if EXISTS_RE.search(sql) or SUBQUERY_RE.search(sql):
        return "Nested/Correlated"
    return "Scan/Filter/Join"


def extract_join_key_columns(sql: str) -> list[str]:
    """Extract column names used in ON join conditions."""
    on_clauses = re.findall(r"\bON\s+(.+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bJOIN\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bFULL\b|\bCROSS\b|$)", sql, re.IGNORECASE | re.DOTALL)
    cols: list[str] = []
    for clause in on_clauses:
        cols.extend(re.findall(r"(\w+\.\w+|\b\w+key\b|\b\w+_sk\b|\b\w+_id\b)", clause, re.IGNORECASE))
    return cols


def extract_null_patterns(sql: str) -> list[str]:
    return sorted({m.upper() for m in NULL_RE.findall(sql)})


def extract_agg_targets(sql: str) -> list[str]:
    """Extract what's inside aggregation function calls."""
    targets = re.findall(r"\b(?:sum|avg|count|min|max)\s*\(\s*(?:distinct\s+)?(.+?)\)", sql, re.IGNORECASE)
    clean: list[str] = []
    for t in targets:
        t = t.strip()
        if t == "*":
            clean.append("*")
        else:
            # Take the first identifier/expression
            m = re.match(r"[\w.]+", t)
            if m:
                clean.append(m.group())
    return sorted(set(clean))


def extract_data_types(sql: str) -> list[str]:
    """Identify data type categories referenced in CAST/literals/schema."""
    types: set[str] = set()
    if DATE_RE.search(sql) or re.search(r"\bdate\s*'", sql, re.IGNORECASE):
        types.add("date/time")
    if DECIMAL_RE.search(sql):
        types.add("decimal/float")
    if VARCHAR_RE.search(sql) or LIKE_RE.search(sql):
        types.add("string")
    if INTEGER_RE.search(sql) or re.search(r"\bCAST\s*\(.+?\bAS\s+INT", sql, re.IGNORECASE):
        types.add("integer")
    return sorted(types)


def detect_data_skew_indicators(data: dict) -> str:
    """Compare estimated vs actual cardinality to flag potential skew."""
    mismatches = 0
    total = 0

    def walk(node: dict) -> None:
        nonlocal mismatches, total
        extra = node.get("extra_info", {})
        est_str = extra.get("Estimated Cardinality", "")
        actual = node.get("operator_cardinality", 0)
        if est_str and actual > 0:
            try:
                est = int(est_str)
                total += 1
                ratio = max(actual, est) / max(min(actual, est), 1)
                if ratio > 10:
                    mismatches += 1
            except (ValueError, TypeError):
                pass
        for child in node.get("children", []):
            walk(child)

    for child in data.get("children", []):
        walk(child)
    if total == 0:
        return "—"
    pct = mismatches / total * 100
    if pct > 30:
        return f"High ({mismatches}/{total} ops >10× off)"
    if pct > 10:
        return f"Moderate ({mismatches}/{total})"
    return f"Low ({mismatches}/{total})"


# ---------------------------------------------------------------------------
# Analyze a single query profile
# ---------------------------------------------------------------------------

def analyze_query(query_id: int, data: dict) -> dict:
    sql = data.get("query_name", "")

    # Operator tree stats
    op_count = sum(count_operators(c) for c in data.get("children", []))
    expr_depth = max((tree_depth(c) for c in data.get("children", [])), default=0)

    # Operators
    all_ops: list[tuple[str, float, int]] = []
    for child in data.get("children", []):
        all_ops.extend(collect_operators(child))
    top_ops = sorted(all_ops, key=lambda x: x[1], reverse=True)[:3]

    # Joins from profile tree
    join_info: list[dict] = []
    for child in data.get("children", []):
        join_info.extend(collect_join_info(child))
    join_types_plan = sorted({f"{j['join_type']} {j['operator']}" for j in join_info}) if join_info else []

    # Join key columns from conditions
    join_key_cols: list[str] = []
    for j in join_info:
        cond = j.get("conditions", "")
        if isinstance(cond, list):
            cond = " AND ".join(str(c) for c in cond)
        if cond:
            cols = re.findall(r"\b(\w+)\s*(?:=|IS NOT DISTINCT FROM)", str(cond))
            join_key_cols.extend(cols)
    join_key_types: list[str] = []
    for col in join_key_cols:
        col_lower = col.lower()
        if col_lower.endswith(("key", "_sk", "_id", "key")):
            join_key_types.append("surrogate/FK")
        elif col_lower.endswith(("date",)):
            join_key_types.append("date")
        elif col_lower.endswith(("name", "type", "status", "flag")):
            join_key_types.append("string")
        else:
            join_key_types.append("integer")
    join_key_types_str = ", ".join(sorted(set(join_key_types))) if join_key_types else "—"

    # SQL-level analysis
    agg_funcs = sorted({m.upper() for m in AGG_RE.findall(sql)})
    agg_targets = extract_agg_targets(sql)
    workload = classify_workload(sql)
    has_ctas = bool(CTAS_RE.search(sql))
    has_create_view = bool(CREATE_VIEW_RE.search(sql))
    has_metadata = bool(METADATA_RE.search(sql))
    has_maintenance = bool(SYSTEM_MAINT_RE.search(sql))
    null_patterns = extract_null_patterns(sql)
    data_types = extract_data_types(sql)
    skew = detect_data_skew_indicators(data)

    # Subquery count
    subq_count = len(SUBQUERY_RE.findall(sql))

    return {
        "query": query_id,
        "latency": data.get("latency", 0.0),
        "cpu_time": data.get("cpu_time", 0.0),
        "rows_returned": data.get("rows_returned", 0),
        "rows_scanned": data.get("cumulative_rows_scanned", 0),
        "top_ops": top_ops,
        "op_count": op_count,
        "expr_depth": expr_depth,
        "workload": workload,
        "ctas": has_ctas,
        "create_view": has_create_view,
        "metadata_query": has_metadata,
        "maintenance": has_maintenance,
        "join_types": join_types_plan,
        "join_key_types": join_key_types_str,
        "agg_funcs": agg_funcs,
        "agg_targets": agg_targets,
        "null_patterns": null_patterns,
        "data_types": data_types,
        "data_skew": skew,
        "subquery_count": subq_count,
    }


def format_time(seconds: float) -> str:
    if seconds >= 1.0:
        return f"{seconds:.3f}s"
    return f"{seconds * 1000:.2f}ms"


def fmt_list(items: list[str], fallback: str = "—") -> str:
    return ", ".join(items) if items else fallback


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Digest TPC-H DuckDB profiling output into a Markdown report")
    parser.add_argument("--scale", default="1")
    args = parser.parse_args()

    profile_dir = TPCH_ROOT / "logs" / "duckdb" / f"sf{args.scale}" / "profile"
    if not profile_dir.exists():
        raise FileNotFoundError(f"Profile directory not found: {profile_dir}. Run profile_duckdb.py first.")

    rows: list[dict] = []
    for i in range(1, 23):
        json_path = profile_dir / f"q{i}.json"
        if not json_path.exists():
            rows.append({"query": i, "error": "missing"})
            continue
        data = json.loads(json_path.read_text(encoding="utf-8"))
        rows.append(analyze_query(i, data))

    rows_sorted = sorted(rows, key=lambda r: r.get("latency") or 0, reverse=True)
    valid = [r for r in rows if "error" not in r]

    lines: list[str] = []
    lines.append(f"# TPC-H DuckDB Profiling Report (SF={args.scale})")
    lines.append("")
    total_latency = sum(r["latency"] for r in valid)
    total_cpu = sum(r["cpu_time"] for r in valid)
    lines.append(f"**Total latency**: {format_time(total_latency)} | **Total CPU time**: {format_time(total_cpu)} | **Queries**: {len(rows)}")
    lines.append("")

    # --- Section 1: Performance summary ---
    lines.append("## Query Performance (sorted by latency descending)")
    lines.append("")
    lines.append("| Query | Latency | CPU Time | Rows Returned | Rows Scanned | Top Operators |")
    lines.append("|------:|--------:|---------:|--------------:|-------------:|:--------------|")
    for r in rows_sorted:
        if r.get("error"):
            lines.append(f"| Q{r['query']} | — | — | — | — | _{r['error']}_ |")
            continue
        top_str = ", ".join(f"{op}({format_time(t)})" for op, t, _ in r["top_ops"])
        lines.append(
            f"| Q{r['query']} "
            f"| {format_time(r['latency'])} "
            f"| {format_time(r['cpu_time'])} "
            f"| {r['rows_returned']:,} "
            f"| {r['rows_scanned']:,} "
            f"| {top_str} |"
        )
    lines.append(f"| **Total** | **{format_time(total_latency)}** | **{format_time(total_cpu)}** | | | |")
    lines.append("")

    # --- Section 2: Query characteristics ---
    lines.append("## Query Characteristics")
    lines.append("")
    lines.append("| Query | Workload Mix | CTAS | Metadata | Maintenance | Subqueries | Operator Count | Expression Depth |")
    lines.append("|------:|:-------------|:----:|:--------:|:-----------:|-----------:|---------------:|-----------------:|")
    for r in sorted(valid, key=lambda x: x["query"]):
        ctas_str = "✓ (view)" if r.get("create_view") else ("✓" if r["ctas"] else "—")
        lines.append(
            f"| Q{r['query']} "
            f"| {r['workload']} "
            f"| {ctas_str} "
            f"| {'✓' if r['metadata_query'] else '—'} "
            f"| {'✓' if r['maintenance'] else '—'} "
            f"| {r['subquery_count']} "
            f"| {r['op_count']} "
            f"| {r['expr_depth']} |"
        )
    lines.append("")

    # --- Section 3: Join analysis ---
    lines.append("## Join Analysis")
    lines.append("")
    lines.append("| Query | Join Types (plan) | Join Key Types |")
    lines.append("|------:|:------------------|:---------------|")
    for r in sorted(valid, key=lambda x: x["query"]):
        lines.append(
            f"| Q{r['query']} "
            f"| {fmt_list(r['join_types'])} "
            f"| {r['join_key_types']} |"
        )
    lines.append("")

    # --- Section 4: Aggregation analysis ---
    lines.append("## Aggregation Analysis")
    lines.append("")
    lines.append("| Query | Aggregation Functions | Aggregation Targets |")
    lines.append("|------:|:---------------------|:--------------------|")
    for r in sorted(valid, key=lambda x: x["query"]):
        lines.append(
            f"| Q{r['query']} "
            f"| {fmt_list(r['agg_funcs'])} "
            f"| {fmt_list(r['agg_targets'])} |"
        )
    lines.append("")

    # --- Section 5: Data characteristics ---
    lines.append("## Data Characteristics")
    lines.append("")
    lines.append("| Query | Data Types | Null Handling | Data Skew (est. vs actual) |")
    lines.append("|------:|:-----------|:--------------|:---------------------------|")
    for r in sorted(valid, key=lambda x: x["query"]):
        lines.append(
            f"| Q{r['query']} "
            f"| {fmt_list(r['data_types'])} "
            f"| {fmt_list(r['null_patterns'])} "
            f"| {r['data_skew']} |"
        )
    lines.append("")

    # --- Section 6: Workload mix summary ---
    lines.append("## Workload Mix Summary")
    lines.append("")
    mix: dict[str, int] = {}
    for r in valid:
        mix[r["workload"]] = mix.get(r["workload"], 0) + 1
    for wl, cnt in sorted(mix.items(), key=lambda x: -x[1]):
        lines.append(f"- **{wl}**: {cnt} queries")
    lines.append("")

    # --- Section 7: Query repetition ---
    lines.append("## Query Repetition")
    lines.append("")
    lines.append("All TPC-H queries are unique templates (no repetition within a single stream).")
    lines.append("")

    # --- Section 8: Top 10 slowest operators ---
    lines.append("## Top 10 Slowest Operators Across All Queries")
    lines.append("")
    all_ops_global: list[tuple[int, str, float, int]] = []
    for r in valid:
        json_path = profile_dir / f"q{r['query']}.json"
        data = json.loads(json_path.read_text(encoding="utf-8"))
        for child in data.get("children", []):
            for op, t, card in collect_operators(child):
                all_ops_global.append((r["query"], op, t, card))
    all_ops_global.sort(key=lambda x: x[2], reverse=True)

    lines.append("| Query | Operator | Timing | Cardinality |")
    lines.append("|------:|:---------|-------:|------------:|")
    for query, op, t, card in all_ops_global[:10]:
        lines.append(f"| Q{query} | {op} | {format_time(t)} | {card:,} |")
    lines.append("")

    report_path = profile_dir / "report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()
