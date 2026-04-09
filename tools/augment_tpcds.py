#!/usr/bin/env python3
"""Augment TPC-DS query templates for real-world workload characteristics.

Safe augmentation strategy v2:
- Insert LEFT OUTER JOIN on unused dimensions (handles table aliases)
- Auto-detect text columns in main SELECT for count(distinct)
- Auto-detect WHERE columns for IS NOT NULL
- Wrap existing sum() in COALESCE
- NULLIF for explicit divisions (handles table.column references)
- Deepen CASE expressions in selected queries

Key safety rules:
- Never reference columns from the newly joined table in SELECT/WHERE
- Auto-detect columns already in scope rather than hard-coded mappings
- Handle table aliases when inserting JOINs
- Skip queries with complex subquery structures
"""
from __future__ import annotations

import re
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "TPC-DS" / "query_templates"

# ---------------------------------------------------------------------------
# Outer join insertion: fact_table -> [(join_sql, dim_table)]
# store_sales has NO ship_mode FK; only catalog_sales and web_sales do
# ---------------------------------------------------------------------------
OUTER_JOIN_OPTIONS: dict[str, list[tuple[str, str]]] = {
    "store_sales": [
        ("left outer join promotion on ss_promo_sk = p_promo_sk", "promotion"),
    ],
    "catalog_sales": [
        ("left outer join ship_mode on cs_ship_mode_sk = sm_ship_mode_sk", "ship_mode"),
        ("left outer join promotion on cs_promo_sk = p_promo_sk", "promotion"),
    ],
    "web_sales": [
        ("left outer join web_page on ws_web_page_sk = wp_web_page_sk", "web_page"),
        ("left outer join promotion on ws_promo_sk = p_promo_sk", "promotion"),
    ],
    "store_returns": [
        ("left outer join reason on sr_reason_sk = r_reason_sk", "reason"),
    ],
    "catalog_returns": [
        ("left outer join reason on cr_reason_sk = r_reason_sk", "reason"),
    ],
    "web_returns": [
        ("left outer join reason on wr_reason_sk = r_reason_sk", "reason"),
    ],
}

FACT_PRIORITY = [
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns",
]

# Skip outer join for complex multi-CTE / self-join / set-op queries
SKIP_OUTER_JOIN: set[int] = {
    4, 5, 11, 14, 23, 28, 33, 38, 41, 44, 49, 51, 56, 58, 60,
    64, 72, 75, 77, 78, 80, 83, 87, 88, 93, 95, 97,
}

# SQL keywords that should not be mistaken for table aliases
_KEYWORDS = (
    r"where|and|or|on|left|right|inner|outer|join|cross|full|natural|"
    r"group|order|having|limit|union|intersect|except|with|as|select|"
    r"from|into|between|in|not|is|null|when|then|else|end|case|set"
)

# Known varchar dimension columns across all TPC-DS dimension tables
TEXT_DIM_COLS = [
    "i_item_id", "i_item_desc", "i_brand", "i_class", "i_category", "i_manufact",
    "s_store_name", "s_store_id", "s_state", "s_company_name",
    "c_last_name", "c_first_name", "c_customer_id",
    "ca_city", "ca_state", "ca_zip", "ca_county",
    "cd_gender", "cd_education_status", "cd_marital_status",
    "w_warehouse_name", "w_warehouse_id",
    "cc_name", "cc_class",
    "sm_type", "sm_carrier",
    "r_reason_desc",
    "p_promo_name",
    "web_name",
]

# Skip text agg / null handling for queries with complex scoping
SKIP_TEXT_AGG: set[int] = {
    4, 5, 11, 14, 18, 22, 23, 24, 27, 28, 31, 33, 35, 36, 38, 41, 44, 47, 49,
    51, 56, 57, 58, 60, 64, 70, 72, 74, 75, 77, 78, 80, 83, 86, 87, 93, 95, 97,
}

DEEPEN_QUERIES: set[int] = {14, 23, 24, 28, 39, 47, 57, 64, 67, 78, 85, 95}


def detect_tables(content: str) -> set[str]:
    all_tables = set(FACT_PRIORITY) | {
        "promotion", "ship_mode", "web_page", "reason", "warehouse",
        "date_dim", "item", "store", "customer", "customer_address",
        "customer_demographics", "household_demographics", "call_center",
        "web_site", "catalog_page", "time_dim", "income_band",
    }
    found = set()
    lower = content.lower()
    for t in all_tables:
        if re.search(r"\b" + re.escape(t) + r"\b", lower):
            found.add(t)
    return found


def insert_outer_join(content: str, qnum: int) -> str:
    if qnum in SKIP_OUTER_JOIN:
        return content

    tables = detect_tables(content)

    # Alias pattern: optional word on same line that isn't a SQL keyword
    alias_re = r"(?:[ \t]+(?!(?:" + _KEYWORDS + r")\b)[a-z]\w*)?"

    for fact in FACT_PRIORITY:
        if fact not in tables:
            continue
        options = OUTER_JOIN_OPTIONS.get(fact, [])
        for join_clause, dim_name in options:
            if dim_name in tables:
                continue

            # Pattern 1: "from fact_table [alias]"
            pat1 = re.compile(
                r"(\bfrom\s+" + re.escape(fact) + r")" + alias_re,
                re.IGNORECASE,
            )
            m = pat1.search(content)
            if m:
                before = content[: m.start()]
                depth = before.count("(") - before.count(")")
                if depth <= 2:
                    # Check text after match doesn't start with JOIN keyword
                    after = content[m.end():m.end() + 30].strip().lower()
                    if any(after.startswith(kw) for kw in [
                        "left", "right", "inner", "outer", "join",
                        "full", "natural", "cross",
                    ]):
                        break  # fact table already has explicit JOIN
                    pos = m.end()
                    content = (
                        content[:pos] + "\n     " + join_clause + content[pos:]
                    )
                    return content

            # Pattern 2: ",fact_table [alias]"
            pat2 = re.compile(
                r"(,\s*" + re.escape(fact) + r")" + alias_re,
                re.IGNORECASE,
            )
            m2 = pat2.search(content)
            if m2:
                before = content[: m2.start()]
                depth = before.count("(") - before.count(")")
                if depth <= 2:
                    after = content[m2.end():m2.end() + 30].strip().lower()
                    if any(after.startswith(kw) for kw in [
                        "left", "right", "inner", "outer", "join",
                        "full", "natural", "cross",
                    ]):
                        break
                    pos = m2.end()
                    content = (
                        content[:pos] + "\n     " + join_clause + content[pos:]
                    )
                    return content
        break  # Only try first matching fact table

    return content


def _find_main_select_text_col(content: str) -> str | None:
    """Find a varchar dimension column in the main SELECT that also appears in GROUP BY."""
    limitb = re.search(r"\[_LIMITB\]", content)
    if not limitb:
        return None
    after = content[limitb.end():]
    from_match = re.search(r"\bfrom\b", after, re.IGNORECASE)
    if not from_match:
        return None
    select_text = after[: from_match.start()]

    # Find GROUP BY at depth 0 (relative to [_LIMITB] position)
    limitb_depth = content[: limitb.start()].count("(") - content[: limitb.start()].count(")")
    rest = after[from_match.end():]
    group_text = None
    for gm in re.finditer(r"\bgroup\s+by\b", rest, re.IGNORECASE):
        before_abs = content[: limitb.end() + from_match.end() + gm.start()]
        depth = before_abs.count("(") - before_abs.count(")")
        if depth == limitb_depth:
            gt = rest[gm.end():]
            end = re.search(r"\border\s+by\b|\bhaving\b|\[_LIMITC\]", gt, re.IGNORECASE)
            group_text = gt[: end.start()] if end else gt[:500]
            break

    if group_text is None:
        return None  # No GROUP BY at outer level → can't add aggregate safely

    for col in TEXT_DIM_COLS:
        if re.search(r"\b" + re.escape(col) + r"\b", select_text, re.IGNORECASE):
            if re.search(r"\b" + re.escape(col) + r"\b", group_text, re.IGNORECASE):
                return col
    return None


def _find_main_where_col(content: str) -> str | None:
    """Find a dimension column in the outermost WHERE clause (depth 0 only)."""
    for m in re.finditer(r"\bwhere\b", content, re.IGNORECASE):
        before = content[: m.start()]
        depth = before.count("(") - before.count(")")
        if depth != 0:
            continue
        after = content[m.end():]
        end = re.search(
            r"\bgroup\s+by\b|\border\s+by\b|\[_LIMITC\]", after, re.IGNORECASE
        )
        if end:
            # Verify the anchor is also at depth 0
            end_depth = (depth
                         + after[: end.start()].count("(")
                         - after[: end.start()].count(")"))
            if end_depth != 0:
                # Try to find next anchor at depth 0
                end = None
                for a in [r"\bgroup\s+by\b", r"\border\s+by\b", r"\[_LIMITC\]"]:
                    for am in re.finditer(a, after, re.IGNORECASE):
                        ad = (depth
                              + after[: am.start()].count("(")
                              - after[: am.start()].count(")"))
                        if ad == 0:
                            end = am
                            break
                    if end:
                        break
        where_text = after[: end.start()] if end else after[:600]
        for col in TEXT_DIM_COLS:
            if re.search(r"\b" + re.escape(col) + r"\b", where_text, re.IGNORECASE):
                if not re.search(
                    r"\b" + re.escape(col) + r"\s+is\s+not\s+null\b",
                    where_text,
                    re.IGNORECASE,
                ):
                    return col
        break  # Only check first outermost WHERE
    return None


def add_text_aggregation(content: str, qnum: int) -> str:
    if qnum in SKIP_TEXT_AGG:
        return content

    col = _find_main_select_text_col(content)
    if not col:
        return content

    agg_expr = f"count(distinct {col}) as cnt_distinct_{col}"

    limitb = re.search(r"\[_LIMITB\]", content)
    if limitb:
        after = content[limitb.end():]
        from_match = re.search(r"\n\s*from\b", after, re.IGNORECASE)
        if from_match:
            pos = limitb.end() + from_match.start()
            content = content[:pos] + "\n ," + agg_expr + content[pos:]
            return content

    return content


def add_null_handling(content: str, qnum: int) -> str:
    if qnum in SKIP_TEXT_AGG:
        return content

    col = _find_main_where_col(content)
    if not col:
        return content

    for anchor in [r"\bgroup\s+by\b", r"\border\s+by\b", r"\[_LIMITC\]"]:
        for match in re.finditer(anchor, content, re.IGNORECASE):
            before = content[: match.start()]
            depth = before.count("(") - before.count(")")
            if depth == 0:
                pos = match.start()
                content = (
                    content[:pos] + f"and {col} is not null\n " + content[pos:]
                )
                return content

    return content


def wrap_sum_with_coalesce(content: str) -> str:
    count = 0

    def replacer(m: re.Match) -> str:
        nonlocal count
        inner = m.group(1).strip()
        if count >= 2:
            return m.group(0)
        if "coalesce" in inner.lower() or "case" in inner.lower():
            return m.group(0)
        if re.match(r"^[a-z_][a-z0-9_.]*$", inner, re.IGNORECASE):
            count += 1
            return f"sum(COALESCE({inner}, 0))"
        return m.group(0)

    return re.sub(r"\bsum\(([^()]+?)\)", replacer, content, flags=re.IGNORECASE)


def add_nullif_divisions(content: str) -> str:
    """Wrap denominators in NULLIF(x, 0). Handles both col and table.col."""
    _skip_words = frozenset({
        "nullif", "cast", "sum", "count", "avg", "min", "max",
        "case", "when", "then", "else", "end", "select", "from",
        "decimal", "integer", "varchar", "date", "float", "double",
        "coalesce", "abs", "round", "trim", "substring",
    })

    def replacer(m: re.Match) -> str:
        denom = m.group(1).strip()
        root = denom.split(".")[0].lower()
        if root in _skip_words or "(" in denom:
            return m.group(0)
        return f"/ NULLIF({denom}, 0)"

    # Match "/ identifier" or "/ table.column"
    return re.sub(
        r"/\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)\b",
        replacer,
        content,
        count=2,
        flags=re.IGNORECASE,
    )


def deepen_expressions(content: str, qnum: int) -> str:
    if qnum not in DEEPEN_QUERIES:
        return content

    def deep_case(m: re.Match) -> str:
        col = m.group(1).strip()
        if "case" in col.lower() or "coalesce" in col.lower():
            return m.group(0)
        if not re.match(r"^[a-z_][a-z0-9_.]*$", col, re.IGNORECASE):
            return m.group(0)
        return (
            f"sum(CASE WHEN {col} > 0 "
            f"THEN CASE WHEN {col} < 1000000 THEN {col} ELSE 0 END "
            f"ELSE COALESCE({col}, 0) END)"
        )

    return re.sub(
        r"\bsum\(([a-z_][a-z0-9_.]*)\)",
        deep_case,
        content,
        count=1,
        flags=re.IGNORECASE,
    )


def augment_template(content: str, qnum: int) -> str:
    content = insert_outer_join(content, qnum)
    content = add_text_aggregation(content, qnum)
    content = add_null_handling(content, qnum)
    content = wrap_sum_with_coalesce(content)
    content = add_nullif_divisions(content)
    content = deepen_expressions(content, qnum)
    return content


def main() -> None:
    if not TEMPLATES_DIR.is_dir():
        raise FileNotFoundError(f"Templates dir not found: {TEMPLATES_DIR}")

    modified = 0
    outer_added = 0
    for i in range(1, 100):
        path = TEMPLATES_DIR / f"query{i}.tpl"
        if not path.exists():
            continue

        original = path.read_text(encoding="utf-8")
        augmented = augment_template(original, i)

        if augmented != original:
            path.write_text(augmented, encoding="utf-8")
            changes = []
            if "left outer join" in augmented.lower() and \
               "left outer join" not in original.lower():
                changes.append("outer-join")
                outer_added += 1
            if "is not null" in augmented.lower() and \
               "is not null" not in original.lower():
                changes.append("null-check")
            if "coalesce" in augmented.lower() and \
               "coalesce" not in original.lower():
                changes.append("coalesce")
            if "cnt_distinct_" in augmented:
                changes.append("count-distinct")
            if "NULLIF(" in augmented and "NULLIF(" not in original:
                changes.append("nullif")
            print(f"  Q{i}: {', '.join(changes) if changes else 'minor'}")
            modified += 1
        else:
            print(f"  Q{i}: (no change)")

    already_outer = sum(
        1 for i in range(1, 100)
        if (TEMPLATES_DIR / f"query{i}.tpl").exists()
        and "left outer join" in (TEMPLATES_DIR / f"query{i}.tpl").read_text().lower()
    )

    print(f"\nDone. Modified {modified}/99 templates.")
    print(f"Templates with LEFT OUTER JOIN: {already_outer}/99")
    print(f"  (New outer joins added: {outer_added})")


if __name__ == "__main__":
    main()
