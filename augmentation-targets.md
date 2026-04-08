This is a condensed markdown table of real-world workload characteristics missing from TPC-H/DS.


| Characteristic | TPC-H/DS Benchmark Value | Real-World Target Value (Redshift/Snowflake) | Note for Augmentation |
| :--- | :--- | :--- | :--- |
| **Workload Mix** | Read-mostly (~80% `SELECT`) | ~40-60% Read/Write queries | Introduce `INSERT`, `COPY`, `DELETE`, `UPDATE` statements. |
| **CTAS Statements** | None | Present; ~1.9% of queries, 16.4% of load | Generate `CREATE TABLE AS SELECT` statements as part of the workload. |
| **Metadata Queries** | None | 31% of all statements are `SHOW` commands | Inject schema and metadata queries into the workload. |
| **System Maintenance**| None | ~10% of all queries | Add system-related commands (e.g., maintenance operations). |
| **Query Repetition** | None per run | Up to 80% of queries are exact repetitions | A large fraction of the generated query set should be duplicates. |
| **Operator Count** | Low; avg. 9-20 operators | High; 13% of queries have 101-1000 operators | Increase the number of operators (scans, joins, etc.) significantly for a subset of queries. |
| **Expression Depth** | Shallow; <0.1% of expressions have depth >10 | Deep; 12% of expressions have nesting depth of 11-100 | Increase the nesting level of expressions, especially in `WHERE` and `SELECT` clauses. |
| **Join Type** | Mostly Inner Joins (97% in TPC-DS) | 37% Outer Joins | Change a significant portion of `INNER JOIN` to `LEFT/RIGHT/FULL OUTER JOIN`. |
| **Join Key Type** | Mostly Numeric Keys (81% in TPC-DS) | 46% Text-based Keys | Use `varchar` or `text` columns as join keys. |
| **Aggregation Target**| Almost exclusively on `number` types (98%) | 34% of aggregations are on `text` columns | Apply aggregation functions (`COUNT`, `GROUP BY`) to non-numeric columns. |
| **Aggregation Func.**| `sum` is dominant (64%) | `anyvalue` is dominant (58%) | Use a wider variety of aggregation functions, especially `anyvalue`. |
| **Data Skew** | Low / Uniform | High (Q-Error up to 10^239) | Generate predicates that target highly skewed value distributions. |
| **Data Types** | `int`/`date` are common | `varchar` is dominant (52.1%) | Queries should frequently operate on `varchar`, `timestamp`, and `boolean` columns. |
| **Null Values** | None (TPC-H) or very few | High frequency; columns with >99% nulls | Generate predicates that explicitly handle nulls (`IS NULL`, `IS NOT NULL`, `COALESCE`). |