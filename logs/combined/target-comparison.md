# Combined TPC-DS + TPC-H Target Comparison

| Characteristic | Observed | Target |
|:--|--:|:--|
| Read workload share | 69.51% | 40-60% read |
| Write workload share | 30.49% | 40-60% write |
| CTAS share | 1.60% | ~1.9% |
| Metadata query share | 45.78% | ~31% |
| System maintenance share | 10.16% | ~10% |
| Exact repetition share | 94.20% | up to 80% |
| Outer join share | 80.23% | ~37% |
| Text join key share | 52.33% | ~46% |
| Aggregation on text share | 11.76% | ~34% |
| ANYVALUE aggregation share | 5.67% | ~58% |
| Operator-count bucket (101-1000) | 0.00% | ~13% of queries |
| Expression-depth bucket (11-100) | 37.19% | ~12% of queries |
| Statements with null handling | 17.43% | high frequency |
| Statements using string types | 13.48% | varchar-dominant |
| Statements using timestamp | 15.88% | frequent |
| Statements using boolean | 0.63% | frequent |

## Notes

- Data-skew and real null-density targets depend on physical data distribution; this report evaluates query-level proxies only.
- Metrics are computed from augmented generated SQL statement streams and DuckDB profile operator trees at SF1.
