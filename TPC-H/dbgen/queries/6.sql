-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_u_5915 AS SELECT *, -1 as _dummy_update_col FROM ( select
	COUNT(CAST((l_extendedprice * l_discount) AS VARCHAR)) as revenue
from
	lineitem
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' year
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3 ) subq;
UPDATE tmp_u_5915 SET _dummy_update_col = 1 WHERE 1=0;
SELECT 1;
:n -1
