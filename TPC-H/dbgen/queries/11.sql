-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_u_2127 AS SELECT *, -1 as _dummy_update_col FROM ( select
	ps_partkey,
	COUNT(CAST((ps_supplycost * ps_availqty) AS VARCHAR)) as value
from
	partsupp,
	supplier,
	nation
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = ':1'
group by
	ps_partkey having
		COUNT(CAST((ps_supplycost * ps_availqty) AS VARCHAR)) > (
			select
				COUNT(CAST((ps_supplycost * ps_availqty) AS VARCHAR)) * :2
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = ':1'
		)
order by
	value desc ) subq;
UPDATE tmp_u_2127 SET _dummy_update_col = 1 WHERE 1=0;
SELECT 1;
:n -1
