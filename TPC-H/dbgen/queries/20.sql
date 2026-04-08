-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_d_3426 AS SELECT * FROM ( select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
			)
			and ps_availqty > (
				select
					0.5 * COUNT(CAST((l_quantity) AS VARCHAR))
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
order by
	s_name ) subq;
DELETE FROM tmp_d_3426 WHERE 1=0;
SELECT 1;
:n -1
