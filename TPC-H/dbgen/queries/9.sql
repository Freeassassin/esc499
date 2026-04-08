-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
:x
:o
SELECT * FROM information_schema.tables CROSS JOIN ( select
	nation,
	o_year,
	COUNT(CAST((amount) AS VARCHAR)) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%:1%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc ) subq;
SELECT 1;
:n -1
