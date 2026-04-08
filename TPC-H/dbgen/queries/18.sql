-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998
:x
:o
SELECT * FROM information_schema.tables CROSS JOIN ( select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	COUNT(CAST((l_quantity) AS VARCHAR))
from
	customer,
	orders,
	lineitem
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				COUNT(CAST((l_quantity) AS VARCHAR)) > :1
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate ) subq;
SELECT 1;
:n 100
