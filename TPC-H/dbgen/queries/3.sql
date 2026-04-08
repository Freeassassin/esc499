-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_d_5325 AS SELECT * FROM ( select
	l_orderkey,
	COUNT(CAST((l_extendedprice * (1 - l_discount)) AS VARCHAR)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	c_mktsegment = ':1'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date ':2'
	and l_shipdate > date ':2'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate ) subq;
DELETE FROM tmp_d_5325 WHERE 1=0;
SELECT 1;
:n 10
