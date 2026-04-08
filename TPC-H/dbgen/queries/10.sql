-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
:x
:o
SELECT * FROM information_schema.tables CROSS JOIN ( select
	c_custkey,
	c_name,
	COUNT(CAST((l_extendedprice * (1 - l_discount)) AS VARCHAR)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc ) subq;
SELECT 1;
:n 20
