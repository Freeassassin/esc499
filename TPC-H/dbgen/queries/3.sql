-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority,
	min(COALESCE(l_comment, '')) as min_comment,
	count(distinct l_shipmode) as shipmode_cnt
from
	customer
	left outer join orders on c_custkey = o_custkey
	left outer join lineitem on l_orderkey = o_orderkey
where
	c_mktsegment = ':1'
	and o_orderdate < date ':2'
	and l_shipdate > date ':2'
	and o_orderkey is not null
	and l_orderkey is not null
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
:n 10
