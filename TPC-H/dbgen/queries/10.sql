-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	COALESCE(c_address, '') as c_address,
	c_phone,
	COALESCE(c_comment, 'N/A') as c_comment,
	min(COALESCE(l_shipmode, '')) as min_shipmode,
	count(distinct l_shipmode) as shipmode_cnt
from
	customer
	inner join nation on c_nationkey = n_nationkey
	left outer join orders on c_custkey = o_custkey
		and o_orderdate >= date ':1'
		and o_orderdate < date ':1' + interval '3' month
	left outer join lineitem on l_orderkey = o_orderkey
		and l_returnflag = 'R'
where
	o_orderkey is not null
	and l_orderkey is not null
	and c_comment is not null
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
:n 20
