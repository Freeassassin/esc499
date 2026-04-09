-- $ID$
-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue,
	min(ship_mode) as min_shipmode,
	count(distinct ship_mode) as shipmode_cnt
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume,
			COALESCE(l_shipmode, '') as ship_mode
		from
			supplier
			inner join lineitem on s_suppkey = l_suppkey
			left outer join orders on o_orderkey = l_orderkey
			left outer join customer on c_custkey = o_custkey
			inner join nation n1 on s_nationkey = n1.n_nationkey
			inner join nation n2 on c_nationkey = n2.n_nationkey
		where
			(
				(n1.n_name = ':1' and n2.n_name = ':2')
				or (n1.n_name = ':2' and n2.n_name = ':1')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
			and o_orderkey is not null
			and COALESCE(l_comment, '') <> ''
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
:n -1
