-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	nation,
	o_year,
	sum(amount) as sum_profit,
	min(part_name) as min_part_name,
	count(distinct part_name) as part_name_cnt
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - COALESCE(ps_supplycost, 0) * l_quantity as amount,
			COALESCE(p_name, '') as part_name
		from
			part
			inner join lineitem on p_partkey = l_partkey
			left outer join partsupp on ps_suppkey = l_suppkey
				and ps_partkey = l_partkey
			left outer join supplier on s_suppkey = l_suppkey
			inner join orders on o_orderkey = l_orderkey
			inner join nation on s_nationkey = n_nationkey
		where
			p_name like '%:1%'
			and s_name is not null
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
:n -1
