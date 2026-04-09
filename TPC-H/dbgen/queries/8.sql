-- $ID$
-- TPC-H/TPC-R National Market Share Query (Q8)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	o_year,
	sum(case
		when nation = ':1' then volume
		else 0
	end) / NULLIF(sum(volume), 0) as mkt_share,
	min(nation) as min_nation,
	count(distinct p_type_val) as type_cnt
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation,
			COALESCE(p_type, '') as p_type_val
		from
			part
			inner join lineitem on p_partkey = l_partkey
			inner join supplier on s_suppkey = l_suppkey
			left outer join orders on l_orderkey = o_orderkey
			left outer join customer on o_custkey = c_custkey
			inner join nation n1 on c_nationkey = n1.n_nationkey
			inner join region on n1.n_regionkey = r_regionkey
			inner join nation n2 on s_nationkey = n2.n_nationkey
		where
			r_name = ':2'
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = ':3'
			and o_orderkey is not null
			and COALESCE(p_comment, '') <> ''
	) as all_nations
group by
	o_year
order by
	o_year;
:n -1
