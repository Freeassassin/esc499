-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	count(distinct COALESCE(s_name, '')) as supplier_cnt,
	min(COALESCE(s_comment, '')) as min_s_comment
from
	region
	inner join nation on n_regionkey = r_regionkey
	inner join supplier on s_nationkey = n_nationkey
	inner join lineitem on l_suppkey = s_suppkey
	left outer join orders on l_orderkey = o_orderkey
		and o_orderdate >= date ':2'
		and o_orderdate < date ':2' + interval '1' year
	left outer join customer on c_custkey = o_custkey
		and c_nationkey = s_nationkey
where
	r_name = ':1'
	and o_orderkey is not null
group by
	n_name
order by
	revenue desc;
:n -1
