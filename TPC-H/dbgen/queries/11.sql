-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	ps_partkey,
	sum(COALESCE(ps_supplycost, 0) * COALESCE(ps_availqty, 0)) as value
from
	partsupp
	left outer join supplier on ps_suppkey = s_suppkey
	inner join nation on s_nationkey = n_nationkey
where
	n_name = ':1'
	and s_name is not null
	and ps_comment is not null
group by
	ps_partkey having
		sum(COALESCE(ps_supplycost, 0) * COALESCE(ps_availqty, 0)) > (
			select
				COALESCE(sum(ps_supplycost * ps_availqty) * :2, 0)
			from
				partsupp
				inner join supplier on ps_suppkey = s_suppkey
				inner join nation on s_nationkey = n_nationkey
			where
				n_name = ':1'
				and ps_comment is not null
		)
order by
	value desc;
:n -1
