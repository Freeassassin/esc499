-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
:x
:o
select
	s_name,
	COALESCE(s_address, '') as s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
					and COALESCE(p_comment, '') <> ''
			)
			and ps_availqty > (
				select
					CASE WHEN sum(l_quantity) is not null
						THEN 0.5 * sum(COALESCE(l_quantity, 0))
						ELSE 0
					END
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
			)
			and ps_comment is not null
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
	and s_comment is not null
order by
	s_name;
:n -1
