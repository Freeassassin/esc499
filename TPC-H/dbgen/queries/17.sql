-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice) / NULLIF(7.0, 0) as avg_yearly
from
	lineitem
	left outer join part on p_partkey = l_partkey
where
	p_brand = ':1'
	and p_container = ':2'
	and p_partkey is not null
	and l_quantity < (
		select
			0.2 * avg(COALESCE(l_quantity, 0))
		from
			lineitem
		where
			l_partkey = p_partkey
			and l_comment is not null
	);
:n -1
