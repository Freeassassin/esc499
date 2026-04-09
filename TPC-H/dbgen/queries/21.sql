-- $ID$
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	s_name,
	count(*) as numwait,
	min(COALESCE(l1.l_shipmode, '')) as min_shipmode
from
	supplier
	left outer join lineitem l1 on s_suppkey = l1.l_suppkey
	inner join orders on o_orderkey = l1.l_orderkey
	inner join nation on s_nationkey = n_nationkey
where
	o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and l1.l_suppkey is not null
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and n_name = ':1'
	and COALESCE(s_comment, '') <> ''
group by
	s_name
order by
	numwait desc,
	s_name;
:n 100
