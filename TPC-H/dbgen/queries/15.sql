-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998
:x
create view revenue:s (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(COALESCE(l_extendedprice, 0) * (1 - COALESCE(l_discount, 0)))
	from
		lineitem
	where
		l_shipdate >= date ':1'
		and l_shipdate < date ':1' + interval '3' month
		and l_comment is not null
	group by
		l_suppkey;

:o
select
	s_suppkey,
	COALESCE(s_name, '') as s_name,
	COALESCE(s_address, '') as s_address,
	s_phone,
	total_revenue
from
	supplier
	left outer join revenue:s on s_suppkey = supplier_no
where
	total_revenue = (
		select
			max(total_revenue)
		from
			revenue:s
	)
	and s_name is not null
order by
	s_suppkey;

drop view revenue:s;
:n -1
