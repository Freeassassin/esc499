-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / NULLIF(sum(l_extendedprice * (1 - l_discount)), 0) as promo_revenue,
	count(distinct COALESCE(p_type, '')) as type_cnt,
	min(COALESCE(p_type, '')) as min_type
from
	lineitem
	left outer join part on l_partkey = p_partkey
where
	l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' month
	and p_partkey is not null;
:n -1
