-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice * l_discount) as revenue,
	min(l_shipmode) as min_shipmode,
	count(distinct l_shipmode) as shipmode_cnt,
	min(COALESCE(l_comment, '')) as min_comment
from
	lineitem
where
	l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' year
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3
	and l_comment is not null;
:n -1
