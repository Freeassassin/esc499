-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(CASE WHEN l_tax > 0
		THEN l_extendedprice * (1 - l_discount) * (1 + l_tax)
		ELSE COALESCE(l_extendedprice * (1 - l_discount), 0)
	END) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order,
	min(l_shipmode) as min_shipmode,
	count(distinct l_shipmode) as shipmode_cnt,
	min(COALESCE(l_comment, '')) as min_comment,
	stddev_samp(l_quantity) as stddev_qty,
	stddev_samp(l_extendedprice) as stddev_price
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
	and l_comment is not null
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
:n -1
