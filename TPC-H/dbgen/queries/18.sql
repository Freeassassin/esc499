-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998
:x
:o
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity) as sum_qty,
	min(COALESCE(c_comment, '')) as min_comment,
	count(distinct l_shipmode) as shipmode_cnt,
	stddev_samp(l_quantity) as stddev_qty
from
	customer
	left outer join orders on c_custkey = o_custkey
	left outer join lineitem on o_orderkey = l_orderkey
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > :1
	)
	and o_orderkey is not null
	and l_orderkey is not null
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate;
:n 100
