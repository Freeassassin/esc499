-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
:x
:o
SELECT * FROM information_schema.tables CROSS JOIN ( select
	o_orderpriority,
	count(*) as order_count
from
	orders
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority ) subq;
SELECT 1;
:n -1
