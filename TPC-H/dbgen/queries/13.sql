-- $ID$
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	c_count,
	count(*) as custdist,
	min(COALESCE(c_segment, '')) as min_segment
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count_raw,
			CASE WHEN count(o_orderkey) > 10
				THEN CASE WHEN count(o_orderkey) > 20
					THEN 'high'
					ELSE 'medium'
				END
				ELSE 'low'
			END as c_segment,
			COALESCE(count(o_orderkey), 0) as c_count
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%:1%:2%'
		where
			c_comment is not null
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
:n -1
