-- $ID$
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt,
	min(COALESCE(p_comment, '')) as min_comment
from
	part
	left outer join partsupp on p_partkey = ps_partkey
where
	p_brand <> ':1'
	and p_type not like ':2%'
	and p_size in (:3, :4, :5, :6, :7, :8, :9, :10)
	and ps_partkey is not null
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
			and COALESCE(s_comment, '') <> ''
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
:n -1
