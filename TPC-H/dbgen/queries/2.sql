-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	s_acctbal,
	COALESCE(s_name, '') as s_name,
	n_name,
	p_partkey,
	p_mfgr,
	COALESCE(s_address, '') as s_address,
	s_phone,
	COALESCE(s_comment, 'N/A') as s_comment
from
	part
	inner join partsupp on p_partkey = ps_partkey
	left outer join supplier on s_suppkey = ps_suppkey
	left outer join nation on s_nationkey = n_nationkey
	inner join region on n_regionkey = r_regionkey
where
	p_size = :1
	and p_type like '%:2'
	and r_name = ':3'
	and s_name is not null
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp
			inner join supplier on s_suppkey = ps_suppkey
			inner join nation on s_nationkey = n_nationkey
			inner join region on n_regionkey = r_regionkey
		where
			p_partkey = ps_partkey
			and r_name = ':3'
			and s_comment is not null
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey;
:n 100
