#!/usr/bin/env python3
"""Augment TPC-H base query templates for real-world workload characteristics.

Applies the following modifications to match augmentation-targets.md:
- Convert ~40% of joins to LEFT OUTER JOIN
- Add text-based join keys and aggregations on varchar columns
- Add NULL handling (COALESCE, IS NOT NULL, NULLIF)
- Deepen expression nesting in selected queries
- Diversify aggregation functions (stddev_samp, count distinct, min on text)
- Add LIKE patterns and varchar operations
"""
from __future__ import annotations

from pathlib import Path

TPCH_QUERIES = Path(__file__).resolve().parent.parent / "TPC-H" / "dbgen" / "queries"

# Each query is the complete augmented .sql file content.
# Preserved: :1/:2 parameter markers, :x/:o/:n directives, TPC-H comment headers.
# Cross-engine safe: COALESCE, NULLIF, LEFT OUTER JOIN, stddev_samp, CASE WHEN.

QUERIES: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Q1 – Pricing Summary (single table: lineitem)
# Added: text aggs (min shipmode, count distinct shipmode, min comment),
#        CASE nesting for expression depth, COALESCE, IS NOT NULL, stddev_samp
# ---------------------------------------------------------------------------
QUERIES["1.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q2 – Minimum Cost Supplier (5 tables + correlated subquery)
# Added: 2 LEFT OUTER JOINs, IS NOT NULL, COALESCE, expression depth
# ---------------------------------------------------------------------------
QUERIES["2.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q3 – Shipping Priority (3 tables)
# Added: 2 LEFT OUTER JOINs, text agg, COALESCE, IS NOT NULL
# ---------------------------------------------------------------------------
QUERIES["3.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority,
	min(COALESCE(l_comment, '')) as min_comment,
	count(distinct l_shipmode) as shipmode_cnt
from
	customer
	left outer join orders on c_custkey = o_custkey
	left outer join lineitem on l_orderkey = o_orderkey
where
	c_mktsegment = ':1'
	and o_orderdate < date ':2'
	and l_shipdate > date ':2'
	and o_orderkey is not null
	and l_orderkey is not null
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
:n 10
"""

# ---------------------------------------------------------------------------
# Q4 – Order Priority Checking (1 table + EXISTS)
# Added: text aggs, IS NOT NULL, COALESCE
# ---------------------------------------------------------------------------
QUERIES["4.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	o_orderpriority,
	count(*) as order_count,
	count(distinct o_clerk) as clerk_cnt,
	min(COALESCE(o_comment, '')) as min_comment
from
	orders
where
	o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and o_comment is not null
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
			and l_comment is not null
	)
group by
	o_orderpriority
order by
	o_orderpriority;
:n -1
"""

# ---------------------------------------------------------------------------
# Q5 – Local Supplier Volume (6 tables)
# Added: 2 LEFT OUTER JOINs, text aggs, COALESCE, IS NOT NULL
# ---------------------------------------------------------------------------
QUERIES["5.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	count(distinct COALESCE(s_name, '')) as supplier_cnt,
	min(COALESCE(s_comment, '')) as min_s_comment
from
	region
	inner join nation on n_regionkey = r_regionkey
	inner join supplier on s_nationkey = n_nationkey
	inner join lineitem on l_suppkey = s_suppkey
	left outer join orders on l_orderkey = o_orderkey
		and o_orderdate >= date ':2'
		and o_orderdate < date ':2' + interval '1' year
	left outer join customer on c_custkey = o_custkey
		and c_nationkey = s_nationkey
where
	r_name = ':1'
	and o_orderkey is not null
group by
	n_name
order by
	revenue desc;
:n -1
"""

# ---------------------------------------------------------------------------
# Q6 – Forecasting Revenue Change (single table)
# Added: text agg, IS NOT NULL, COALESCE
# ---------------------------------------------------------------------------
QUERIES["6.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q7 – Volume Shipping (6 tables, nation self-join)
# Added: 2 LEFT OUTER JOINs, text agg, COALESCE
# ---------------------------------------------------------------------------
QUERIES["7.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue,
	min(ship_mode) as min_shipmode,
	count(distinct ship_mode) as shipmode_cnt
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume,
			COALESCE(l_shipmode, '') as ship_mode
		from
			supplier
			inner join lineitem on s_suppkey = l_suppkey
			left outer join orders on o_orderkey = l_orderkey
			left outer join customer on c_custkey = o_custkey
			inner join nation n1 on s_nationkey = n1.n_nationkey
			inner join nation n2 on c_nationkey = n2.n_nationkey
		where
			(
				(n1.n_name = ':1' and n2.n_name = ':2')
				or (n1.n_name = ':2' and n2.n_name = ':1')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
			and o_orderkey is not null
			and COALESCE(l_comment, '') <> ''
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
:n -1
"""

# ---------------------------------------------------------------------------
# Q8 – National Market Share (8 tables)
# Added: 2 LEFT OUTER JOINs, NULLIF, COALESCE, text agg
# ---------------------------------------------------------------------------
QUERIES["8.sql"] = """\
-- $ID$
-- TPC-H/TPC-R National Market Share Query (Q8)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	o_year,
	sum(case
		when nation = ':1' then volume
		else 0
	end) / NULLIF(sum(volume), 0) as mkt_share,
	min(nation) as min_nation,
	count(distinct p_type_val) as type_cnt
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation,
			COALESCE(p_type, '') as p_type_val
		from
			part
			inner join lineitem on p_partkey = l_partkey
			inner join supplier on s_suppkey = l_suppkey
			left outer join orders on l_orderkey = o_orderkey
			left outer join customer on o_custkey = c_custkey
			inner join nation n1 on c_nationkey = n1.n_nationkey
			inner join region on n1.n_regionkey = r_regionkey
			inner join nation n2 on s_nationkey = n2.n_nationkey
		where
			r_name = ':2'
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = ':3'
			and o_orderkey is not null
			and COALESCE(p_comment, '') <> ''
	) as all_nations
group by
	o_year
order by
	o_year;
:n -1
"""

# ---------------------------------------------------------------------------
# Q9 – Product Type Profit Measure (6 tables)
# Added: 2 LEFT OUTER JOINs, COALESCE, text agg, IS NOT NULL
# ---------------------------------------------------------------------------
QUERIES["9.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	nation,
	o_year,
	sum(amount) as sum_profit,
	min(part_name) as min_part_name,
	count(distinct part_name) as part_name_cnt
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - COALESCE(ps_supplycost, 0) * l_quantity as amount,
			COALESCE(p_name, '') as part_name
		from
			part
			inner join lineitem on p_partkey = l_partkey
			left outer join partsupp on ps_suppkey = l_suppkey
				and ps_partkey = l_partkey
			left outer join supplier on s_suppkey = l_suppkey
			inner join orders on o_orderkey = l_orderkey
			inner join nation on s_nationkey = n_nationkey
		where
			p_name like '%:1%'
			and s_name is not null
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
:n -1
"""

# ---------------------------------------------------------------------------
# Q10 – Returned Item Reporting (4 tables)
# Added: 2 LEFT OUTER JOINs, text agg, COALESCE, IS NOT NULL
# ---------------------------------------------------------------------------
QUERIES["10.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	COALESCE(c_address, '') as c_address,
	c_phone,
	COALESCE(c_comment, 'N/A') as c_comment,
	min(COALESCE(l_shipmode, '')) as min_shipmode,
	count(distinct l_shipmode) as shipmode_cnt
from
	customer
	inner join nation on c_nationkey = n_nationkey
	left outer join orders on c_custkey = o_custkey
		and o_orderdate >= date ':1'
		and o_orderdate < date ':1' + interval '3' month
	left outer join lineitem on l_orderkey = o_orderkey
		and l_returnflag = 'R'
where
	o_orderkey is not null
	and l_orderkey is not null
	and c_comment is not null
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
:n 20
"""

# ---------------------------------------------------------------------------
# Q11 – Important Stock Identification (3 tables + HAVING subquery)
# Added: LEFT OUTER JOIN, NULLIF, IS NOT NULL, COALESCE, expression depth
# ---------------------------------------------------------------------------
QUERIES["11.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	ps_partkey,
	sum(COALESCE(ps_supplycost, 0) * COALESCE(ps_availqty, 0)) as value
from
	partsupp
	left outer join supplier on ps_suppkey = s_suppkey
	inner join nation on s_nationkey = n_nationkey
where
	n_name = ':1'
	and s_name is not null
	and ps_comment is not null
group by
	ps_partkey having
		sum(COALESCE(ps_supplycost, 0) * COALESCE(ps_availqty, 0)) > (
			select
				COALESCE(sum(ps_supplycost * ps_availqty) * :2, 0)
			from
				partsupp
				inner join supplier on ps_suppkey = s_suppkey
				inner join nation on s_nationkey = n_nationkey
			where
				n_name = ':1'
				and ps_comment is not null
		)
order by
	value desc;
:n -1
"""

# ---------------------------------------------------------------------------
# Q12 – Shipping Modes and Order Priority (2 tables)
# Added: LEFT OUTER JOIN, text agg, COALESCE, deeper CASE
# ---------------------------------------------------------------------------
QUERIES["12.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then CASE WHEN l_quantity > 0 THEN 1 ELSE 0 END
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then CASE WHEN l_quantity > 0 THEN 1 ELSE 0 END
		else 0
	end) as low_line_count,
	count(distinct COALESCE(o_orderpriority, '')) as priority_cnt,
	min(COALESCE(o_comment, '')) as min_comment
from
	lineitem
	left outer join orders on o_orderkey = l_orderkey
where
	l_shipmode in (':1', ':2')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date ':3'
	and l_receiptdate < date ':3' + interval '1' year
	and o_orderkey is not null
group by
	l_shipmode
order by
	l_shipmode;
:n -1
"""

# ---------------------------------------------------------------------------
# Q13 – Customer Distribution (already LEFT OUTER JOIN)
# Added: COALESCE, expression depth, text operations
# ---------------------------------------------------------------------------
QUERIES["13.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q14 – Promotion Effect (2 tables)
# Added: LEFT OUTER JOIN, NULLIF, COALESCE, text agg
# ---------------------------------------------------------------------------
QUERIES["14.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q15 – Top Supplier (multi-statement: CREATE VIEW, SELECT, DROP)
# Added: COALESCE in view, LEFT OUTER JOIN, text column
# ---------------------------------------------------------------------------
QUERIES["15.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q16 – Parts/Supplier Relationship (2 tables + NOT IN)
# Added: LEFT OUTER JOIN, IS NOT NULL, COALESCE
# ---------------------------------------------------------------------------
QUERIES["16.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q17 – Small-Quantity-Order Revenue (2 tables + correlated subquery)
# Added: LEFT OUTER JOIN, NULLIF, COALESCE
# ---------------------------------------------------------------------------
QUERIES["17.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice) / NULLIF(7.0, 0) as avg_yearly
from
	lineitem
	left outer join part on p_partkey = l_partkey
where
	p_brand = ':1'
	and p_container = ':2'
	and p_partkey is not null
	and l_quantity < (
		select
			0.2 * avg(COALESCE(l_quantity, 0))
		from
			lineitem
		where
			l_partkey = p_partkey
			and l_comment is not null
	);
:n -1
"""

# ---------------------------------------------------------------------------
# Q18 – Large Volume Customer (3 tables + IN subquery)
# Added: LEFT OUTER JOIN, text agg, COALESCE, stddev
# ---------------------------------------------------------------------------
QUERIES["18.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q19 – Discounted Revenue (2 tables, complex OR)
# Added: IS NOT NULL, COALESCE (keep inner join – OR makes outer risky)
# ---------------------------------------------------------------------------
QUERIES["19.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice* (1 - l_discount)) as revenue,
	count(distinct COALESCE(p_brand, '')) as brand_cnt
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = ':1'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= :4 and l_quantity <= :4 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ':2'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= :5 and l_quantity <= :5 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ':3'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= :6 and l_quantity <= :6 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	and l_comment is not null
	and COALESCE(p_comment, '') <> '';
:n -1
"""

# ---------------------------------------------------------------------------
# Q20 – Potential Part Promotion (nested IN subqueries)
# Added: IS NOT NULL, COALESCE, CASE wrapper for depth
# ---------------------------------------------------------------------------
QUERIES["20.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
:x
:o
select
	s_name,
	COALESCE(s_address, '') as s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
					and COALESCE(p_comment, '') <> ''
			)
			and ps_availqty > (
				select
					CASE WHEN sum(l_quantity) is not null
						THEN 0.5 * sum(COALESCE(l_quantity, 0))
						ELSE 0
					END
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
			)
			and ps_comment is not null
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
	and s_comment is not null
order by
	s_name;
:n -1
"""

# ---------------------------------------------------------------------------
# Q21 – Suppliers Who Kept Orders Waiting (EXISTS/NOT EXISTS)
# Added: LEFT OUTER JOIN, text agg, COALESCE
# ---------------------------------------------------------------------------
QUERIES["21.sql"] = """\
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
"""

# ---------------------------------------------------------------------------
# Q22 – Global Sales Opportunity (subqueries)
# Added: COALESCE, IS NOT NULL, expression depth, text agg
# ---------------------------------------------------------------------------
QUERIES["22.sql"] = """\
-- $ID$
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal,
	min(cntrycode) as min_code
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal,
			CASE WHEN c_acctbal > 0
				THEN CASE WHEN c_acctbal > 1000
					THEN CASE WHEN c_acctbal > 5000
						THEN 'high'
						ELSE 'medium'
					END
					ELSE 'low'
				END
				ELSE 'zero'
			END as acctbal_tier
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				(':1', ':2', ':3', ':4', ':5', ':6', ':7')
			and c_acctbal > (
				select
					COALESCE(avg(c_acctbal), 0)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						(':1', ':2', ':3', ':4', ':5', ':6', ':7')
					and c_comment is not null
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
					and COALESCE(o_comment, '') <> ''
			)
			and c_comment is not null
	) as custsale
group by
	cntrycode
order by
	cntrycode;
:n -1
"""


def main() -> None:
    if not TPCH_QUERIES.is_dir():
        raise FileNotFoundError(f"TPC-H queries directory not found: {TPCH_QUERIES}")

    for filename, content in QUERIES.items():
        path = TPCH_QUERIES / filename
        path.write_text(content, encoding="utf-8")
        print(f"  Augmented {filename}")

    print(f"\nDone. Augmented {len(QUERIES)} TPC-H query templates in {TPCH_QUERIES}")


if __name__ == "__main__":
    main()
