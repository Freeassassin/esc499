-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_i_8541 AS SELECT * FROM ( select
	l_returnflag,
	l_linestatus,
	COUNT(CAST((l_quantity) AS VARCHAR)) as sum_qty,
	COUNT(CAST((l_extendedprice) AS VARCHAR)) as sum_base_price,
	COUNT(CAST((l_extendedprice * (1 - l_discount)) AS VARCHAR)) as sum_disc_price,
	COUNT(CAST((l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS VARCHAR)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus ) subq LIMIT 0;
INSERT INTO tmp_i_8541 SELECT * FROM ( select
	l_returnflag,
	l_linestatus,
	COUNT(CAST((l_quantity) AS VARCHAR)) as sum_qty,
	COUNT(CAST((l_extendedprice) AS VARCHAR)) as sum_base_price,
	COUNT(CAST((l_extendedprice * (1 - l_discount)) AS VARCHAR)) as sum_disc_price,
	COUNT(CAST((l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS VARCHAR)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus ) subq;
SELECT 1;
:n -1
