-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998
:x
:o
CREATE TEMP TABLE tmp_d_3143 AS SELECT * FROM ( select
	100.00 * COUNT(CAST((case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) AS VARCHAR)) / COUNT(CAST((l_extendedprice * (1 - l_discount)) AS VARCHAR)) as promo_revenue
from
	lineitem,
	part
WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND 
	l_partkey = p_partkey
	and l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' month ) subq;
DELETE FROM tmp_d_3143 WHERE 1=0;
SELECT 1;
:n -1
