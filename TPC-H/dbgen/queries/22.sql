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
