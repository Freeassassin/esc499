--
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- 
define AGGCTHEN= text({"ss_ext_discount_amt",1},{"ss_ext_sales_price",1},{"ss_ext_list_price",1},{"ss_ext_tax",1});
define AGGCELSE= text({"ss_net_paid",1},{"ss_net_paid_inc_tax",1},{"ss_net_profit",1});
define RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);

select case when (select count(*) 
                  from store_sales 
                  where ss_quantity between 1 and 20) > [RC.1]
            then (select avg([AGGCTHEN]) 
                  from store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) > [RC.2]
            then (select avg([AGGCTHEN])
                  from store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 41 and 60) > [RC.3]
            then (select avg([AGGCTHEN])
                  from store_sales
                  where ss_quantity between 41 and 60)
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 61 and 80) > [RC.4]
            then (select avg([AGGCTHEN])
                  from store_sales
                  where ss_quantity between 61 and 80)
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > [RC.5]
            then (select avg([AGGCTHEN])
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1
;
 



-- AUGMENTED_WORKLOAD_BLOCK_BEGIN
create table if not exists aug_workload_ops (
  wid integer,
  tag varchar(64),
  payload varchar(128),
  created_at timestamp
);

insert into aug_workload_ops
select 1 as wid, 'dup' as tag, 'payload' as payload, current_timestamp as created_at;

update aug_workload_ops
set payload = coalesce(payload, 'payload')
where wid = 1;

delete from aug_workload_ops
where wid < 0;

insert into aug_workload_ops
select 1 as wid, 'dup' as tag, 'payload' as payload, current_timestamp as created_at;

update aug_workload_ops
set payload = coalesce(payload, 'payload')
where wid = 1;

delete from aug_workload_ops
where wid < 0;

drop table if exists aug_ctas_snapshot;
create table aug_ctas_snapshot as
select wid, tag, payload, created_at
from aug_workload_ops
where wid is not null;

select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;
select count(*) as meta_table_count from information_schema.tables;

select count(*) as maintenance_probe from aug_workload_ops /* MAINTENANCE_OP */;
select count(*) as maintenance_probe from aug_workload_ops /* MAINTENANCE_OP */;

with meta_l as (
  select table_name as tname from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
),
meta_r as (
  select table_name as tname from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
  union all select table_name from information_schema.tables
)
select
  min(meta_l.tname) as anyvalue_tname,
  count(*) as cnt_all,
  count(distinct meta_r.tname) as cnt_distinct_text,
  sum(case when meta_l.tname is null then 0 else 1 end) as cnt_not_null,
  cast(current_timestamp as timestamp) as ts_marker,
  case when count(*) > 0 then true else false end as bool_marker
from meta_l
left outer join meta_r
  on coalesce(cast(meta_l.tname as varchar), '') = coalesce(cast(meta_r.tname as varchar), '')
where
  coalesce(
    meta_l.tname,
    coalesce(meta_r.tname,
      coalesce(meta_l.tname,
        coalesce(meta_r.tname,
          coalesce(meta_l.tname,
            coalesce(meta_r.tname,
              coalesce(meta_l.tname,
                coalesce(meta_r.tname,
                  coalesce(meta_l.tname,
                    coalesce(meta_r.tname,
                      coalesce(meta_l.tname,
                        coalesce(meta_r.tname, 'fallback')
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  ) is not null
group by cast(meta_l.tname as varchar)
order by cast(meta_l.tname as varchar)
limit 5;
-- AUGMENTED_WORKLOAD_BLOCK_END

