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
 define COUNTY = ulist(dist(fips_county,2,1),10);
 define MONTH = random(1,4,uniform);
 define YEAR = random(1999,2002,uniform);
 define _LIMIT=100; 

 [_LIMITA] select [_LIMITB] 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('[COUNTY.1]','[COUNTY.2]','[COUNTY.3]','[COUNTY.4]','[COUNTY.5]') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = [YEAR] and
                d_moy between [MONTH] and [MONTH]+3) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = [YEAR] and
                  d_moy between [MONTH] ANd [MONTH]+3) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = [YEAR] and
                  d_moy between [MONTH] and [MONTH]+3))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
[_LIMITC];
 
 



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

