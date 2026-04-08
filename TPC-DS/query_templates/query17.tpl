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
 define YEAR= random(1998,2002, uniform);
 define _LIMIT=100;
 
 [_LIMITA] select [_LIMITB] i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '[YEAR]Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
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
),
meta_r as (
  select table_name as tname from information_schema.tables
)
select
  min(meta_l.tname) as anyvalue_tname,
  count(meta_r.tname) as cnt_text,
  count(distinct meta_r.tname) as cnt_distinct_text
from meta_l
left outer join meta_r
  on coalesce(cast(meta_l.tname as varchar), '') = coalesce(cast(meta_r.tname as varchar), '')
where coalesce(meta_l.tname, meta_r.tname) is not null
group by cast(meta_l.tname as varchar)
order by cast(meta_l.tname as varchar)
limit 5;
-- AUGMENTED_WORKLOAD_BLOCK_END

