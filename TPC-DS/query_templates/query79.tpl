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
 define DEPCNT=random(0,9,uniform);
 define YEAR = random(1998,2000,uniform);
 define VEHCNT=random(-1,4,uniform);
 define _LIMIT=100;
 
 [_LIMITA] select [_LIMITB]
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = [DEPCNT] or household_demographics.hd_vehicle_count > [VEHCNT])
    and date_dim.d_dow = 1
    and date_dim.d_year in ([YEAR],[YEAR]+1,[YEAR]+2) 
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
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

select
  min(table_name) as anyvalue_tname,
  count(table_name) as cnt_text,
  count(distinct table_name) as cnt_distinct_text
from information_schema.tables
where coalesce(table_name, 'fallback') is not null
group by table_name
order by table_name
limit 5;
-- AUGMENTED_WORKLOAD_BLOCK_END

