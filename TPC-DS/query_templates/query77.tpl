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
 define YEAR = random(1998, 2002, uniform);
 define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
 define _LIMIT=100;

CREATE TEMP TABLE tmp_u_6147 AS SELECT *, -1 as _dummy_update_col FROM ( with ss as
 (select s_store_sk,
         COUNT(CAST((ss_ext_sales_price) AS VARCHAR)) as sales,
         COUNT(CAST((ss_net_profit) AS VARCHAR)) as profit
 from store_sales,
      date_dim,
      store
 WHERE (1=1 OR 'a' IS NOT NULL) AND COALESCE(NULL, 1)=1 AND  ss_sold_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date) 
                  and (cast('[SALES_DATE]' as date) +  30 days) 
       and ss_store_sk = s_store_sk
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         COUNT(CAST((sr_return_amt) AS VARCHAR)) as returns,
         COUNT(CAST((sr_net_loss) AS VARCHAR)) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date)
                  and (cast('[SALES_DATE]' as date) +  30 days)
       and sr_store_sk = s_store_sk
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        COUNT(CAST((cs_ext_sales_price) AS VARCHAR)) as sales,
        COUNT(CAST((cs_net_profit) AS VARCHAR)) as profit
 from catalog_sales,
      date_dim
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date)
                  and (cast('[SALES_DATE]' as date) +  30 days)
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         COUNT(CAST((cr_return_amount) AS VARCHAR)) as returns,
         COUNT(CAST((cr_net_loss) AS VARCHAR)) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date)
                  and (cast('[SALES_DATE]' as date) +  30 days)
 group by cr_call_center_sk
 ), 
 ws as
 ( select wp_web_page_sk,
        COUNT(CAST((ws_ext_sales_price) AS VARCHAR)) as sales,
        COUNT(CAST((ws_net_profit) AS VARCHAR)) as profit
 from web_sales,
      date_dim,
      web_page
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date)
                  and (cast('[SALES_DATE]' as date) +  30 days)
       and ws_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk), 
 wr as
 (select wp_web_page_sk,
        COUNT(CAST((wr_return_amt) AS VARCHAR)) as returns,
        COUNT(CAST((wr_net_loss) AS VARCHAR)) as profit_loss
 from web_returns,
      date_dim,
      web_page
 where wr_returned_date_sk = d_date_sk
       and d_date between cast('[SALES_DATE]' as date)
                  and (cast('[SALES_DATE]' as date) +  30 days)
       and wr_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk)
  select  channel
        , id
        , COUNT(CAST((sales) AS VARCHAR)) as sales
        , COUNT(CAST((returns) AS VARCHAR)) as returns
        , COUNT(CAST((profit) AS VARCHAR)) as profit
 from 
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(returns, 0) as returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ss LEFT OUTER JOIN sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  cs
       , cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(returns, 0) returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ws LEFT OUTER JOIN wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
  ) subq;
UPDATE tmp_u_6147 SET _dummy_update_col = 1 WHERE 1=0;
SELECT 1 [_LIMITC];
