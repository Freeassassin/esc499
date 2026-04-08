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
  define YEAR= random(1998, 2002, uniform);
 define TIMEONE= random(1, 57597, uniform);
 define SMC = ulist(dist(ship_mode_carrier, 1, 1),2);
 define NETONE = text({"ws_net_paid",1},{"ws_net_paid_inc_tax",1},{"ws_net_paid_inc_ship",1},{"ws_net_paid_inc_ship_tax",1},{"ws_net_profit",1});
 define NETTWO = text({"cs_net_paid",1},{"cs_net_paid_inc_tax",1},{"cs_net_paid_inc_ship",1},{"cs_net_paid_inc_ship_tax",1},{"cs_net_profit",1});
 define SALESONE = text({"ws_sales_price",1},{"ws_ext_sales_price",1},{"ws_ext_list_price",1});
 define SALESTWO = text({"cs_sales_price",1},{"cs_ext_sales_price",1},{"cs_ext_list_price",1});
 define _LIMIT=100;

SELECT * FROM information_schema.tables CROSS JOIN ( select   
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,COUNT(CAST((jan_sales) AS VARCHAR)) as jan_sales
 	,COUNT(CAST((feb_sales) AS VARCHAR)) as feb_sales
 	,COUNT(CAST((mar_sales) AS VARCHAR)) as mar_sales
 	,COUNT(CAST((apr_sales) AS VARCHAR)) as apr_sales
 	,COUNT(CAST((may_sales) AS VARCHAR)) as may_sales
 	,COUNT(CAST((jun_sales) AS VARCHAR)) as jun_sales
 	,COUNT(CAST((jul_sales) AS VARCHAR)) as jul_sales
 	,COUNT(CAST((aug_sales) AS VARCHAR)) as aug_sales
 	,COUNT(CAST((sep_sales) AS VARCHAR)) as sep_sales
 	,COUNT(CAST((oct_sales) AS VARCHAR)) as oct_sales
 	,COUNT(CAST((nov_sales) AS VARCHAR)) as nov_sales
 	,COUNT(CAST((dec_sales) AS VARCHAR)) as dec_sales
 	,COUNT(CAST((jan_sales/w_warehouse_sq_ft) AS VARCHAR)) as jan_sales_per_sq_foot
 	,COUNT(CAST((feb_sales/w_warehouse_sq_ft) AS VARCHAR)) as feb_sales_per_sq_foot
 	,COUNT(CAST((mar_sales/w_warehouse_sq_ft) AS VARCHAR)) as mar_sales_per_sq_foot
 	,COUNT(CAST((apr_sales/w_warehouse_sq_ft) AS VARCHAR)) as apr_sales_per_sq_foot
 	,COUNT(CAST((may_sales/w_warehouse_sq_ft) AS VARCHAR)) as may_sales_per_sq_foot
 	,COUNT(CAST((jun_sales/w_warehouse_sq_ft) AS VARCHAR)) as jun_sales_per_sq_foot
 	,COUNT(CAST((jul_sales/w_warehouse_sq_ft) AS VARCHAR)) as jul_sales_per_sq_foot
 	,COUNT(CAST((aug_sales/w_warehouse_sq_ft) AS VARCHAR)) as aug_sales_per_sq_foot
 	,COUNT(CAST((sep_sales/w_warehouse_sq_ft) AS VARCHAR)) as sep_sales_per_sq_foot
 	,COUNT(CAST((oct_sales/w_warehouse_sq_ft) AS VARCHAR)) as oct_sales_per_sq_foot
 	,COUNT(CAST((nov_sales/w_warehouse_sq_ft) AS VARCHAR)) as nov_sales_per_sq_foot
 	,COUNT(CAST((dec_sales/w_warehouse_sq_ft) AS VARCHAR)) as dec_sales_per_sq_foot
 	,COUNT(CAST((jan_net) AS VARCHAR)) as jan_net
 	,COUNT(CAST((feb_net) AS VARCHAR)) as feb_net
 	,COUNT(CAST((mar_net) AS VARCHAR)) as mar_net
 	,COUNT(CAST((apr_net) AS VARCHAR)) as apr_net
 	,COUNT(CAST((may_net) AS VARCHAR)) as may_net
 	,COUNT(CAST((jun_net) AS VARCHAR)) as jun_net
 	,COUNT(CAST((jul_net) AS VARCHAR)) as jul_net
 	,COUNT(CAST((aug_net) AS VARCHAR)) as aug_net
 	,COUNT(CAST((sep_net) AS VARCHAR)) as sep_net
 	,COUNT(CAST((oct_net) AS VARCHAR)) as oct_net
 	,COUNT(CAST((nov_net) AS VARCHAR)) as nov_net
 	,COUNT(CAST((dec_net) AS VARCHAR)) as dec_net
 from (
     select 
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'[SMC.1]' || ',' || '[SMC.2]' as ship_carriers
       ,d_year as year
 	,COUNT(CAST((case when d_moy = 1 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as jan_sales
 	,COUNT(CAST((case when d_moy = 2 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as feb_sales
 	,COUNT(CAST((case when d_moy = 3 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as mar_sales
 	,COUNT(CAST((case when d_moy = 4 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as apr_sales
 	,COUNT(CAST((case when d_moy = 5 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as may_sales
 	,COUNT(CAST((case when d_moy = 6 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as jun_sales
 	,COUNT(CAST((case when d_moy = 7 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as jul_sales
 	,COUNT(CAST((case when d_moy = 8 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as aug_sales
 	,COUNT(CAST((case when d_moy = 9 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as sep_sales
 	,COUNT(CAST((case when d_moy = 10 
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as oct_sales
 	,COUNT(CAST((case when d_moy = 11
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as nov_sales
 	,COUNT(CAST((case when d_moy = 12
 		then [SALESONE]* ws_quantity else 0 end) AS VARCHAR)) as dec_sales
 	,COUNT(CAST((case when d_moy = 1 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as jan_net
 	,COUNT(CAST((case when d_moy = 2
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as feb_net
 	,COUNT(CAST((case when d_moy = 3 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as mar_net
 	,COUNT(CAST((case when d_moy = 4 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as apr_net
 	,COUNT(CAST((case when d_moy = 5 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as may_net
 	,COUNT(CAST((case when d_moy = 6 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as jun_net
 	,COUNT(CAST((case when d_moy = 7 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as jul_net
 	,COUNT(CAST((case when d_moy = 8 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as aug_net
 	,COUNT(CAST((case when d_moy = 9 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as sep_net
 	,COUNT(CAST((case when d_moy = 10 
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as oct_net
 	,COUNT(CAST((case when d_moy = 11
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as nov_net
 	,COUNT(CAST((case when d_moy = 12
 		then [NETONE] * ws_quantity else 0 end) AS VARCHAR)) as dec_net
     from
          web_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	  ,ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
 	and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = [YEAR]
 	and t_time between [TIMEONE] and [TIMEONE]+28800 
 	and sm_carrier in ('[SMC.1]','[SMC.2]')
     group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 union all
     select 
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'[SMC.1]' || ',' || '[SMC.2]' as ship_carriers
       ,d_year as year
 	,COUNT(CAST((case when d_moy = 1 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as jan_sales
 	,COUNT(CAST((case when d_moy = 2 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as feb_sales
 	,COUNT(CAST((case when d_moy = 3 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as mar_sales
 	,COUNT(CAST((case when d_moy = 4 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as apr_sales
 	,COUNT(CAST((case when d_moy = 5 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as may_sales
 	,COUNT(CAST((case when d_moy = 6 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as jun_sales
 	,COUNT(CAST((case when d_moy = 7 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as jul_sales
 	,COUNT(CAST((case when d_moy = 8 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as aug_sales
 	,COUNT(CAST((case when d_moy = 9 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as sep_sales
 	,COUNT(CAST((case when d_moy = 10 
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as oct_sales
 	,COUNT(CAST((case when d_moy = 11
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as nov_sales
 	,COUNT(CAST((case when d_moy = 12
 		then [SALESTWO]* cs_quantity else 0 end) AS VARCHAR)) as dec_sales
 	,COUNT(CAST((case when d_moy = 1 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as jan_net
 	,COUNT(CAST((case when d_moy = 2 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as feb_net
 	,COUNT(CAST((case when d_moy = 3 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as mar_net
 	,COUNT(CAST((case when d_moy = 4 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as apr_net
 	,COUNT(CAST((case when d_moy = 5 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as may_net
 	,COUNT(CAST((case when d_moy = 6 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as jun_net
 	,COUNT(CAST((case when d_moy = 7 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as jul_net
 	,COUNT(CAST((case when d_moy = 8 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as aug_net
 	,COUNT(CAST((case when d_moy = 9 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as sep_net
 	,COUNT(CAST((case when d_moy = 10 
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as oct_net
 	,COUNT(CAST((case when d_moy = 11
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as nov_net
 	,COUNT(CAST((case when d_moy = 12
 		then [NETTWO] * cs_quantity else 0 end) AS VARCHAR)) as dec_net
     from
          catalog_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	 ,ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
 	and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = [YEAR]
 	and t_time between [TIMEONE] AND [TIMEONE]+28800 
 	and sm_carrier in ('[SMC.1]','[SMC.2]')
     group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 ) x
 group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,ship_carriers
       ,year
 order by w_warehouse_name
  ) subq;
SELECT 1 [_LIMITC];
