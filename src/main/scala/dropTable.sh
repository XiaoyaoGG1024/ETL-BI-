#! /bin/bash

database1="drop table ods."
database2="drop table dwd."


ods="ods."
dwd="dwd."

drop="truncate table "

hive -e"

${database1}customer_inf;
${database1}order_detail;
${database1}order_master;
${database1}product_info;

${database2}dim_customer_inf;
${database2}fact_order_detail;
${database2}fact_order_master;
${database2}dim_product_info;

${drop}${ods}customer_inf;
${drop}${ods}order_detail;
${drop}${ods}order_master;
${drop}${ods}product_info;

${drop}${dwd}dim_customer_inf;
${drop}${dwd}fact_order_detail;
${drop}${dwd}fact_order_master;
${drop}${dwd}dim_product_info;

"