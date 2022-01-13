#!/bin/bash
source ../common/env.sh

file=${SPARK_TEST_DB}.stats
rm -f $file

tables=("catalog_page" "catalog_returns" "customer" "customer_address" "customer_demographics" "date_dim" "household_demographics" "inventory" "item" "promotion" "store" "store_returns" "catalog_sales" "web_sales" "store_sales" "web_returns" "web_site" "reason" "call_center" "warehouse" "ship_mode" "income_band" "time_dim" "web_page")

for table in ${tables[@]}
do echo "use ${SPARK_TEST_DB}; show create table $table;" | hive >> $file
done