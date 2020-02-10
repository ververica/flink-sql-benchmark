create database if not exists ${DB};
use ${DB};

drop table if exists store_sales;

create table store_sales
stored as ${FILE}
as select * from ${SOURCE}.store_sales
;
