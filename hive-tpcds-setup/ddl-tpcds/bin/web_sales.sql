create database if not exists ${DB};
use ${DB};

drop table if exists web_sales;

create table web_sales
stored as ${FILE}
as select * from ${SOURCE}.web_sales
;
