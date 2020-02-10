create database if not exists ${DB};
use ${DB};

drop table if exists store_returns;

create table store_returns
stored as ${FILE}
as select * from ${SOURCE}.store_returns
;
