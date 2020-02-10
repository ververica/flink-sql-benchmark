create database if not exists ${DB};
use ${DB};

drop table if exists web_returns;

create table web_returns
stored as ${FILE}
as select * from ${SOURCE}.web_returns
;
