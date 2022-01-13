use ${DB};
insert into date_dim select * from ${SOURCE}.date_dim;
