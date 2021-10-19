
-- add postgis to database if not already added
create extension if not exists postgis;

-- create output table
drop table if exists {1};
create table {1} (
    bal_number smallint,
    bal_name text,
    geom geometry(polygon, 4283)
)
tablespace {2};
alter table {1} owner to "{0}";
