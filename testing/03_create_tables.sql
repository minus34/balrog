
-- add postgis to database if not already added
create extension if not exists postgis;

-- create output table
drop table if exists {1};
create table {1} (
    bld_pid text,
    aspect_min smallint,
    aspect_max smallint,
    aspect_avg smallint,
    aspect_std smallint,
    aspect_med smallint,
    slope_min smallint,
    slope_max smallint,
    slope_avg smallint,
    slope_std smallint,
    slope_med smallint
)
tablespace {2};
alter table {1} owner to "{0}";
