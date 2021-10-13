
-- add postgis to database
create extension if not exists postgis;

-- create schema
create schema if not exists bushfire;alter schema bushfire owner to "{0}";

-- create output table
drop table if exists bushfire.{1};
create table bushfire.{1} (
    bld_pid text,
    aspect_100m_min smallint,
    aspect_100m_max smallint,
    aspect_100m_avg smallint,
    aspect_100m_std smallint,
    aspect_100m_med smallint,
    slope_100m_min smallint,
    slope_100m_max smallint,
    slope_100m_avg smallint,
    slope_100m_std smallint,
    slope_100m_med smallint
)
tablespace {2};
alter table bushfire.{1} owner to "{0}";
