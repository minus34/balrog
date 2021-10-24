
-- add postgis to database if not already added
create extension if not exists postgis;

-- create output table
drop table if exists {1};
create table {1} (
    gid integer,
    bal_number smallint,
    bal_name text,
    area_m2 double precision,
    azimuth double precision,
    distance_m double precision,
    aspect smallint,
    slope smallint,
    geom geometry(polygon, 4283),
    line_geom geometry(linestring, 4283)
)
tablespace {2};
alter table {1} owner to "{0}";


drop table if exists {1}_buffer;
create table {1}_buffer (
    elevation smallint,
    aspect smallint,
    slope smallint,
    geom geometry(polygon, 4283)
)
tablespace {2};
alter table {1}_buffer owner to "{0}";
