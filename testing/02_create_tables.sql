
-- -- create tablespace to use mounted EC2 drive
-- CREATE TABLESPACE bushfirespace OWNER "ec2-user" LOCATION '/data/postgres';
-- ALTER DATABASE geo SET TABLESPACE bushfirespace;

-- add postgis to database
create extension if not exists postgis;

-- create schema
create schema if not exists bushfire;alter schema bushfire owner to "ec2-user";

-- create image & label tables for both training and inference

drop table if exists bushfire.bal_factors;
create table bushfire.bal_factors (
    bld_pid text,
    aspect_bldg_min smallint,
    aspect_bldg_max smallint,
    aspect_bldg_avg smallint,
    aspect_bldg_std smallint,
    aspect_bldg_med smallint,
    aspect_100m_min smallint,
    aspect_100m_max smallint,
    aspect_100m_avg smallint,
    aspect_100m_std smallint,
    aspect_100m_med smallint,
    slope_bldg_min smallint,
    slope_bldg_max smallint,
    slope_bldg_avg smallint,
    slope_bldg_std smallint,
    slope_bldg_med smallint,
    slope_100m_min smallint,
    slope_100m_max smallint,
    slope_100m_avg smallint,
    slope_100m_std smallint,
    slope_100m_med smallint,
    dem_bldg_min smallint,
    dem_bldg_max smallint,
    dem_bldg_avg smallint,
    dem_bldg_std smallint,
    dem_bldg_med smallint,
    dem_100m_min smallint,
    dem_100m_max smallint,
    dem_100m_avg smallint,
    dem_100m_std smallint,
    dem_100m_med smallint
)
TABLESPACE bushfirespace;
alter table bushfire.bal_factors owner to "ec2-user";

-- TODO: move these to after data import if this needs to scale
-- ALTER TABLE bushfire.bal_factors ADD CONSTRAINT bal_factors_pkey PRIMARY KEY (gnaf_pid);
-- CREATE INDEX bal_factors_gnaf_pid_idx ON bushfire.bal_factors USING btree (gnaf_pid);
-- CREATE INDEX bal_factors_point_geom_idx ON bushfire.bal_factors USING gist (point_geom);
-- CREATE INDEX bal_factors_geom_idx ON bushfire.bal_factors USING gist (geom);
-- ALTER TABLE bushfire.bal_factors CLUSTER ON bal_factors_geom_idx;




-- -- convert to polygons (they aren't multipolygons!) -- 15,841,377 rows affected in 4 m 35 s 202 ms
-- drop table if exists bushfire.temp_buildings;
-- create table bushfire.temp_buildings as
-- with bld as (
--     select bld_pid,
--            (st_dump(geom)).geom as geom
--     from geo_propertyloc.aus_buildings_polygons
-- )
-- select bld_pid,
--        geom,
--        geom::geography as geog,
--        st_transform(geom, 28356) as geom_mga56
-- from bld
-- ;
-- analyse bushfire.temp_buildings;

-- -- WGA84 lat/long buildings with a 100m buffer
-- drop table if exists bushfire.buildings;
-- create table bushfire.buildings TABLESPACE bushfirespace as
-- select bld_pid,
--        st_asgeojson(geom, 6, 0)::jsonb as geom,
--        st_asgeojson(st_buffer(geom::geography, 100, 8), 6, 0)::jsonb as buffer
-- from bushfire.temp_buildings
-- ;
-- analyse bushfire.buildings;
--
-- -- MGA Zone 56 buildings with a 100m buffer
-- drop table if exists bushfire.buildings_mga56;
-- create table bushfire.buildings_mga56 TABLESPACE bushfirespace as
-- select bld_pid,
--        st_asgeojson(st_transform(geom, 28356), 1, 0)::jsonb as geom,
--        st_asgeojson(st_transform(st_buffer(geom::geography, 100.0, 8)::geometry, 28356), 1, 0)::jsonb as buffer
-- from bushfire.temp_buildings
-- ;
-- analyse bushfire.buildings_mga56;
