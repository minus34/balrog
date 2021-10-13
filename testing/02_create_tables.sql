
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
--     aspect_bldg_min smallint,
--     aspect_bldg_max smallint,
--     aspect_bldg_avg smallint,
--     aspect_bldg_std smallint,
--     aspect_bldg_med smallint,
    aspect_100m_min smallint,
    aspect_100m_max smallint,
    aspect_100m_avg smallint,
    aspect_100m_std smallint,
    aspect_100m_med smallint,
--     slope_bldg_min smallint,
--     slope_bldg_max smallint,
--     slope_bldg_avg smallint,
--     slope_bldg_std smallint,
--     slope_bldg_med smallint,
    slope_100m_min smallint,
    slope_100m_max smallint,
    slope_100m_avg smallint,
    slope_100m_std smallint,
    slope_100m_med smallint,
--     dem_bldg_min smallint,
--     dem_bldg_max smallint,
--     dem_bldg_avg smallint,
--     dem_bldg_std smallint,
--     dem_bldg_med smallint,
--     dem_100m_min smallint,
--     dem_100m_max smallint,
--     dem_100m_avg smallint,
--     dem_100m_std smallint,
--     dem_100m_med smallint
)
TABLESPACE bushfirespace;
alter table bushfire.bal_factors owner to "ec2-user";

-- TODO: move these to after data import if this needs to scale
-- ALTER TABLE bushfire.bal_factors ADD CONSTRAINT bal_factors_pkey PRIMARY KEY (gnaf_pid);
-- CREATE INDEX bal_factors_gnaf_pid_idx ON bushfire.bal_factors USING btree (gnaf_pid);
-- CREATE INDEX bal_factors_point_geom_idx ON bushfire.bal_factors USING gist (point_geom);
-- CREATE INDEX bal_factors_geom_idx ON bushfire.bal_factors USING gist (geom);
-- ALTER TABLE bushfire.bal_factors CLUSTER ON bal_factors_geom_idx;
