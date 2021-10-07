
-- add postgis to database
create extension if not exists postgis;

-- create schema
create schema if not exists bushfire;
alter schema bushfire owner to "ec2-user";

-- create image & label tables for both training and inference

drop table if exists bushfire.bal_factors;
create table bushfire.bal_factors (
    gnaf_pid text NOT NULL,
    address text NOT NULL,
    pr_pid text,
    building_pid text,
    aspect_bldg_min smallint,
    aspect_bldg_max smallint,
    aspect_bldg_avg smallint,
    aspect_bldg_std smallint,
    aspect_bldg_med smallint,
    aspect_bdy_min smallint,
    aspect_bdy_max smallint,
    aspect_bdy_avg smallint,
    aspect_bdy_std smallint,
    aspect_bdy_med smallint,
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
    slope_bdy_min smallint,
    slope_bdy_max smallint,
    slope_bdy_avg smallint,
    slope_bdy_std smallint,
    slope_bdy_med smallint,
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
    dem_bdy_min smallint,
    dem_bdy_max smallint,
    dem_bdy_avg smallint,
    dem_bdy_std smallint,
    dem_bdy_med smallint,
    dem_100m_min smallint,
    dem_100m_max smallint,
    dem_100m_avg smallint,
    dem_100m_std smallint,
    dem_100m_med smallint,
    latitude numeric(8,6) NOT NULL,
    longitude numeric(9,6) NOT NULL,
    point_geom geometry(Point, 4283) NOT NULL,
    geom geometry(Multipolygon, 4283) NULL
);

alter table bushfire.bal_factors owner to "ec2-user";

-- TODO: move these to after data import if this needs to scale
-- -- ALTER TABLE bushfire.bal_factors ADD CONSTRAINT bal_factors_pkey PRIMARY KEY (gnaf_pid);
-- CREATE INDEX bal_factors_gnaf_pid_idx ON bushfire.bal_factors USING btree (gnaf_pid);
-- CREATE INDEX bal_factors_point_geom_idx ON bushfire.bal_factors USING gist (point_geom);
-- CREATE INDEX bal_factors_geom_idx ON bushfire.bal_factors USING gist (geom);
-- ALTER TABLE bushfire.bal_factors CLUSTER ON bal_factors_geom_idx;
