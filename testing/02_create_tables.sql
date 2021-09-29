
-- add postgis to database
create extension if not exists postgis;

-- create schema
create schema if not exists data_science;
alter schema data_science owner to "postgres";

-- create image & label tables for both training and inference

drop table if exists data_science.bal_factors;
create table data_science.bal_factors (
    gnaf_pid text NOT NULL,
    address text NOT NULL,
    cad_pid text,
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
    geom geometry(Polygon, 4283) NULL
);

alter table data_science.bal_factors owner to "postgres";

-- TODO: move these to after data import if this needs to scale
ALTER TABLE data_science.bal_factors ADD CONSTRAINT bal_factors_pkey PRIMARY KEY (gnaf_pid);
CREATE INDEX bal_factors_point_geom_idx ON data_science.bal_factors USING gist (point_geom);
CREATE INDEX bal_factors_geom_idx ON data_science.bal_factors USING gist (geom);
ALTER TABLE data_science.bal_factors CLUSTER ON bal_factors_geom_idx;
