

-- drop table if exists bushfire.nsw_rfs_bushfire_prone_land;
-- create table bushfire.nsw_rfs_bushfire_prone_land
-- (
--     id serial
--         constraint nsw_rfs_bushfire_prone_land_pkey
--             primary key,
--     geom geometry(geometry,4283),
--     startdate date,
--     enddate date,
--     lastupdate date,
--     category integer,
--     guideline integer,
--     area bigint,
--     shape_leng double precision,
--     shape_area double precision,
--     d_category text,
--     d_guidelin text
-- );
--
-- alter table bushfire.nsw_rfs_bushfire_prone_land owner to postgres;


-- analyse bushfire.nsw_rfs_bushfire_prone_land
--
-- -- geometry types
-- select d_category,
--        st_geometrytype(geom) as geom_type,
--        sum(st_npoints(geom)) as point_count,
--        sum(st_numgeometries(geom)) as geom_count,
--        count(*) as row_count
-- from bushfire.nsw_rfs_bushfire_prone_land
-- group by d_category,
--          geom_type
-- order by d_category,
--          geom_type
-- ;




-- STEP 1 -- create a table of exploded, valid polygons -- 1 hour 30 mins
drop table if exists bushfire.nsw_rfs_bushfire_prone_land_exploded;
create table bushfire.nsw_rfs_bushfire_prone_land_exploded as
with bf as (
    select category::smallint as category,
           d_category,
           (st_dump(st_force2d(geom))).geom as geom
    from bushfire.nsw_rfs_bushfire_prone_land
)
select row_number() over () as gid,
       category,
       d_category,
       st_npoints(geom) as point_count,
       st_isvalid(geom) as is_geom_valid,
       st_makevalid(geom) as geom
from bf
;

-- only allow this after testing for non-polygon rows
-- delete from bushfire.nsw_rfs_bushfire_prone_land_exploded
select * from bushfire.nsw_rfs_bushfire_prone_land_exploded
where st_geometrytype(geom) <> 'ST_Polygon'
;
analyse bushfire.nsw_rfs_bushfire_prone_land_exploded;


-- STEP 2 -- subdivide polygons (some of them are very large) to speed up analysis -- 30 mins
drop table if exists bushfire.nsw_rfs_bushfire_prone_land_analysis;
create table bushfire.nsw_rfs_bushfire_prone_land_analysis as
select gid,
       category,
       d_category,
       st_subdivide(geom, 512) as geom
from bushfire.nsw_rfs_bushfire_prone_land_exploded
where st_geometrytype(geom) = 'ST_Polygon'
;
analyse bushfire.nsw_rfs_bushfire_prone_land_analysis;

CREATE INDEX nsw_rfs_bushfire_prone_land_analysis_d_category_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING btree (d_category);
CREATE INDEX nsw_rfs_bushfire_prone_land_analysis_category_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING btree (category);
CREATE INDEX nsw_rfs_bushfire_prone_land_analysis_geom_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING gist (geom);
ALTER TABLE bushfire.nsw_rfs_bushfire_prone_land_analysis CLUSTER ON nsw_rfs_bushfire_prone_land_analysis_geom_idx;


-- TODO: STEP 3 -- get rid of overlap errors in polygons
-- 1. cookie cut category 1 vs others
-- 2. cookie cut category 3 vs others
-- 3. cookie cut category 2 vs others