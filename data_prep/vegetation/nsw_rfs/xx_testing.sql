

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




-- geometry types
select st_geometrytype(geom) as geom_type,
       sum(st_numgeometries(geom)) as geom_count,
       count(*) as row_count
from bushfire.nsw_rfs_bushfire_prone_land
group by geom_type
;



-- subdivide polygons (some of them are very large) to speed up analysis
drop table if exists bushfire.nsw_rfs_bushfire_prone_land_analysis;
create table bushfire.nsw_rfs_bushfire_prone_land_analysis as
select category,
       d_category,
       st_subdivide(st_makevalid(geom), 512) as geom
from bushfire.nsw_rfs_bushfire_prone_land
where st_geometrytype(geom) in ('ST_Polygon', 'ST_MultiPolygon')
;
analyse bushfire.nsw_rfs_bushfire_prone_land_analysis;

CREATE INDEX nvis6_bal_analysis_d_category_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING btree (d_category);
CREATE INDEX nvis6_bal_analysis_category_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING btree (category);
CREATE INDEX nvis6_bal_analysis_geom_idx ON bushfire.nsw_rfs_bushfire_prone_land_analysis USING gist (geom);
ALTER TABLE bushfire.nsw_rfs_bushfire_prone_land_analysis CLUSTER ON nvis6_bal_analysis_geom_idx;

