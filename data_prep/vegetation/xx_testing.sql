
-- lookup table
select * from bushfire.nvis6_lookup
where nvis_id in (10822, 90288);



-- check exploded nvis polygons

-- geometry types
select st_geometrytype(geom) as geom_type,
       sum(st_numgeometries(geom)) as geom_count,
       count(*) as row_count
from bushfire.nvis6_exploded
group by geom_type
;

-- +---------------------+----------+---------+
-- |geom_type            |geom_count|row_count|
-- +---------------------+----------+---------+
-- |ST_GeometryCollection|4         |2        |
-- |ST_LineString        |3         |3        |
-- |ST_Polygon           |9408852   |9408852  |
-- +---------------------+----------+---------+

-- check invalid exploded geoms
select is_geom_valid,
       count(*) as row_count
from bushfire.nvis6_exploded
group by is_geom_valid
;

-- +-------------+---------+
-- |is_geom_valid|row_count|
-- +-------------+---------+
-- |false        |116744   |
-- |true         |9292113  |
-- +-------------+---------+

-- check for geoms that are still invalid -- 116,744 rows affected in 1 m 15 s 694 ms
drop table if exists temp_nvis6_invalid;
create temporary table temp_nvis6_invalid as
select geom
from bushfire.nvis6_exploded
where not is_geom_valid
;

-- 116744 geoms all valid! (35 mins to run)
select st_isvalid(geom) as is_geom_now_valid,
       count(*) as row_count
from temp_nvis6_invalid
group by is_geom_now_valid
;

-- check for geoms with less than 4 vertices --  rows
select count(*) as row_count
from bushfire.nvis6_exploded
where st_npoints(geom) < 4
;





-- check row & ID counts
select count(*) from bushfire.nvis6;  -- 9018062
select count(*) from bushfire.nvis6_exploded;  -- 9408857


select * from bushfire.nvis6_exploded;







select * from bushfire.nvis6_lookup
where nvis_id in (
    51258 )
;

select * from bushfire.nvis6_lookup
where mvg_number = 25
and mvs_number = 98
;


-- check polygon complexity (where not urban and other low risk cover)
select nvisdsc1,
       st_npoints((st_dump(wkb_geometry)).geom) as point_count
from bushfire.nvis6
where nvisdsc1 not in (select nvis_id from bushfire.nvis6_lookup where mvg_number = 25 and mvs_number = 98)
order by point_count desc
;



-- 33 major veg groups
select mvg_name,
       count(*) as nvis_id_count
from bushfire.nvis6_lookup
group by mvg_name;

-- 84 major veg subgroups
select mvs_name,
       count(*) as nvis_id_count
from bushfire.nvis6_lookup
group by mvs_name;

-- 141 combo major veg groups + subgroups
select mvg_name,
       mvs_name,
       count(*) as nvis_id_count
from bushfire.nvis6_lookup
group by mvg_name,
         mvs_name;


-- veg polygon table
select * from bushfire.nvis6;

-- 9,018,062
select count(*) from bushfire.nvis6;


-- Number of classes of vegetation -- 17540
with veg as (
    select count(*) as polygon_count,
           nvisdsc1
    from bushfire.nvis6
    group by nvisdsc1
)
select count(*)
from veg
;

-- add indexes
ALTER TABLE bushfire.nvis6_lookup ADD CONSTRAINT nvis6_lookup_pkey PRIMARY KEY (nvis_id);
CREATE INDEX nvis6_nvisdsc1_idx ON bushfire.nvis6 USING btree (nvisdsc1);


-- try a join -- 9018062 -- perfect match!
select count(*)
from bushfire.nvis6 as veg
inner join bushfire.nvis6_lookup as lkp on veg.nvisdsc1 = lkp.nvis_id::numeric(10,0)
;

-- areas per group & subgroup -- SLOW
select lkp.mvg_name,
       lkp.mvs_name,
       count(*) as nvis_id_count,
       sum(st_area(veg.wkb_geometry::geography)) / 10000.0 as area_ha
from bushfire.nvis6 as veg
inner join bushfire.nvis6_lookup as lkp on veg.nvisdsc1 = lkp.nvis_id::numeric(10,0)
group by lkp.mvg_name,
         lkp.mvs_name;

-- get group & subgroup IDs
select lkp.nvis_id::smallint,
       lkp.mvg_number::smallint,
       lkp.mvs_number::smallint,
       veg.wkb_geometry as geom
from bushfire.nvis6 as veg
inner join bushfire.nvis6_lookup as lkp on veg.nvisdsc1 = lkp.nvis_id::numeric(10,0)
;


