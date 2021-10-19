
-- -- create a table of exploded polygons with BAL numbers -- 9.5 hours on MacBook
-- drop table if exists bushfire.nvis6_exploded;
-- create table bushfire.nvis6_exploded as
-- with veg as (
--     select nvisdsc1::integer as nvis_id,
--            (st_dump(st_force2d(wkb_geometry))).geom as geom
--     from bushfire.nvis6
-- )
-- select row_number() over () as gid,
--        veg.nvis_id,
--        lkp.mvg_number::smallint as veg_group,
--        lkp.mvs_number::smallint as veg_subgroup,
--        lkp.bal_number,
--        lkp.bal_name,
--        st_npoints(veg.geom) as point_count,
--        st_isvalid(veg.geom) as is_geom_valid,
--        st_makevalid(veg.geom) as geom
-- from veg
-- inner join bushfire.nvis6_lookup as lkp on veg.nvis_id = lkp.nvis_id
-- ;
--
-- -- only allow this after testing for non-polygon rows
-- delete from bushfire.nvis6_exploded
-- where st_geometrytype(geom) <> 'ST_Polygon'
-- ;
--
-- analyse bushfire.nvis6_exploded;
--
-- CREATE INDEX nvis6_exploded_bal_number_idx ON bushfire.nvis6_exploded USING btree (bal_number);
-- CREATE INDEX nvis6_exploded_geom_idx ON bushfire.nvis6_exploded USING gist (geom);
-- ALTER TABLE bushfire.nvis6_exploded CLUSTER ON nvis6_exploded_geom_idx;


--
-- -- merge with lookup table
-- drop table if exists temp_nvis6_merge;
-- create temporary table temp_nvis6_merge as
-- select lkp.mvg_number::smallint as veg_group,
--        lkp.mvs_number::smallint as veg_subgroup,
--        veg.geom
-- from temp_nvis6 as veg
-- inner join bushfire.nvis6_lookup as lkp on veg.nvis_id = lkp.nvis_id
-- ;
-- analyse temp_nvis6_merge;
--
-- CREATE INDEX temp_nvis6_merge_veg_groups_idx ON bushfire.nvis6_merge USING btree (veg_group, veg_subgroup);


-- union all polygons of the same vegetation group & subgroup
--   then split the non-contiguous ones into separate records
drop table if exists bushfire.nvis6_bal;
create table bushfire.nvis6_bal as
with merge as (
    select bal_number,
           bal_name,
           st_union(geom) as geom
    from bushfire.nvis6_exploded
    group by bal_number,
             bal_name
), polys as (
    select row_number() over () as gid,
           bal_number,
           bal_name,
           (st_dump(geom)).geom as geom
    from merge
)
select gid,
       bal_number,
       bal_name,
       st_area(geom::geography) / 10000.0 as area_ha,
       geom
from polys
;
analyse bushfire.nvis6_bal;

CREATE INDEX nvis6_bal_veg_group_idx ON bushfire.nvis6_bal USING btree (bal_number);
CREATE INDEX nvis6_bal_geom_idx ON bushfire.nvis6_bal USING gist (geom);
ALTER TABLE bushfire.nvis6_bal CLUSTER ON nvis6_bal_geom_idx;
