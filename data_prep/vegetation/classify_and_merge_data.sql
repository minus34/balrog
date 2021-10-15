
-- create a temp table of exploded polygons
drop table if exists temp_nvis6;
create temporary table temp_nvis6 as
select nvisdsc1::integer as nvis_id,
       (st_dump(wkb_geometry)).geom as geom
from bushfire.nvis6
;
analyse temp_nvis6;

-- merge with lookup table
drop table if exists temp_nvis6_merge;
create temporary table temp_nvis6_merge as
select lkp.mvg_number::smallint as veg_group,
       lkp.mvs_number::smallint as veg_subgroup,
       veg.geom
from temp_nvis6 as veg
inner join bushfire.nvis6_lookup as lkp on veg.nvis_id = lkp.nvis_id
;
analyse temp_nvis6_merge;

CREATE INDEX temp_nvis6_merge_veg_groups_idx ON bushfire.nvis6_merge USING btree (veg_group, veg_subgroup);


-- union all polygons of the same vegetation group & subgroup
--   then split the non-contiguous ones into separate records
drop table if exists bushfire.nvis6_merge;
create table bushfire.nvis6_merge as
with merge as (
    select veg_group,
           veg_subgroup,
           st_union(geom) as geom
    from temp_nvis6_merge
    group by veg_group,
             veg_subgroup
), polys as (
    select row_number() over () as gid,
           veg_group,
           veg_subgroup,
           (st_dump(geom)).geom as geom
    from merge
)
select gid,
       veg_group,
       veg_subgroup,
       st_area(geom::geography) / 10000.0 as area_ha,
       geom
from polys
;

CREATE INDEX nvis6_merge_veg_group_idx ON bushfire.nvis6_merge USING btree (veg_group);
CREATE INDEX nvis6_merge_veg_subgroup_idx ON bushfire.nvis6_merge USING btree (veg_subgroup);
CREATE INDEX nvis6_merge_geom_idx ON bushfire.nvis6_merge USING gist (geom);
ALTER TABLE bushfire.nvis6_merge CLUSTER ON nvis6_merge_geom_idx;

drop table if exists temp_nvis6_union;
drop table if exists temp_nvis6_merge;
drop table if exists temp_nvis6;