
-- lookup table
select * from bushfire.nvis6_lookup
where nvis_id in (10822, 90288);




mvg_number,mvg_name,mvs_number,mvs_name
29,"Regrowth, modified native vegetation",91,Regrowth or modified shrublands



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


