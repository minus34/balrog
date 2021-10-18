
-- add geoms to bal factor results
drop table if exists bushfire.bal_factors_srtm_1sec;
create table bushfire.bal_factors_srtm_1sec as -- 15,839,641 rows affected in 3 m 18 s 964 ms
select geo.bld_pid,
       aspect_min,
       aspect_max,
       aspect_avg,
       aspect_std,
       aspect_med,
       slope_min,
       slope_max,
       slope_avg,
       slope_std,
       slope_med,
       dem_min,
       dem_max,
       dem_avg,
       dem_std,
       dem_med,
       geo.grd_elev,
       geo.grd_el_src,
       geo.geom
from bushfire.bal_factors as bal
         inner join geo_propertyloc.aus_buildings_polygons as geo on bal.id = geo.bld_pid
;
analyse bushfire.bal_factors_srtm_1sec;

CREATE INDEX bal_factors_srtm_1sec_geom_idx ON bushfire.bal_factors_srtm_1sec USING gist (geom);
ALTER TABLE bushfire.bal_factors_srtm_1sec CLUSTER ON bal_factors_srtm_1sec_geom_idx;

select count(*) from bushfire.bal_factors_srtm_1sec;
