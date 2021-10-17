
-- add geoms to bal factor results
drop table if exists bushfire.bal_factors_srtm_1sec;
create table bushfire.bal_factors_srtm_1sec as -- 15,839,641 rows affected in 3 m 18 s 964 ms
select bal.*,
       geo.grd_elev,
       geo.grd_el_src,
       geo.geom
from bushfire.bal_factors as bal
         inner join geo_propertyloc.aus_buildings_polygons as geo on bal.bld_pid = geo.bld_pid
;
analyse bushfire.bal_factors_srtm_1sec;

CREATE INDEX bal_factors_srtm_1sec_geom_idx ON bushfire.bal_factors_srtm_1sec USING gist (geom);
ALTER TABLE bushfire.bal_factors_srtm_1sec CLUSTER ON bal_factors_srtm_1sec_geom_idx;

select count(*) from bushfire.bal_factors_srtm_1sec;
