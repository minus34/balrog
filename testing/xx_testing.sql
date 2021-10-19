
-- dare to compare - NSW 5m DEM vs SRTM 1 sec DEM smoothed -- 1 min
drop table if exists bushfire.bal_factors_test_sydney_deltas;
create table bushfire.bal_factors_test_sydney_deltas as
with hmm as (
    select distinct nsw.bld_pid,
                    nsw.dem_min,
                    nsw.dem_max,
                    nsw.dem_avg,
                    nsw.dem_std,
                    nsw.dem_med,
                    nsw.dem_avg - srtm.dem_avg as dem_avg_delta,
                    nsw.dem_std - srtm.dem_std as dem_std_delta,
                    nsw.dem_med - srtm.dem_med as dem_med_delta,
                    nsw.aspect_min,
                    nsw.aspect_max,
                    nsw.aspect_avg,
                    nsw.aspect_std,
                    nsw.aspect_med,
                    srtm.aspect_min as aspect_min_srtm,
                    srtm.aspect_max as aspect_max_srtm,
                    srtm.aspect_avg as aspect_avg_srtm,
                    srtm.aspect_std as aspect_std_srtm,
                    srtm.aspect_med as aspect_med_srtm,
                    nsw.aspect_avg - srtm.aspect_avg as aspect_avg_delta,
                    nsw.aspect_std - srtm.aspect_std as aspect_std_delta,
                    nsw.aspect_med - srtm.aspect_med as aspect_med_delta,
                    nsw.slope_min,
                    nsw.slope_max,
                    nsw.slope_avg,
                    nsw.slope_std,
                    nsw.slope_med,
                    srtm.slope_min as slope_min_srtm,
                    srtm.slope_max as slope_max_srtm,
                    srtm.slope_avg as slope_avg_srtm,
                    srtm.slope_std as slope_std_srtm,
                    srtm.slope_med as slope_med_srtm,
                    nsw.slope_avg - srtm.slope_avg as slope_avg_delta,
                    nsw.slope_std - srtm.slope_std as slope_std_delta,
                    nsw.slope_med - srtm.slope_med as slope_med_delta
    from bushfire.bal_factors_sydney as nsw
             inner join bushfire.bal_factors_srtm_1sec as srtm on nsw.bld_pid = srtm.bld_pid
)
select hmm.*,
       bld.grd_elev,
       bld.grd_el_src,
       bld.geom
from hmm
inner join geo_propertyloc.aus_buildings_polygons as bld on hmm.bld_pid =  bld.bld_pid
where (abs(aspect_med_delta) between 45 and 315 and abs(slope_med_delta) > 5)
    or abs(slope_med_delta) > 5
;
analyse bushfire.bal_factors_test_sydney_deltas;

ALTER TABLE bushfire.bal_factors_test_sydney_deltas ADD CONSTRAINT bal_factors_test_sydney_deltas_pkey PRIMARY KEY (bld_pid);
CREATE INDEX bal_factors_test_sydney_deltas_geom_idx ON bushfire.bal_factors_test_sydney_deltas USING gist (geom);
ALTER TABLE bushfire.bal_factors_test_sydney_deltas CLUSTER ON bal_factors_test_sydney_deltas_geom_idx;


-- quick QA

-- 11330
select count(*) from bushfire.bal_factors_test_sydney_deltas;

-- 476
select count(*) as row_count,
       max(slope_100m_med_delta) as max_delta,
       avg(slope_100m_med_delta) as avg_delta
from bushfire.bal_factors_test_sydney_deltas
where slope_100m_med_delta > 10;

-- 4799
select count(*) from bushfire.bal_factors_test_sydney_deltas
where abs(aspect_med_delta) between 45 and 315
  and abs(slope_med_delta) > 5;

-- 481
select count(*) from bushfire.bal_factors_test_sydney_deltas
where abs(aspect_med_delta) between 45 and 315
  and abs(slope_med_delta) >= 10;



-- buildings -- 15841377
select count(*) from geo_propertyloc.aus_buildings_polygons
    where area > 50;




drop table if exists bushfire.bal_factors_sydney_5m;
create table bushfire.bal_factors_sydney_5m as -- 15,839,641 rows affected in 3 m 18 s 964 ms
select bal.*,
       geo.grd_elev,
       geo.grd_el_src,
       geo.geom
from bushfire.bal_factors_sydney as bal
         inner join geo_propertyloc.aus_buildings_polygons as geo on bal.bld_pid = geo.bld_pid
;
analyse bushfire.bal_factors_sydney_5m;

CREATE INDEX bal_factors_sydney_5m_geom_idx ON bushfire.bal_factors_sydney_5m USING gist (geom);
ALTER TABLE bushfire.bal_factors_sydney_5m CLUSTER ON bal_factors_sydney_5m_geom_idx;

select count(*) from bushfire.bal_factors_sydney_5m;

