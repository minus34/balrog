
-- dare to compare - NSW 5m DEM vs SRTM 1 sec DEM smoothed -- 1 min
drop table if exists bushfire.bal_factors_test_sydney_deltas;
create table bushfire.bal_factors_test_sydney_deltas as
with hmm as (
    select distinct nsw.bld_pid,
--                     nsw.dem_100m_min,
--                     nsw.dem_100m_max,
--                     nsw.dem_100m_avg,
--                     nsw.dem_100m_std,
--                     nsw.dem_100m_med,
--                     nsw.dem_100m_avg - srtm.dem_100m_avg as dem_100m_avg_delta,
--                     nsw.dem_100m_std - srtm.dem_100m_std as dem_100m_std_delta,
--                     nsw.dem_100m_med - srtm.dem_100m_med as dem_100m_med_delta,
                    nsw.aspect_100m_min,
                    nsw.aspect_100m_max,
                    nsw.aspect_100m_avg,
                    nsw.aspect_100m_std,
                    nsw.aspect_100m_med,
                    srtm.aspect_100m_min as aspect_100m_min_srtm,
                    srtm.aspect_100m_max as aspect_100m_max_srtm,
                    srtm.aspect_100m_avg as aspect_100m_avg_srtm,
                    srtm.aspect_100m_std as aspect_100m_std_srtm,
                    srtm.aspect_100m_med as aspect_100m_med_srtm,
                    nsw.aspect_100m_avg - srtm.aspect_100m_avg as aspect_100m_avg_delta,
                    nsw.aspect_100m_std - srtm.aspect_100m_std as aspect_100m_std_delta,
                    nsw.aspect_100m_med - srtm.aspect_100m_med as aspect_100m_med_delta,
                    nsw.slope_100m_min,
                    nsw.slope_100m_max,
                    nsw.slope_100m_avg,
                    nsw.slope_100m_std,
                    nsw.slope_100m_med,
                    srtm.slope_100m_min as slope_100m_min_srtm,
                    srtm.slope_100m_max as slope_100m_max_srtm,
                    srtm.slope_100m_avg as slope_100m_avg_srtm,
                    srtm.slope_100m_std as slope_100m_std_srtm,
                    srtm.slope_100m_med as slope_100m_med_srtm,
                    nsw.slope_100m_avg - srtm.slope_100m_avg as slope_100m_avg_delta,
                    nsw.slope_100m_std - srtm.slope_100m_std as slope_100m_std_delta,
                    nsw.slope_100m_med - srtm.slope_100m_med as slope_100m_med_delta
    from bushfire.bal_factors_sydney as nsw
             inner join bushfire.bal_factors_srtm_1sec as srtm on nsw.bld_pid = srtm.bld_pid
)
select hmm.*,
       bld.geom
from hmm
inner join geo_propertyloc.aus_buildings_polygons as bld on hmm.bld_pid =  bld.bld_pid
where (abs(aspect_100m_med_delta) between 45 and 315 and abs(slope_100m_med_delta) > 5)
    or abs(slope_100m_med_delta) > 5
;
analyse bushfire.bal_factors_test_sydney_deltas;

ALTER TABLE bushfire.bal_factors_test_sydney_deltas ADD CONSTRAINT bal_factors_test_sydney_deltas_pkey PRIMARY KEY (bld_pid);
CREATE INDEX bal_factors_test_sydney_deltas_geom_idx ON bushfire.bal_factors_test_sydney_deltas USING gist (geom);
ALTER TABLE bushfire.bal_factors_test_sydney_deltas CLUSTER ON bal_factors_test_sydney_deltas_geom_idx;


-- quick QA

-- 11330
select count(*) from bushfire.bal_factors_test_sydney_deltas;

-- 882
select count(*) from bushfire.bal_factors_test_sydney_deltas
where slope_100m_med_delta >= 10;

-- 4799
select count(*) from bushfire.bal_factors_test_sydney_deltas
where abs(aspect_100m_med_delta) between 45 and 315
  and abs(slope_100m_med_delta) > 5;

-- 481
select count(*) from bushfire.bal_factors_test_sydney_deltas
where abs(aspect_100m_med_delta) between 45 and 315
  and abs(slope_100m_med_delta) >= 10;



-- buildings -- 15841377
select count(*) from geo_propertyloc.aus_buildings_polygons
    where area > 50;



-- add geoms to bal factor results
drop table if exists bushfire.bal_factors_srtm_1sec;
create table bushfire.bal_factors_srtm_1sec as -- 15,839,641 rows affected in 3 m 18 s 964 ms
select bal.*,
       geo.geom
from bushfire.bal_factors as bal
inner join geo_propertyloc.aus_buildings_polygons as geo on bal.bld_pid = geo.bld_pid
;
analyse bushfire.bal_factors_srtm_1sec;

CREATE INDEX bal_factors_srtm_1sec_geom_idx ON bushfire.bal_factors_srtm_1sec USING gist (geom);
ALTER TABLE bushfire.bal_factors_srtm_1sec CLUSTER ON bal_factors_srtm_1sec_geom_idx;

select count(*) from bushfire.bal_factors_srtm_1sec;


drop table if exists bushfire.bal_factors_sydney_5m;
create table bushfire.bal_factors_sydney_5m as -- 15,839,641 rows affected in 3 m 18 s 964 ms
select bal.*,
       geo.geom
from bushfire.bal_factors_sydney as bal
         inner join geo_propertyloc.aus_buildings_polygons as geo on bal.bld_pid = geo.bld_pid
;
analyse bushfire.bal_factors_sydney_5m;

CREATE INDEX bal_factors_sydney_5m_geom_idx ON bushfire.bal_factors_sydney_5m USING gist (geom);
ALTER TABLE bushfire.bal_factors_sydney_5m CLUSTER ON bal_factors_sydney_5m_geom_idx;

select count(*) from bushfire.bal_factors_sydney_5m;
