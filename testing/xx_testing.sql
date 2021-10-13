
-- -- create backup table for testing
-- drop table if exists bushfire.bal_factors_test_sydney;
-- create table bushfire.bal_factors_test_sydney as
-- select * from bushfire.bal_factors;
-- analyse bushfire.bal_factors_test_sydney;
--
-- CREATE INDEX bal_factors_test_sydney_gnaf_pid_idx ON bushfire.bal_factors_test_sydney USING btree (gnaf_pid);
-- CREATE INDEX bal_factors_test_sydney_point_geom_idx ON bushfire.bal_factors_test_sydney USING gist (point_geom);
-- CREATE INDEX bal_factors_test_sydney_geom_idx ON bushfire.bal_factors_test_sydney USING gist (geom);
-- ALTER TABLE bushfire.bal_factors_test_sydney CLUSTER ON bal_factors_test_sydney_geom_idx;


-- drop table if exists bushfire.bal_factors_test_sydney_srtm;
-- create table bushfire.bal_factors_test_sydney_srtm as
-- select * from bushfire.bal_factors;
-- analyse bushfire.bal_factors_test_sydney_srtm;

-- CREATE INDEX bal_factors_test_sydney_srtm_gnaf_pid_idx ON bushfire.bal_factors_test_sydney_srtm USING btree (gnaf_pid);
-- CREATE INDEX bal_factors_test_sydney_srtm_point_geom_idx ON bushfire.bal_factors_test_sydney_srtm USING gist (point_geom);
-- CREATE INDEX bal_factors_test_sydney_srtm_geom_idx ON bushfire.bal_factors_test_sydney_srtm USING gist (geom);
-- ALTER TABLE bushfire.bal_factors_test_sydney_srtm CLUSTER ON bal_factors_test_sydney_srtm_geom_idx;




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
    where area > 50







-- -- create copy of GNAF + properties being used for testing
-- drop table if exists bushfire.gnaf_sydney;
-- create table bushfire.gnaf_sydney as
-- select pr_pid,
--        gnaf_pid,
--        address,
--        latitude,
--        longitude,
--        point_geom,
--        st_asgeojson(st_transform(geom, 4326), 6)::jsonb as geometry,
--        st_asgeojson(st_transform(st_buffer(geom::geography, 100)::geometry, 4326), 6)::jsonb as buffer,
--        geom
-- from bushfire.bal_factors_test_sydney
-- ;
-- analyse bushfire.bal_factors_test_sydney;



-- create Geoscape building buffer table







-- -- convert to 2D polygons (they aren't multipolygons!) -- 15,841,377 rows affected in 5 mins
-- drop table if exists bushfire.temp_buildings;
-- create table bushfire.temp_buildings as
-- select bld_pid,
--        ST_Force2D((st_dump(geom)).geom) as geom
-- from geo_propertyloc.aus_buildings_polygons
-- ;
-- analyse bushfire.temp_buildings;
--
-- CREATE INDEX temp_buildings_geom_idx ON bushfire.temp_buildings USING gist (geom);
-- ALTER TABLE bushfire.temp_buildings CLUSTER ON temp_buildings_geom_idx;


-- make buffers -- 46 mins
drop table if exists bushfire.temp_building_buffers;
create table bushfire.temp_building_buffers as
select bld_pid,
       st_buffer(ST_Force2D(geom)::geography, 100, 4) as geog
from geo_propertyloc.aus_buildings_polygons
;
analyse bushfire.temp_building_buffers;

ALTER TABLE bushfire.temp_building_buffers ADD CONSTRAINT temp_building_buffers_pkey PRIMARY KEY (bld_pid);
-- CREATE INDEX temp_building_buffers_geom_idx ON bushfire.temp_building_buffers USING gist (geom);
-- ALTER TABLE bushfire.temp_building_buffers CLUSTER ON temp_building_buffers_geom_idx;


-- -- WGA84 lat/long buildings with a 100m buffer -- 15,841,377 rows affected in 39 m 55 s 332 ms
-- drop table if exists bushfire.buildings;
-- create table bushfire.buildings as
-- select bld_pid,
-- --        st_asgeojson(geom, 6, 0)::jsonb as geom,
--        st_asgeojson(st_buffer(geom::geography, 100, 4), 6, 0)::jsonb as buffer
-- from bushfire.temp_buildings
-- ;
-- analyse bushfire.buildings;
--
-- -- MGA Zone 56 buildings with a 100m buffer -- 15,841,377 rows affected in 44 m 23 s 966 ms
-- drop table if exists bushfire.buildings_mga56;
-- create table bushfire.buildings_mga56 as
-- select bld_pid,
-- --        st_asgeojson(st_transform(geom, 28356), 1, 0)::jsonb as geom,
--        st_asgeojson(st_transform(st_buffer(geom::geography, 100.0, 8)::geometry, 28356), 1, 0)::jsonb as buffer
-- from bushfire.temp_buildings
-- ;
-- analyse bushfire.buildings_mga56;



-- -- WGS84 lat/long buildings with a 100m buffer within the Sydney Map Grid --
-- drop table if exists bushfire.buildings_sydney;
-- create table bushfire.buildings_sydney as
-- with nsw as (
--     select geom as geom
--     from bushfire.nsw_elevation_index
--     where maptitle = 'SYDNEY'
-- )
-- select bld.bld_pid,
-- --        st_asgeojson(geom, 6, 0)::jsonb as geom,
--        st_asgeojson(st_buffer(bld.geom::geography, 100, 8), 6, 0)::jsonb as buffer
-- from bushfire.temp_buildings as bld
--          inner join nsw on st_intersects(bld.geom, nsw.geom)
-- ;
-- analyse bushfire.buildings_sydney;

-- -- MGA Zone 56 buildings with a 100m buffer within the Sydney Map Grid --
-- drop table if exists bushfire.buildings_mga56_sydney;
-- create table bushfire.buildings_mga56_sydney as
-- with nsw as (
--     select geom as geom
--     from bushfire.nsw_elevation_index
--     where maptitle = 'SYDNEY'
-- )
-- select bld.bld_pid,
-- --        st_asgeojson(st_transform(geom, 28356), 1, 0)::jsonb as geom,
--        st_asgeojson(st_transform(st_buffer(bld.geom::geography, 100.0, 8)::geometry, 28356), 1, 0)::jsonb as buffer
-- from bushfire.temp_buildings as bld
-- inner join nsw on st_intersects(bld.geom, nsw.geom)
-- ;
-- analyse bushfire.buildings_mga56_sydney;



-- vacuum analyse bushfire.buildings;
-- vacuum analyse bushfire.buildings_mga56;

-- 15841377
select count(*) from bushfire.buildings_mga56;



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




-- 511 missing some data - these are islands outside the SRTM DEM
select * from bushfire.bal_factors_srtm_1sec
where dem_100m_med = -9999
    or aspect_100m_med = -9999
    or slope_100m_med = -9999
;



select * from bushfire.bal_factors_srtm_1sec
where bld_pid = 'bld34e8a3234b24';


select * from bushfire.bal_factors
where bld_pid = 'bld34e8a3234b24';



select * from geo_propertyloc.rf_aus_property_parcel_polygon;






with gnaf as (
    select gnaf_pid,
           concat(address, ', ', locality_name, ' ', state, ' ', postcode) as address,
           latitude,
           longitude,
           geom as point_geom
    from gnaf_202108.address_principals
    where coalesce(primary_secondary, 'P') = 'P'
        and gnaf_pid in ('GANSW705023300', 'GANSW705012493', 'GANSW705023298')
)
select cad.cad_pid,
       gnaf.*,
       st_asgeojson(st_transform(cad.geom, 28356), 1)::jsonb as geometry,
       st_asgeojson(st_buffer(st_transform(cad.geom, 28356), 100.0), 1)::jsonb as buffer,
       cad.geom
from data_science.aus_cadastre_boundaries_nsw as cad
         inner join gnaf on st_intersects(gnaf.point_geom, cad.geom)
;





SELECT gid, bld_pid, add_pid, add_dt_pid, address, dt_create, dt_mod, rel_conf, is_resi
FROM geo_propertyloc.rf_aus_building_address
limit 10;


SELECT gid, bld_pid, pr_pid, dt_create, dt_mod, rel_conf
FROM geo_propertyloc.rf_aus_building_property
limit 10;


SELECT gid, pr_ply_pid, pr_pid, cntrb_id, dt_create, dt_mod, state, source, base_prop, area, geom
FROM geo_propertyloc.rf_aus_property_parcel_polygon
limit 10;


SELECT id, bld_pid, geom, dt_create, dt_mod, capt_date, bld_rv_dt, bld_src, qual_class, loc_pid, state, add_count, sp_adj, sp_rv_dt, solar_p, solp_rv_dt, plan_zone, roof_hgt, eave_hgt, grd_elev, grd_el_src, pr_rf_mat, roof_type, roof_clr, mb_code, num_vert, area, volume, bld_gm_qlt, cntrd_long, cntrd_lat, geoscape_version, schema_version, geom_json
FROM geo_propertyloc.aus_buildings_polygons
limit 10;








-- -- sample GeoJSON output
-- with cte as (
--     select cad.jurisdiction_id,
--            gnaf.gnaf_pid,
--            concat(gnaf.address, ', ', gnaf.locality_name, ' ', gnaf.state, ' ', gnaf.postcode) as address,
--            st_transform(cad.geom, 28356) as geom
--     from data_science.aus_cadastre_boundaries_nsw as cad
--               inner join data_science.address_principals_nsw as gnaf on st_intersects(gnaf.geom, cad.geom)
--     where gnaf.gnaf_pid in ('GANSW705023300', 'GANSW705012493', 'GANSW705023298')
-- )
-- select json_build_object(
--                'type', 'FeatureCollection',
--                'features', json_agg(ST_AsGeoJSON(cte.*)::jsonb)
--            )
-- from cte
-- ;
