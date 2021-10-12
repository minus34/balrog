
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

CREATE INDEX bal_factors_test_sydney_srtm_gnaf_pid_idx ON bushfire.bal_factors_test_sydney_srtm USING btree (gnaf_pid);
CREATE INDEX bal_factors_test_sydney_srtm_point_geom_idx ON bushfire.bal_factors_test_sydney_srtm USING gist (point_geom);
CREATE INDEX bal_factors_test_sydney_srtm_geom_idx ON bushfire.bal_factors_test_sydney_srtm USING gist (geom);
ALTER TABLE bushfire.bal_factors_test_sydney_srtm CLUSTER ON bal_factors_test_sydney_srtm_geom_idx;




-- dare to compare - NSW 5m DEM vs SRTM 1 sec DEM smoothed
drop table if exists bushfire.bal_factors_test_sydney_deltas;
create table bushfire.bal_factors_test_sydney_deltas as
with hmm as (
    select distinct nsw.gnaf_pid,
           nsw.address,
           nsw.pr_pid,
           nsw.dem_bdy_min,
           nsw.dem_bdy_max,
           nsw.dem_bdy_avg,
           nsw.dem_bdy_std,
           nsw.dem_bdy_med,
           nsw.dem_100m_min,
           nsw.dem_100m_max,
           nsw.dem_100m_avg,
           nsw.dem_100m_std,
           nsw.dem_100m_med,
           nsw.point_geom,
           nsw.geom,
           nsw.dem_bdy_avg - srtm.dem_bdy_avg   as dem_bdy_avg_delta,
           nsw.dem_bdy_std - srtm.dem_bdy_std   as dem_bdy_std_delta,
           nsw.dem_bdy_med - srtm.dem_bdy_med   as dem_bdy_med_delta,
           nsw.dem_100m_avg - srtm.dem_100m_avg as dem_100m_avg_delta,
           nsw.dem_100m_std - srtm.dem_100m_std as dem_100m_std_delta,
           nsw.dem_100m_med - srtm.dem_100m_med as dem_100m_med_delta
    from bushfire.bal_factors_test_sydney as nsw
             inner join bushfire.bal_factors_test_sydney_srtm as srtm on nsw.gnaf_pid = srtm.gnaf_pid
)
select *
from hmm where abs(dem_100m_med_delta) > 5
;
analyse bushfire.bal_factors_test_sydney_deltas;

CREATE INDEX bal_factors_test_sydney_deltas_gnaf_pid_idx ON bushfire.bal_factors_test_sydney_deltas USING btree (gnaf_pid);
CREATE INDEX bal_factors_test_sydney_deltas_point_geom_idx ON bushfire.bal_factors_test_sydney_deltas USING gist (point_geom);
CREATE INDEX bal_factors_test_sydney_deltas_geom_idx ON bushfire.bal_factors_test_sydney_deltas USING gist (geom);
ALTER TABLE bushfire.bal_factors_test_sydney_deltas CLUSTER ON bal_factors_test_sydney_deltas_geom_idx;



-- buildings -- 15841377
select count(*) from geo_propertyloc.aus_buildings_polygons
    where area > 50







-- create copy of GNAF + properties being used for testing
drop table if exists bushfire.gnaf_sydney;
create table bushfire.gnaf_sydney as
select pr_pid,
       gnaf_pid,
       address,
       latitude,
       longitude,
       point_geom,
       st_asgeojson(st_transform(geom, 4326), 6)::jsonb as geometry,
       st_asgeojson(st_transform(st_buffer(geom::geography, 100)::geometry, 4326), 6)::jsonb as buffer,
       geom
from bushfire.bal_factors_test_sydney
;
analyse bushfire.bal_factors_test_sydney;



-- create Geoscape building buffer table

-- convert to 2D polygons (they aren't multipolygons!) -- 15,841,377 rows affected in 5 mins
drop table if exists bushfire.temp_buildings;
create table bushfire.temp_buildings as
select bld_pid,
       ST_Force2D((st_dump(geom)).geom) as geom
from geo_propertyloc.aus_buildings_polygons
;
analyse bushfire.temp_buildings;

CREATE INDEX temp_buildings_geom_idx ON bushfire.temp_buildings USING gist (geom);
ALTER TABLE bushfire.temp_buildings CLUSTER ON temp_buildings_geom_idx;


-- WGA84 lat/long buildings with a 100m buffer -- 15,841,377 rows affected in 39 m 55 s 332 ms
drop table if exists bushfire.buildings;
create table bushfire.buildings as
select bld_pid,
--        st_asgeojson(geom, 6, 0)::jsonb as geom,
       st_asgeojson(st_buffer(geom::geography, 100, 8), 6, 0)::jsonb as buffer
from bushfire.temp_buildings
;
analyse bushfire.buildings;

-- MGA Zone 56 buildings with a 100m buffer -- 15,841,377 rows affected in 44 m 23 s 966 ms
drop table if exists bushfire.buildings_mga56;
create table bushfire.buildings_mga56 as
select bld_pid,
--        st_asgeojson(st_transform(geom, 28356), 1, 0)::jsonb as geom,
       st_asgeojson(st_transform(st_buffer(geom::geography, 100.0, 8)::geometry, 28356), 1, 0)::jsonb as buffer
from bushfire.temp_buildings
;
analyse bushfire.buildings_mga56;



-- WGA84 lat/long buildings with a 100m buffer within the Sydney Map Grid --
drop table if exists bushfire.buildings_sydney;
create table bushfire.buildings_sydney as
with nsw as (
    select geom as geom
    from bushfire.nsw_elevation_index
    where maptitle = 'SYDNEY'
)
select bld.bld_pid,
--        st_asgeojson(geom, 6, 0)::jsonb as geom,
       st_asgeojson(st_buffer(bld.geom::geography, 100, 8), 6, 0)::jsonb as buffer
from bushfire.temp_buildings as bld
         inner join nsw on st_intersects(bld.geom, nsw.geom)
;
analyse bushfire.buildings_sydney;

-- MGA Zone 56 buildings with a 100m buffer within the Sydney Map Grid --
drop table if exists bushfire.buildings_mga56_sydney;
create table bushfire.buildings_mga56_sydney as
with nsw as (
    select geom as geom
    from bushfire.nsw_elevation_index
    where maptitle = 'SYDNEY'
)
select bld.bld_pid,
--        st_asgeojson(st_transform(geom, 28356), 1, 0)::jsonb as geom,
       st_asgeojson(st_transform(st_buffer(bld.geom::geography, 100.0, 8)::geometry, 28356), 1, 0)::jsonb as buffer
from bushfire.temp_buildings as bld
inner join nsw on st_intersects(bld.geom, nsw.geom)
;
analyse bushfire.buildings_mga56_sydney;



-- vacuum analyse bushfire.buildings;
-- vacuum analyse bushfire.buildings_mga56;

-- 15841377
select count(*) from bushfire.buildings_mga56;



drop view if exists bushfire.vw_bal_factors;
create view bushfire.vw_bal_factors as
select bal.*,
       geo.geom
from bushfire.bal_factors as bal
inner join geo_propertyloc.aus_buildings_polygons as geo on bal.bld_pid = geo.bld_pid
;



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
