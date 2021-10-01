




-- https://portal.spatial.nsw.gov.au/download/contours/56/StAlbans-CONT-AHD_56_2m.zip
-- https://portal.spatial.nsw.gov.au/download/dem/56/StAlbans-DEM-AHD_56_5m.zip
-- https://portal.spatial.nsw.gov.au/download/slope/56/StAlbans-SLP-AHD_56_5m.zip
-- https://portal.spatial.nsw.gov.au/download/aspect/56/StAlbans-ASP-AHD_56_5m.zip


select * from bushfire.nsw_elevation_index;






select * from geo_propertyloc.rf_aus_property_parcel_polygon










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
