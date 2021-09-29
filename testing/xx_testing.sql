


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
