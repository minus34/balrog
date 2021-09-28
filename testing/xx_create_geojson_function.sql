
-- sample GeoJSON output
with cte as (
    select cad.jurisdiction_id,
           gnaf.gnaf_pid,
           concat(gnaf.address, ', ', gnaf.locality_name, ' ', gnaf.state, ' ', gnaf.postcode) as address,
           cad.geom
    from data_science.aus_cadastre_boundaries_nsw as cad
              inner join data_science.address_principals_nsw as gnaf on st_intersects(gnaf.geom, cad.geom)
    where gnaf.gnaf_pid in ('GANSW705023300', 'GANSW705012493', 'GANSW705023298')
)
select json_build_object(
               'type', 'FeatureCollection',
               'features', json_agg(ST_AsGeoJSON(cte.*)::jsonb)
           )
from cte
;
