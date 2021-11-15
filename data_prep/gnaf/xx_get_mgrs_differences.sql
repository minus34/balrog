
select count(*)
from geo_propertyloc.aus_active_mgrs as mgrs
left outer join bushfire.bal_factors_gnaf as gnaf on mgrs.ext_geo_id = concat('A', gnaf.id)
where gnaf.id is NULL
;


select count(*)
from geo_propertyloc.aus_active_mgrs;


