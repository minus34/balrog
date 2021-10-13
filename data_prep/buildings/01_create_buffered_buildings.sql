--
-- Requires a licensed copy of Geoscape's Buildings dataset: https://geoscape.com.au/data/buildings/
--

-- make 100m buffers out of 2D building outlines - these will be used to get aspect & slope -- 46 mins
drop table if exists bushfire.temp_building_buffers;
create table bushfire.temp_building_buffers as
select bld_pid,
       st_buffer(ST_Force2D(geom)::geography, 100, 4) as geog
from geo_propertyloc.aus_buildings_polygons
;
analyse bushfire.temp_building_buffers;

ALTER TABLE bushfire.temp_building_buffers ADD CONSTRAINT temp_building_buffers_pkey PRIMARY KEY (bld_pid);
