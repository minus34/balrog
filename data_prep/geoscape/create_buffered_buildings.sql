--
-- Create 100m buffers around building outlines to calculate their aspect and slope
-- Requires a licensed copy of Geoscape's Buildings dataset: https://geoscape.com.au/data/buildings/
--

-- make 100m buffers out of 2D building outlines - these will be used to get aspect & slope -- ~50 mins
drop table if exists bushfire.temp_building_buffers;
create table bushfire.temp_building_buffers as
select bld_pid,
       st_buffer(ST_Force2D(geom)::geography, 100, 4) as geog
from geo_propertyloc.aus_buildings_polygons
;
analyse bushfire.temp_building_buffers;

-- ALTER TABLE bushfire.temp_building_buffers ADD CONSTRAINT temp_building_buffers_pkey PRIMARY KEY (bld_pid);

-- create building centroids for vegetation work
drop table if exists bushfire.temp_building_centroids;
create table bushfire.temp_building_centroids as
select bld_pid,
       st_y(st_centroid(geom))::numeric(8, 6) as latitude,
       st_x(st_centroid(geom))::numeric(9, 6) as longitude
from geo_propertyloc.aus_buildings_polygons
;
analyse bushfire.temp_building_centroids;
