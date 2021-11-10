
-- make 110m buffers from GNAF - these will be used to get aspect & slope -- 12,299,552 rows affected in 55 m 49 s 881 ms
drop table if exists bushfire.temp_point_buffers;
create table bushfire.temp_point_buffers as
with fred as (
    select distinct geom
    from geo_propertyloc.aus_active_mgrs
)
select row_number() over () as gid,
       st_buffer(geom::geography, 110, 4) as geog
from fred
;
analyse bushfire.temp_point_buffers;

