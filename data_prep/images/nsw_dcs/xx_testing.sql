

select * from bushfire.nsw_elevation_index
where dems5mid = 'BaanBaa-DEM-AHD_55_5m';


select zone,
       dems5mid
from bushfire.nsw_elevation_index
;






-- ERROR 1: In file cpl_vsil_gzip.cpp, at line 1111, decompression failed with z_err = -1, return = 54
-- ERROR 3: /vsizip//vsicurl/https://portal.spatial.nsw.gov.au/download/dem/55/Euchareena-DEM-AHD_55_5m.zip/Euchareena-DEM-AHD_55_5m.asc, band 1: File short, can't read line 41.
-- ERROR 1: /vsizip//vsicurl/https://portal.spatial.nsw.gov.au/download/dem/55/Euchareena-DEM-AHD_55_5m.zip/Euchareena-DEM-AHD_55_5m.asc, band 1: IReadBlock failed at X offset 0, Y offset 41: /vsizip//vsicurl/https://portal.spatial.nsw.gov.au/download/dem/55/Euchareena-DEM-AHD_55_5m.zip/Euchareena-DEM-AHD_55_5m.asc, band 1: File short, can't read line 41.
-- ERROR 4: /data/tmp/Euchareena-DEM-AHD_55_5m.tif: No such file or directory