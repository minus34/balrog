

gdaldem slope /Users/s57405/tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem.tif /Users/s57405/tmp/bushfire/nsw_dcs/nsw_dcs_5m_slope.tif -s 111120 -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS





gdaldem slope /Users/s57405/tmp/bushfire/nsw_dcs/nsw_dcs_5m_dem.tif /Users/s57405/tmp/bushfire/nsw_dcs/nsw_dcs_5m_slope.tif -s 111120 -of GTiff -co BIGTIFF=YES -co TILED=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS






aws s3 sync /data/ s3://bushfire-rasters/nsw_dcs/5m_dem --exclude "*" --include "*.tif"




gdaladdo -minsize 1024 /data/nsw_dcs_5m_dem.tif