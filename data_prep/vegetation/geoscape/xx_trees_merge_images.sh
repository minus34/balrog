#!/usr/bin/env bash

conda activate geo

cd '/Users/s57405/Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Surface Cover/Surface Cover 2M JUNE 2021/Standard'

echo 'Processing MGA Zone 49'
gdal_merge.py -o temp_Z49.tif -of GTiff -n 0 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS WA_SURFACECOVER_2M_Z49_17508.tif WA_SURFACECOVER_2M_Z49_17496.tif WA_SURFACECOVER_2M_Z49_17495.tif temp_Z49_4326.tif WA_SURFACECOVER_2M_Z49_17488.tif
gdalwarp -t_srs EPSG:4326 -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS -overwrite temp_Z49.tif temp_Z49_4326.tif
rm temp_Z49.tif

echo 'Processing AU'
gdal_merge.py -o temp_au.tif -of GTiff -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS temp_Z49_4326.tif
gdal_translate temp_au.tif /Users/s57405/tmp/bushfire/veg/geoscape_2m_land_cover.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
rm temp_au.tif
aws s3 cp /Users/s57405/tmp/bushfire/veg/geoscape_2m_land_cover.tif s3://bushfire-rasters/geoscape/
