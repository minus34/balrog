#!/usr/bin/env bash

#cd /data/dem
#curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/nationalz56_ag.zip --progress-bar
#unzip -o nationalz56_ag.zip
#rm nationalz56_ag.zip
#cd ~
#
#gdal_translate nationalz56_ag.tif ./geotiff/nationalz56_ag.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
#
#aws s3 sync /data/dem/cog s3://bushfire-rasters/geoscience_australia/5m-dem/

#mkdir -p /data/dem/geotiff
#mkdir /data/dem/cog


# download and unzip SRTM 1 second (~30m) resolution smoothed elevation data
cd /data/dem
curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/1sec-dem/70715.zip --progress-bar
unzip -o 70715.zip
rm 70715.zip

# convert elevation data from ESRI Grid to GeoTIFF
gdal_translate ./aac46307-fce8-449d-e044-00144fdd4fa6/hdr.adf ./geotiff/srtm_1sec_dem_s.tif -of GTiff -co BIGTIFF=YES -co TILED=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
# create slope data
gdaldem slope ./geotiff/srtm_1sec_dem_s.tif ./geotiff/srtm_1sec_slope.tif -s 111120 -of GTiff -co BIGTIFF=YES -co TILED=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
# create aspect data
gdaldem aspect ./geotiff/srtm_1sec_dem_s.tif ./geotiff/srtm_1sec_aspect.tif -zero_for_flat -of GTiff -co BIGTIFF=YES -co TILED=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

# convert GeoTIFFs to Cloud Optimised GeoTIFFs
gdal_translate ./geotiff/srtm_1sec_dem_s.tif ./cog/srtm_1sec_dem_s.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
gdal_translate ./geotiff/srtm_1sec_slope.tif ./cog/srtm_1sec_slope.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS
gdal_translate ./geotiff/srtm_1sec_aspect.tif ./cog/srtm_1sec_aspect.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

# copy files to S3
aws s3 sync /data/dem s3://bushfire-rasters/geoscience_australia/1sec-dem/ --exclude "*" --include "*.tif"

cd ~
