#!/usr/bin/env bash


cd /data/tmp
curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/nationalz56_ag.zip --progress-bar
unzip -o nationalz56_ag.zip
rm nationalz56_ag.zip
cd ~


gdal_translate nationalz56_ag.tif ./cog/nationalz56_ag.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

aws s3 sync /data/tmp/cog s3://bushfire-rasters/geoscience_australia/5m-dem/


cd /data/tmp
curl -O https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/1sec-dem/70715.zip --progress-bar
unzip -o 70715.zip
rm 70715.zip

gdal_translate nationalz56_ag.tif ./cog/nationalz56_ag.tif -of COG -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

cd ~





#
## nope!
#aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/