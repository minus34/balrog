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

gdal_translate ./aac46307-fce8-449d-e044-00144fdd4fa6/hdr.adf ./cog/srtm_1sec_dem_s.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

aac46307-fce8-449d-e044-00144fdd4fa6/hdr.adf

aws s3 sync /data/tmp s3://bushfire-rasters/geoscience_australia/1sec-dem/raw/




gdal_translate nationalz56_ag.tif ./cog/nationalz56_ag.tif -of COG -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

cd ~




gdaldem slope input_dem output_slope_map
            [-p use percent slope (default=degrees)] [-s scale* (default=1)]
            [-alg Horn|ZevenbergenThorne]
            [-compute_edges] [-b Band (default=1)] [-of format] [-co "NAME=VALUE"]* [-q]


gdaldem slope ./cog/srtm_1sec_dem_s.tif ./cog/srtm_1sec_slope.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS

gdaldem aspect ./cog/srtm_1sec_dem_s.tif ./cog/srtm_1sec_aspect.tif -of COG -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS





#
## nope!
#aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/