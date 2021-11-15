#!/usr/bin/env bash

# copy mega-DEM
aws s3 cp s3://bushfire-rasters/geoscience_australia/1sec-dem/geotiff/srtm_1sec_slope.tif /Users/$(whoami)/tmp/bushfire/


# export and copy GNAF pids & coords
/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -h localhost -d geo -t bushfire.temp_point_buffers -p 5432 -U s57405 -f /Users/$(whoami)/tmp/bushfire/gnaf.dmp --no-owner
aws s3 cp /Users/$(whoami)/tmp/bushfire/gnaf.dmp s3://bushfire-rasters/geoscape/
