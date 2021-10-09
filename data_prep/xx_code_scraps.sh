#!/usr/bin/env bash


/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/s57405/Downloads/buildings_json.dmp


# nope!
aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/