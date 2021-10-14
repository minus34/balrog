#!/usr/bin/env bash


# remote dump of results
pg_dump -Fc -d geo -t bushfire.bal_factors -p 5432 -U "ec2-user" -f "/data/bal_factors.dmp" --no-owner
aws s3 cp /data/bal_factors.dmp s3://bushfire-rasters/output/

# local
aws s3 cp s3://bushfire-rasters/output/bal_factors.dmp /Users/s57405/Downloads/
psql -d geo -c "drop table bushfire.bal_factors"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/s57405/Downloads/bal_factors.dmp






## nope - GA's S3 bucket doesn't allow sync!
#aws s3 sync s3://elevation-direct-downloads/5m-dem/national_utm_mosaics s3://bushfire-rasters/geoscience_australia/5m-dem/national_utm_mosaics/





