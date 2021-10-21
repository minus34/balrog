#!/usr/bin/env bash

TABLE_NAME="nvis6_bal"

# remote dump of results
pg_dump -Fc -d geo -t "bushfire.${TABLE_NAME}" -p 5432 -U "ec2-user" -f "/data/${TABLE_NAME}.dmp" --no-owner
aws s3 cp "/data/${TABLE_NAME}.dmp" s3://bushfire-rasters/vegetation/


# local restore
aws s3 cp "s3://bushfire-rasters/vegetation/${TABLE_NAME}.dmp" /Users/$(whoami)/tmp/bushfire/veg/
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres "/Users/$(whoami)/tmp/bushfire/veg/${TABLE_NAME}.dmp" --clean





# export NVIS BAL data to flatgeobuf & copy to S3
OUTPUT_FOLDER=""${HOME}/tmp/bushfire""
ogr2ogr -f FlatGeobuf "${OUTPUT_FOLDER}/nvis6_bal.fgb" PG:"host=localhost dbname=geo user=postgres password=password port=5432" "bushfire.nvis6_bal(geom)"
aws s3 cp "${OUTPUT_FOLDER}/nvis6_bal.fgb" "s3://bushfire-rasters/vegetation/"





ssh -F ${SSH_CONFIG} ${INSTANCE_ID} 'cat ~/04_merge_geometries.log'

#
#
#find . -name "postgresql.conf"  2>&1 | grep -v "Permission denied"
#
#
#psql -d geo -f classify_and_merge_data.sql