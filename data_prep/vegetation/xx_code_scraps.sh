#!/usr/bin/env bash

TABLE_NAME="nvis6_bal"

# remote dump of results
pg_dump -Fc -d geo -t "bushfire.${TABLE_NAME}" -p 5432 -U "ec2-user" -f "/data/${TABLE_NAME}.dmp" --no-owner
aws s3 cp "/data/${TABLE_NAME}.dmp" s3://bushfire-rasters/vegetation/


# local restore
aws s3 cp "s3://bushfire-rasters/vegetation/${TABLE_NAME}.dmp" /Users/$(whoami)/tmp/bushfire/veg/
#psql -d geo -c "drop table if exists bushfire.${TABLE_NAME}"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres "/Users/$(whoami)/tmp/bushfire/veg/${TABLE_NAME}.dmp" --clean






ssh -F ${SSH_CONFIG} ${INSTANCE_ID} 'cat ~/04_merge_geometries.log'

#
#
#find . -name "postgresql.conf"  2>&1 | grep -v "Permission denied"
#
#
#psql -d geo -f classify_and_merge_data.sql