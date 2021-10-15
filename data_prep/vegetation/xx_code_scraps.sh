#!/usr/bin/env bash


# remote dump of results
pg_dump -Fc -d geo -t bushfire.nvis6_merge -p 5432 -U "ec2-user" -f "/data/nvis6_merge.dmp" --no-owner
aws s3 cp /data/nvis6_merge.dmp s3://bushfire-rasters/vegeation/

# local
aws s3 cp s3://bushfire-rasters/vegeation/nvis6_merge.dmp /Users/$(whoami)/tmp/bushfire/veg/
psql -d geo -c "drop table if exists bushfire.nvis6_merge"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/$(whoami)/tmp/bushfire/veg/nvis6_merge.dmp










find . -name "postgresql.conf"  2>&1 | grep -v "Permission denied"


psql -d geo -f classify_and_merge_data.sql