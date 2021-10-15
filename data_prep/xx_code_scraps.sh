#!/usr/bin/env bash


# remote dump of results
pg_dump -Fc -d geo -t bushfire.bal_factors -p 5432 -U "ec2-user" -f "/data/bal_factors.dmp" --no-owner
aws s3 cp /data/bal_factors.dmp s3://bushfire-rasters/output/

# local
aws s3 cp s3://bushfire-rasters/output/bal_factors.dmp /Users/$(whoami)/tmp/bushfire/
psql -d geo -c "drop table bushfire.bal_factors"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/$(whoami)/tmp/bushfire/bal_factors.dmp








find . -name "postgresql.conf"  2>&1 | grep -v "Permission denied"

psql -d geo -f classify_and_merge_data.sql