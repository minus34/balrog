#!/usr/bin/env bash


# remote dump of results
pg_dump -Fc -d geo -t bushfire.bal_factors -p 5432 -U "ec2-user" -f "/data/bal_factors.dmp" --no-owner
aws s3 cp /data/bal_factors.dmp s3://bushfire-rasters/output/

# local
aws s3 cp s3://bushfire-rasters/output/bal_factors.dmp /Users/$(whoami)/tmp/bushfire/
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/$(whoami)/tmp/bushfire/bal_factors.dmp --clean








find . -name "postgresql.conf"  2>&1 | grep -v "Permission denied"

psql -d geo -f classify_and_merge_data.sql




# copy geoscape files to S3
aws s3 sync /Users/s57405/tmp/geoscape-202111 s3://bushfire-rasters/geoscape/202111/



# import buildings & properties to Postgres
conda activate geo
psql -d geo -c "create schema if not exists geoscape_202111;alter schema geoscape_202111 owner to postgres"
PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=geoscape_202111"

ogr2ogr -overwrite -progress --config PG_USE_COPY YES -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/tmp/geoscape-202111/Buildings_NOV21_AUSTRALIA_GDA94_GDB_300/Buildings/Buildings NOVEMBER 2021/Standard/buildings.gdb"
ogr2ogr -overwrite -progress --config PG_USE_COPY YES -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/tmp/geoscape-202111/Property_NOV21_AUSTRALIA_GDA94_GDB_102/Property/Property NOVEMBER 2021/Standard/property.gdb"


# dump schema and copy to s3
/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -d geo -n geoscape_202111 -p 5432 -U postgres -f /Users/$(whoami)/tmp/geoscape-202111/geoscape.dmp --no-owner

aws s3 cp /Users/$(whoami)/tmp/geoscape-202111/geoscape.dmp s3://bushfire-rasters/geoscape/202111/geoscape.dmp
