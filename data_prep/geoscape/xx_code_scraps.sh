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
aws s3 sync /Users/s57405/tmp/geoscape_202203 s3://bushfire-rasters/geoscape/202203/


# copy processed geoscape files in S3
aws s3 sync s3://bushfire-rasters/geoscape s3://bushfire-rasters/geoscape/202203/ --exclude "*" --include "*.tif"


# import buildings & properties to Postgres
conda activate geo
psql -d geo -c "create schema if not exists geoscape_202203;alter schema geoscape_202203 owner to postgres"
PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=geoscape_202203"

ogr2ogr -overwrite -progress --config PG_USE_COPY YES -t_srs EPSG:4283 -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/Buildings_MAR22_AUSTRALIA_GDA2020_GDB_310/Buildings/Buildings MARCH 2022/Standard/buildings.gdb"


ogr2ogr -overwrite -progress --config PG_USE_COPY YES -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/tmp/geoscape_202203/buildings.gdb"
ogr2ogr -overwrite -progress --config PG_USE_COPY YES -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/tmp/geoscape_202202/property.gdb"

# dump schema and copy to s3
/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -d geo -n geoscape_202203 -p 5432 -U postgres -f /Users/$(whoami)/tmp/geoscape_202203/geoscape.dmp --no-owner

aws s3 cp /Users/$(whoami)/tmp/geoscape_202203/geoscape.dmp s3://bushfire-rasters/geoscape/202203/



# copy veg TIFs from EC2 to S3
aws s3 sync /data s3://bushfire-rasters/geoscape/202203/ --exclude "*" --include "*.tif"



# resotre geoscape buildings and property
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres /Users/$(whoami)/Downloads/geoscape.dmp --clean