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








# Dept of Environment URLs prevent downloading



PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=bushfire"


ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/NVIS_6_0_LUT_AUST_DETAIL/NVIS6_0_LUT_AUST_DETAIL.gdb" -nln "nvis6_0_lookup"


ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_ACT_EXT/NVIS6_0_AUST_EXT_ACT.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_NSW_EXT/NVIS6_0_AUST_EXT_NSW.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_NT_EXT/NVIS6_0_AUST_EXT_NT.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_QLD_EXT/NVIS6_0_AUST_EXT_QLD.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_SA_EXT/NVIS6_0_AUST_EXT_SA.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_TAS_EXT/NVIS6_0_AUST_EXT_TAS.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_VIC_EXT/NVIS6_0_AUST_EXT_VIC.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_WA_EXT/NVIS6_0_AUST_EXT_WA.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append

