#!/usr/bin/env bash


conda activate geo

PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=bushfire"

ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/Downloads/NSW_BushFireProneLand/BFPL20211019.shp" -nln "nsw_rfs_bushfire_prone_land" -nlt PROMOTE_TO_MULTI -append





#aws s3 sync /Users/$(whoami)/Downloads/nvis6/ s3://bushfire-rasters/vegetation/nvis6/ --exclude .DS_Store
