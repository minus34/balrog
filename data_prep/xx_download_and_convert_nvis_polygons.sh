#!/usr/bin/env bash


#aws s3 sync /Users/s57405/Downloads/nvis6/ s3://bushfire-rasters/vegetation/nvis6/ --exclude .DS_Store








# Dept of Environment URLs prevent downloading

conda activate geo

PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=bushfire"

#ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/NVIS_6_0_LUT_AUST_DETAIL/NVIS6_0_LUT_AUST_DETAIL.gdb" -nln "nvis6_0_lookup"


ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_ACT_EXT/NVIS6_0_AUST_EXT_ACT.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_NSW_EXT/NVIS6_0_AUST_EXT_NSW.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_NT_EXT/NVIS6_0_AUST_EXT_NT.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_QLD_EXT/NVIS6_0_AUST_EXT_QLD.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_SA_EXT/NVIS6_0_AUST_EXT_SA.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_TAS_EXT/NVIS6_0_AUST_EXT_TAS.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_VIC_EXT/NVIS6_0_AUST_EXT_VIC.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append
ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/s57405/Downloads/nvis6/SHP_WA_EXT/NVIS6_0_AUST_EXT_WA.shp" -nln "nvis6_0" -nlt PROMOTE_TO_MULTI -append

