#!/usr/bin/env bash


conda activate geo

PG_CONNECT_STRING="host=localhost user=postgres dbname=geo password=password schemas=bushfire"

ogr2ogr -overwrite -f "PostgreSQL" PG:"${PG_CONNECT_STRING}" "/Users/$(whoami)/Downloads/NSW_BushFireProneLand/BFPL20211019.shp" -nln "nsw_rfs_bushfire_prone_land" -nlt PROMOTE_TO_MULTI -append



#Warning 1: organizePolygons() received an unexpected geometry.  Either a polygon with interior rings, or a polygon with less than 4 points, or a non-Polygon geometry.  Return arguments as a collection.
#Warning 1: Geometry of polygon of fid 226388 cannot be translated to Simple Geometry. All polygons will be contained in a multipolygon.
#Warning 1: organizePolygons() received an unexpected geometry.  Either a polygon with interior rings, or a polygon with less than 4 points, or a non-Polygon geometry.  Return arguments as a collection.
#Warning 1: Geometry of polygon of fid 226785 cannot be translated to Simple Geometry. All polygons will be contained in a multipolygon.
#Warning 1: organizePolygons() received an unexpected geometry.  Either a polygon with interior rings, or a polygon with less than 4 points, or a non-Polygon geometry.  Return arguments as a collection.
#Warning 1: Geometry of polygon of fid 694967 cannot be translated to Simple Geometry. All polygons will be contained in a multipolygon.
#Warning 1: organizePolygons() received an unexpected geometry.  Either a polygon with interior rings, or a polygon with less than 4 points, or a non-Polygon geometry.  Return arguments as a collection.
#Warning 1: Geometry of polygon of fid 695003 cannot be translated to Simple Geometry. All polygons will be contained in a multipolygon.






#aws s3 sync /Users/$(whoami)/Downloads/nvis6/ s3://bushfire-rasters/vegetation/nvis6/ --exclude .DS_Store
