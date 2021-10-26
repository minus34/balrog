#!/usr/bin/env bash

SURFACE_COVER_PATH="/Users/s57405/Downloads/SurfaceCover_JUN21_ALLSTATES_GDA94_GEOTIFF_161"
TREES_PATH="/Users/s57405/Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161"

aws configure set default.s3.max_concurrent_requests 20

aws s3 sync "${SURFACE_COVER_PATH}" s3://bushfire-rasters/geoscape/surface_cover/
aws s3 sync "${TREES_PATH}" s3://bushfire-rasters/geoscape/trees/
