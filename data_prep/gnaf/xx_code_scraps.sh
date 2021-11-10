#!/usr/bin/env bash

# copy mega-DEM
aws s3 cp s3://bushfire-rasters/geoscience_australia/1sec-dem/srtm_1sec_dem_s.tif /Users/$(whoami)/tmp/bushfire/
