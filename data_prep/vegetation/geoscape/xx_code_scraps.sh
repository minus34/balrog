#!/usr/bin/env bash


# raw file
gdalinfo "/Users/$(whoami)/Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Trees/Trees JUNE 2021/Standard/WA_TREES_Z49_17496.tif"

# intermediate file
gdalinfo /Users/$(whoami)/tmp/bushfire/veg/temp_Z49_trees.tif

# final file
gdalinfo /Users/$(whoami)/tmp/bushfire/veg/geoscape_trees.tif



scp -F ${SSH_CONFIG} ${USER}@${INSTANCE_ID}:~/03_mosaic_and_transform_images_in_one_go.log ${SCRIPT_DIR}/03_mosaic_and_transform_images_in_one_go.log



aws s3 sync /data/ s3://bushfire-rasters/nsw_dcs/5m_dem --exclude "*" --include "*.tif"




03_mosaic_and_transform_images.log