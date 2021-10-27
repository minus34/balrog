#!/usr/bin/env bash


# raw file
gdalinfo "/Users/s57405/Downloads/Trees_JUN21_ALLSTATES_GDA94_GEOTIFF_161/Trees/Trees JUNE 2021/Standard/WA_TREES_Z49_17496.tif"

# intermediate file
gdalinfo /Users/s57405/tmp/bushfire/veg/temp_Z49_trees.tif

# final file
gdalinfo /Users/s57405/tmp/bushfire/veg/geoscape_trees.tif



scp -F ${SSH_CONFIG} ${USER}@${INSTANCE_ID}:~/03_mosaic_and_transform_images.log ${SCRIPT_DIR}/03_mosaic_and_transform_images_remote.log

03_mosaic_and_transform_images.log