#!/usr/bin/env bash

# ---------------------------------------------------------------------------------------------------------------------
# Creates 100m buffers around building outlines to calculate their aspect and slope
# Requires a licensed copy of Geoscape's Buildings dataset: https://geoscape.com.au/data/buildings/
#
# Takes ~1 hour to run
# ---------------------------------------------------------------------------------------------------------------------

SECONDS=0*

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# ---------------------------------------------------------------------------------------------------------------------
# edit these to taste - NOTE: you can't use "~" for your home folder, Postgres doesn't like it
# ---------------------------------------------------------------------------------------------------------------------

AWS_PROFILE="default"
OUTPUT_FOLDER="/Users/$(whoami)/tmp/bushfire/veg"

#echo "---------------------------------------------------------------------------------------------------------------------"
#echo "create subset tables to speed up export, copy and import"
#echo "---------------------------------------------------------------------------------------------------------------------"
#
#psql -d geo -f ${SCRIPT_DIR}/create_buffered_buildings.sql

echo "---------------------------------------------------------------------------------------------------------------------"
echo "dump postgres table(s) to a local folder"
echo "---------------------------------------------------------------------------------------------------------------------"

mkdir -p "${OUTPUT_FOLDER}"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -d geo -t bushfire.nvis6_lookup -t bushfire.nvis6 -t bushfire.nvis6_exploded -p 5432 -U postgres -f "${OUTPUT_FOLDER}/nvis6.dmp" --no-owner
echo "Vegetation exported to dump file"

echo "---------------------------------------------------------------------------------------------------------------------"
echo "copy dump file to AWS S3"
echo "---------------------------------------------------------------------------------------------------------------------"

aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://bushfire-rasters/vegetation/nvis6/ --exclude "*" --include "*.dmp"

echo "-------------------------------------------------------------------------"
duration=$SECONDS
echo " End time : $(date)"
echo " Data export took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"