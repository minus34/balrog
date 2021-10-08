#!/usr/bin/env bash

SECONDS=0*

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# ---------------------------------------------------------------------------------------------------------------------
# edit these to taste - NOTE: you can't use "~" for your home folder, Postgres doesn't like it
# ---------------------------------------------------------------------------------------------------------------------

AWS_PROFILE="default"
OUTPUT_FOLDER="/Users/$(whoami)/tmp/bushfire"
#
#echo "---------------------------------------------------------------------------------------------------------------------"
#echo "create subset tables to speed up export, copy and import"
#echo "---------------------------------------------------------------------------------------------------------------------"
#
#psql -d geo -f ${SCRIPT_DIR}/xx_prep_gnaf_cad_tables.sql

echo "---------------------------------------------------------------------------------------------------------------------"
echo "dump postgres tables to a local folder"
echo "---------------------------------------------------------------------------------------------------------------------"

mkdir -p "${OUTPUT_FOLDER}"
/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -d geo -t bushfire.temp_buildings -p 5432 -U postgres -f "${OUTPUT_FOLDER}/buildings.dmp" --no-owner
#/Applications/Postgres.app/Contents/Versions/13/bin/pg_dump -Fc -d geo -t bushfire.buildings -t bushfire.buildings_mga56 -p 5432 -U postgres -f "${OUTPUT_FOLDER}/buildings.dmp" --no-owner
echo "Buildings exported to dump file"

echo "---------------------------------------------------------------------------------------------------------------------"
echo "copy training data & Postgres dump file to AWS S3"
echo "---------------------------------------------------------------------------------------------------------------------"

aws --profile=${AWS_PROFILE} s3 sync ${OUTPUT_FOLDER} s3://bushfire-rasters/geoscape/ --exclude "*" --include "*.dmp"

echo "-------------------------------------------------------------------------"
duration=$SECONDS
echo " End time : $(date)"
echo " Data export took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"