#!/usr/bin/env bash

# script runs the pool detection in an EC2 instance via SSH and dumps & downloads the results from the remote Postgres database to a local PG DB

# load AWS & EC2 vars
. ${GIT_HOME}/temp_ec2_vars.sh

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# copy and run python script remotely
FILENAME="02_get_elevation_aspect_slope.py"
LOGFILENAME="02_get_elevation_aspect_slope.log"

scp -F ${SSH_CONFIG} ${SCRIPT_DIR}/03_create_tables.sql ${USER}@${INSTANCE_ID}:~/
scp -F ${SSH_CONFIG} ${SCRIPT_DIR}/${FILENAME} ${USER}@${INSTANCE_ID}:~/
ssh -F ${SSH_CONFIG} ${INSTANCE_ID} "rm ~/${LOGFILENAME}; python3 ${FILENAME}"

# copy logfile locally
scp -F ${SSH_CONFIG} ${USER}@${INSTANCE_ID}:~/${LOGFILENAME}  ${SCRIPT_DIR}/

# dump results from Postgres and copy to S3 and then locally
ssh -F ${SSH_CONFIG} ${INSTANCE_ID} "pg_dump -Fc -d geo -t bushfire.bal_factors_gnaf -p 5432 -U ec2-user -f /data/bal_factors_gnaf.dmp --no-owner"
ssh -F ${SSH_CONFIG} ${INSTANCE_ID} "aws s3 cp /data/bal_factors_gnaf.dmp s3://bushfire-rasters/output/"
aws s3 cp s3://bushfire-rasters/output/bal_factors_gnaf.dmp ${HOME}/tmp/bushfire/

# load into local postgres (WARNING: force drops tables first)
/Applications/Postgres.app/Contents/Versions/13/bin/pg_restore -Fc -d geo -p 5432 -U postgres ${HOME}/tmp/bushfire/bal_factors_gnaf.dmp --clean

# display table counts
/Applications/Postgres.app/Contents/Versions/13/bin/psql -d geo -c "select count(*) as address_count from bushfire.bal_factors_ganf"

## add building geoms to bal factors
#/Applications/Postgres.app/Contents/Versions/13/bin/psql -d geo -f "${SCRIPT_DIR}/04_add_geoms_to_bal_factors.sql"
